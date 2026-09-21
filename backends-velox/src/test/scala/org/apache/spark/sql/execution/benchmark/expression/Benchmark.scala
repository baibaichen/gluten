/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.benchmark.expression

import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.benchmark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, ReplaceExpressions}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

import org.scalatest.exceptions.TestCanceledException

import java.io.OutputStream
import java.nio.file.Files
import java.util.function.Consumer

import scala.concurrent.duration.Duration

/** Shared execution in the caller's scopes; registerCases prepares both engines before run(). */
final private[benchmark] class Benchmark(
    spark: SparkSession,
    scenario: Catalog.CaseDef,
    context: Data.Context,
    options: RunOptions =
      RunOptions(Duration.Zero, Duration.Zero),
    output: Option[OutputStream] = None,
    data: Option[Data.Plan] = None,
    profiler: Option[Profiler] = None,
    isBenchmark: Boolean = true)
  extends benchmark.Benchmark(
    scenario.id,
    context.rows,
    options.minNumIters,
    options.warmup,
    options.minTime,
    output = output) {

  lazy val (dataPlan, native, vanilla, inputs) = {
    val plan = data.getOrElse(Data.compile(scenario.inputs))
    val (nativeExpression, nativeAttributes) = fresh(plan.inputSchema, true)
    val (vanillaExpression, vanillaAttributes) = fresh(plan.inputSchema, false)
    require(nativeExpression.dataType == vanillaExpression.dataType, "Engine result types differ")
    (
      plan,
      new NativeExpressionEvaluator(Seq(nativeExpression), nativeAttributes),
      prepareJvm(vanillaExpression, vanillaAttributes),
      Data.materialize(plan, context, options.batchSize))
  }

  def registerCases(): Unit = {
    BenchmarkBlackhole.requireEnabled(isBenchmark)
    // Prepare inputs and compile before Spark starts timing.
    val _ = inputs
    register("vanilla")(runVanilla())
    register("native")(runNative())
  }

  private def register(engine: String, numIters: Int = 0)(action: => Unit): Unit = profiler match {
    case None => super.addCase(engine, numIters)(_ => action)
    case Some(controller) =>
      val path = controller.profileRoot.resolve(scenario.id).resolve(engine)
        .resolve("profile.collapsed")
      // Match Spark's registered Case bounds, not arbitrary external measure overrides.
      val minIters = if (numIters != 0) numIters else options.minNumIters
      val minNanos = if (numIters != 0) 0L else options.minTime.toNanos
      var elapsed = 0L
      super.addTimerCase(engine, numIters) {
        timer =>
          if (timer.iteration <= 0) {
            Files.createDirectories(path.getParent)
            require(!Files.exists(path), s"Profile already exists: $path")
            elapsed = 0L
          }
          val measured = timer.iteration >= 0
          // Sample the whole measured phase, including gaps, without resetting the sampling clock.
          // A failed start must not authorize cleanup of another profiler session.
          if (timer.iteration == 0) controller.start()
          var stopAfter = true
          try Utils.tryWithSafeFinally {
              timer.startTiming()
              Utils.tryWithSafeFinally(action)(timer.stopTiming())
              if (measured) {
                if (minNanos > 0) elapsed += timer.totalTime()
                stopAfter = timer.iteration + 1 >= minIters && elapsed >= minNanos
              }
            } {
              if (measured && stopAfter) {
                controller.stop()
                controller.dump(path)
              }
            }
          catch {
            case e: InterruptedException =>
              Thread.currentThread().interrupt()
              throw e
          }
      }
  }

  private[expression] def checked[T](f: => T): T =
    try f
    catch {
      case canceled: TestCanceledException => throw canceled
      case scala.util.control.NonFatal(cause) =>
        throw new IllegalArgumentException(
          s"${scenario.sourceLocation} case=${scenario.id}: ${cause.getMessage}",
          cause)
    }

  private def singleProject(plan: LogicalPlan, attributes: Seq[Attribute]): Project = {
    require(plan.resolved, s"Unresolved expression plan: $plan")
    plan match {
      case project @ Project(Seq(alias: Alias), relation: LocalRelation) =>
        require(relation.output == attributes, "Input schema or expression IDs changed")
        require(alias.references.subsetOf(relation.outputSet), "Expression has foreign inputs")
        project
      case _ => throw new IllegalArgumentException(s"Expected one Project(LocalRelation): $plan")
    }
  }

  private[expression] def prepareAnalyzed(schema: StructType, native: Boolean): Project = checked {
    val relation = LocalRelation(SparkShimLoader.getSparkShims.attributesFromStruct(schema))
    val parsed = spark.sessionState.sqlParser.parseExpression(scenario.sql)
    require(!parsed.exists(_.isInstanceOf[SubqueryExpression]), "Subqueries are not scalar inputs")
    val analyzed = spark.sessionState.executePlan(
      Project(Seq(Alias(parsed, "result")()), relation)).analyzed
    analyzed.foreach(_.expressions.foreach(_.foreach {
      case expression @ (_: AggregateExpression | _: WindowExpression | _: Generator |
          _: SubqueryExpression) =>
        throw new IllegalArgumentException(s"Unsupported scalar expression: $expression")
      case expression =>
        require(expression.deterministic, s"Nondeterministic expression: $expression")
    }))
    val project = singleProject(analyzed, relation.output)
    def nativeReplacement(expression: Expression): Expression = expression match {
      case replaceable: RuntimeReplaceable with InheritAnalysisRules =>
        nativeReplacement(replaceable.replacement)
      // ArraySize is not in the converter map; Size is. Keep Encode and StructsToJson intact.
      case size: ArraySize => nativeReplacement(size.replacement)
      case other => other.mapChildren(nativeReplacement)
    }
    val replaced = if (native) project.mapExpressions(nativeReplacement)
    else ReplaceExpressions.apply(project)
    val rewritten = SparkShimLoader.getSparkShims.rewriteWithExpression(replaced)
    val prepared = singleProject(ConstantFolding(rewritten), relation.output)
    prepared.projectList.foreach(_.foreach {
      case expression: ArraySize =>
        throw new IllegalArgumentException(s"Unprepared expression: $expression")
      case expression: RuntimeReplaceable if !native =>
        throw new IllegalArgumentException(s"Unreplaced JVM expression: $expression")
      case _ =>
    })
    prepared
  }

  private def fresh(schema: StructType, native: Boolean): (Expression, Seq[Attribute]) = {
    val project = prepareAnalyzed(schema, native)
    (project.projectList.head.asInstanceOf[Alias].child, project.child.output)
  }

  def freshJvm(): (Expression, Seq[Attribute]) = fresh(dataPlan.inputSchema, native = false)

  def runNative(consume: (ColumnarBatch, ColumnarBatch, Int) => Unit = (_, _, _) => ()): Unit = {
    var offset = 0
    var i = 0
    while (i < inputs.batches.length) {
      val input = inputs.batches(i)
      val output = native.evaluate(input)
      try consume(input, output, offset)
      finally output.close()
      offset += input.numRows()
      i += 1
    }
  }

  def runVanilla(
      predicate: BasePredicate = vanilla,
      rows: Array[UnsafeRow] = inputs.rows,
      from: Int = 0,
      until: Int = -1): Unit = {
    // -1 consumes the entire array; explicit ranges are used by correctness capture.
    val end = if (until == -1) rows.length else until
    var i = from
    while (i < end) {
      predicate.eval(rows(i))
      i += 1
    }
  }

  private[expression] def prepareJvm(
      expression: Expression,
      attributes: Seq[Attribute],
      collector: Option[Consumer[Any]] = None): BasePredicate = {
    BenchmarkBlackhole.requireEnabled(isBenchmark)
    require(
      SQLConf.get.getConf(SQLConf.CODEGEN_FACTORY_MODE).toString ==
        CodegenObjectFactoryMode.CODEGEN_ONLY.toString,
      "The benchmark terminal requires CODEGEN_ONLY"
    )
    val predicate = Predicate.create(ConsumeCodegenResult(expression, collector), attributes)
    predicate.initialize(0)
    predicate
  }
}

final private[expression] case class ConsumeCodegenResult(
    child: Expression,
    collector: Option[Consumer[Any]])
  extends UnaryExpression
  with NonSQLExpression {
  override def dataType: DataType = BooleanType
  override def nullable: Boolean = false
  override def foldable: Boolean = false
  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("The benchmark terminal requires code generation")
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val result = child.genCode(ctx)
    val consume = collector match {
      case Some(sink) =>
        val target = ctx.addReferenceObj("terminalResult", sink, classOf[Consumer[_]].getName)
        code"""
          if (${result.isNull}) {
            $target.accept(null);
          } else {
            $target.accept(${result.value});
          }
        """
      case None => child.dataType match {
          case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType |
              DoubleType | DateType | TimestampType | BinaryType | _: ArrayType | _: MapType |
              _: StructType | _: DecimalType =>
            code"""
              org.apache.spark.sql.execution.benchmark.expression.BenchmarkBlackhole.consume(
                ${result.isNull}, ${result.value});
            """
          case _: StringType =>
            code"""
              org.apache.spark.sql.execution.benchmark.expression.BenchmarkBlackhole.consume(
                ${result.isNull},
                ${result.isNull} ? null : ${result.value}.getBaseObject(),
                ${result.isNull} ? 0L : ${result.value}.getBaseOffset(),
                ${result.isNull} ? 0 : ${result.value}.numBytes());
            """
          case other => throw new IllegalArgumentException(s"Unsupported metric output: $other")
        }
    }
    ev.copy(
      code = code"""
          ${result.code}
          $consume
        """,
      isNull = FalseLiteral,
      value = TrueLiteral
    )
  }
}
