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
package org.apache.spark.sql.execution.benchmark

import org.apache.spark.SparkFunSuite
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

import org.scalatest.BeforeAndAfterAll

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest

import scala.collection.mutable.ArrayBuffer

// scalastyle:off nonascii
class RTrimExpressionBenchmarkSuite
  extends SparkFunSuite
  with SQLConfHelper
  with BeforeAndAfterAll {
  import ExpressionBenchmark._

  private val seed = 20260912L
  private val widths = Seq(10, 12, 13, 64, 256)
  private val patterns = Seq("none", "first", "cluster", "half-even", "last", "penultimate", "all")
  private val alignedNames =
    (for {
      width <- widths
      pattern <- patterns
    } yield s"rtrim-l$width-$pattern") ++ Seq(
      "rtrim-l64-null50",
      "rtrim-l64-utf8-half-even",
      "identity-l10",
      "identity-l256",
      "rtrim-leading-boundary",
      "rtrim-custom")
  private val representative = Seq(
    "rtrim-l10-none" -> 10,
    "rtrim-l12-half-even" -> 12,
    "rtrim-l13-half-even" -> 13,
    "rtrim-l64-half-even" -> 64,
    "rtrim-l256-none" -> 256,
    "rtrim-l256-last" -> 256,
    "rtrim-l256-penultimate" -> 256,
    "rtrim-l256-all" -> 256,
    "rtrim-l64-null50" -> 64,
    "rtrim-l64-utf8-half-even" -> 64,
    "identity-l10" -> 10,
    "identity-l256" -> 256
  )

  private def scenario(name: String): Case = {
    val selected = RTrimExpressionBenchmark.cases.find(_.name == name)
    assert(selected.isDefined, s"Missing historical case: $name")
    selected.get
  }

  private def example(
      values: Seq[Seq[String]],
      expression: Seq[Attribute] => Seq[Expression]): Case = {
    val schema = values.head.indices.foldLeft(new StructType()) {
      (schema, i) => schema.add(s"input$i", StringType, nullable = true)
    }
    Case(
      "example",
      schema,
      expression,
      (_, row) => InternalRow.fromSeq(values(row).map(UTF8String.fromString)))
  }

  private def inputStrings(prepared: Prepared): Seq[String] =
    prepared.vanillaRows.map {
      row => if (row.isNullAt(0)) null else row.getUTF8String(0).toString
    }.toVector

  private def fingerprint(values: Seq[String]): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    values.foreach {
      value =>
        val bytes = if (value == null) null else value.getBytes(UTF_8)
        digest.update(
          ByteBuffer.allocate(4).putInt(if (bytes == null) -1 else bytes.length).array())
        if (bytes != null) digest.update(bytes)
    }
    digest.digest().map(b => f"${b & 0xff}%02x").mkString
  }

  override def afterAll(): Unit = {
    try {
      shutdown()
    } finally {
      super.afterAll()
    }
  }

  test("rtrim uses stable UnsafeRows and identical native vector inputs") {
    val values = Seq(null, "", " ", "  keep", "tail  ", "中文 ", "x" * 64 + "  ")
    val expected = Seq(null, "", "", "  keep", "tail", "中文", "x" * 64)
    val selected = example(values.map(Seq(_)), attrs => Seq(StringTrimRight(attrs.head)))
    withPrepared(selected, values.size, batchSize = 3, seed = 7L) {
      prepared =>
        assert(prepared.vanillaRows.forall(_.isInstanceOf[UnsafeRow]))
        assert(!(prepared.vanillaRows(0) eq prepared.vanillaRows(1)))
        assert(prepared.nativeBatchSizes == Seq(3, 3, 1))
        assert(prepared.nativeInputValues == values.map(value => Vector(value)))
        assert(prepared.vanillaResults == expected)
        assert(prepared.nativeResults == expected)
        (0 until 3).foreach {
          _ =>
            prepared.runVanilla()
            prepared.runNative()
            assert(inputStrings(prepared) == values)
            assert(prepared.nativeInputValues == values.map(value => Vector(value)))
            assert(prepared.vanillaResults == expected)
            assert(prepared.nativeResults == expected)
        }
    }
  }

  // Frozen 7d2610bf07 rowBytes: seed 20260912, 17 rows, native batches 8 + 8 + 1.
  // Hash length-prefixed UTF8 rows (-1 for NULL), preserving boundaries and whole-row order.
  private val historicalFingerprints = Map(
    "rtrim-l10-none" -> "64f46a07e1b33927faa93acb50f5bfa2819f53752b901dd6af6bcdd87789a161",
    "rtrim-l12-half-even" -> "c5da2547f3a4c14e8c7e23ba155172c4a09d7f85a64421fe6d2fb97a5b0a4e8a",
    "rtrim-l13-half-even" -> "f6238fe9c49db2a5c6de4247d2363e242b3f138e564a1e8ce931a047226e06fb",
    "rtrim-l64-half-even" -> "aad5766132f537ff94bbeedd9db7de2be0604ec3c491aaae903b3675dddf1397",
    "rtrim-l256-none" -> "0025c30a134258bf43eae325e0a9a2494c15edcfa395d33ddb7db0cd64470c1b",
    "rtrim-l256-last" -> "5f06daaaba97917045583fdc7694cf61db6cf5b267ea10a3be4c1240937f3f0f",
    "rtrim-l256-penultimate" ->
      "41bad2c858fa5b54f50556476a4f97b4dfa13988f8c8fb29da79069f82ab95b0",
    "rtrim-l256-all" -> "345a71675210077115ca5aa5ced9114540238b9256eaea4e071fed721be39748",
    "rtrim-l64-null50" -> "c17cb7a31bf9121c4c6598d2436f5f76d3c4c0fe65acb7cde73ad31e433628b5",
    "rtrim-l64-utf8-half-even" ->
      "c8e7403f54a38b449f8823a4d6b07cc55f073caa23bfd8189b973d44cc907ff4",
    "identity-l10" -> "64f46a07e1b33927faa93acb50f5bfa2819f53752b901dd6af6bcdd87789a161",
    "identity-l256" -> "0025c30a134258bf43eae325e0a9a2494c15edcfa395d33ddb7db0cd64470c1b"
  )

  test("historical representative catalog retains unique case identities") {
    val names = RTrimExpressionBenchmark.cases.map(_.name)
    assert(names.distinct == names, "Case names must be unique")
    assert(representative.map(_._1).forall(names.contains))
  }

  representative.foreach {
    case (name, width) =>
      test(s"historical $name retains exact logical input bytes and expected engine results") {
        withPrepared(scenario(name), 17, 8, seed) {
          prepared =>
            val inputs = inputStrings(prepared)
            assert(prepared.nativeBatchSizes == Seq(8, 8, 1))
            assert(inputs.filter(_ != null).forall(_.getBytes(UTF_8).length == width))
            assert(fingerprint(inputs) == historicalFingerprints(name))
            assert(prepared.nativeInputValues == inputs.map(Seq(_)))
            val results = inputs.map {
              value =>
                if (value == null || name.startsWith("identity-")) value
                else value.reverse.dropWhile(_ == ' ').reverse
            }
            assert(prepared.vanillaResults == results)
            assert(prepared.nativeResults == results)
        }
      }
  }

  test("historical no-trim values are nonconstant and seeded without synthetic empty strings") {
    val selected = scenario("rtrim-l10-none")
    val first = withPrepared(selected, 300, 7, seed)(inputStrings)
    assert(first.take(4) == Seq("01352830ct", "01352831hy", "01352832md", "01352833ri"))
    assert(first.distinct.size == 300)
    assert(first.forall(value => value.length == 10 && !value.endsWith(" ")))
    assert(
      Seq(10239, 10240, 3999999).map(i => selected.input(seed, i).getUTF8String(0).toString) ==
        Seq("01350fcfdu", "01350030iz", "010820cfri"))
    assert(withPrepared(selected, 300, 16, seed)(inputStrings) == first)
    assert(withPrepared(selected, 300, 7, seed + 1)(inputStrings) != first)
  }

  test("historical half-even trimming restarts at each native batch boundary") {
    withPrepared(scenario("rtrim-l12-half-even"), 8, 3, seed) {
      prepared =>
        val expected = Seq(
          "01352830ct  ",
          "01352831hypg",
          "01352832md  ",
          "01352833ri  ",
          "01352834wnev",
          "01352835bs  ",
          "01352836gx  ",
          "01352837lctk")
        assert(prepared.nativeBatchSizes == Seq(3, 3, 2))
        assert(inputStrings(prepared) == expected)
        assert(prepared.nativeInputValues == expected.map(Seq(_)))
        prepared.verify()
    }
  }

  test("historical null sampling keeps half-even trim independent of NULL positions") {
    withPrepared(scenario("rtrim-l64-null50"), 200, 200, seed) {
      prepared =>
        val inputs = inputStrings(prepared)
        val nullRows = inputs.indices.filter(i => inputs(i) == null)
        assert(nullRows.take(11) == Seq(0, 3, 5, 6, 8, 9, 11, 14, 16, 17, 19))
        assert(nullRows == inputs.indices.filter(i => ((i % 200) * 73 + 37) % 200 < 100))
        assert(nullRows.size == 100)
        assert(nullRows.count(_ % 2 == 0) == 50)
        assert(inputs.count(value => value != null && value.endsWith("  ")) == 50)
        prepared.verify()
    }
  }

  test("historical UTF8 bodies and ASCII remainders retain byte widths and row identity") {
    withPrepared(scenario("rtrim-l64-utf8-half-even"), 2, 2, seed) {
      prepared =>
        val expected = Seq("01352830" + "丰" * 18 + "  ", "01352831" + "丱" * 18 + "pg")
        assert(inputStrings(prepared) == expected)
        assert(expected.forall(_.getBytes(UTF_8).length == 64))
        assert(prepared.nativeInputValues == expected.map(Seq(_)))
        assert(prepared.nativeResults == Seq(expected.head.dropRight(2), expected(1)))
        prepared.verify()
    }
  }

  test("last and penultimate permute complete rows within full partial and singleton batches") {
    // 16640 exercises a full 10240 batch and the same 6400-row tail shape as the 4M run.
    Seq((1, 1), (2, 2), (17, 8), (16640, 10240)).foreach {
      case (rows, batchSize) =>
        def inputs(name: String): Seq[String] =
          withPrepared(scenario(name), rows, batchSize, seed) {
            prepared =>
              assert(
                prepared.nativeBatchSizes ==
                  (0 until rows by batchSize).map(start => math.min(batchSize, rows - start)))
              val values = inputStrings(prepared)
              assert(prepared.nativeInputValues == values.map(Seq(_)))
              prepared.verify()
              values
          }
        val original = inputs("rtrim-l256-last")
        val swapped = inputs("rtrim-l256-penultimate")
        (0 until rows by batchSize).foreach {
          start =>
            val before = original.slice(start, start + batchSize)
            val after = swapped.slice(start, start + batchSize)
            val expected =
              if (before.size < 2) before
              else before.dropRight(2) ++ Seq(before.last, before(before.size - 2))
            assert(after == expected)
            assert(after.sorted == before.sorted)
            assert(before.count(_.endsWith("  ")) == 1)
            assert(after.count(_.endsWith("  ")) == 1)
            assert(before.last.take(8) == f"${(start + before.size - 1L) ^ seed}%08x")
        }
        if (rows == 16640) {
          assert(original.last.take(8) == "013568cf")
          assert(swapped(swapped.size - 2) == original.last)
          assert(swapped.last.take(8) == "013568ce")
        }
    }
  }

  test("rtrim boundary cases preserve leading spaces and custom trim prefixes") {
    withPrepared(scenario("rtrim-leading-boundary"), 2, 1, 1L) {
      prepared =>
        val expected = "  " + "x" * 60
        assert(prepared.vanillaResults == Seq(expected, expected))
        assert(prepared.nativeResults == Seq(expected, expected))
    }
    withPrepared(scenario("rtrim-custom"), 1, 1, 2L) {
      prepared =>
        assert(prepared.nativeInputValues == Seq(Seq("xyvaluexy", "xy")))
        assert(prepared.vanillaResults == Seq("xyvalue"))
        assert(prepared.nativeResults == Seq("xyvalue"))
    }
  }

  test("one expression can read multiple independently bound input columns") {
    val values = Seq(
      Seq("leftxy", "xy"),
      Seq("xy", "x"),
      Seq("keep", ""),
      Seq(null, "xy"),
      Seq("value", null),
      Seq("中文xy", "xy"))
    val selected = example(values, attrs => Seq(StringTrimRight(attrs(0), Some(attrs(1)))))
    withPrepared(selected, values.size, batchSize = 4, seed = 0L) {
      prepared =>
        assert(prepared.nativeInputValues == values)
        val expected = Seq("left", "xy", "keep", null, null, "中文")
        assert(prepared.vanillaResults == expected)
        assert(prepared.nativeResults == expected)
    }
  }

  test("external row counts allow empty correctness inputs and small partial batches") {
    Seq("rtrim-l10-none", "rtrim-l256-last", "rtrim-l256-penultimate", "identity-l10").foreach {
      name =>
        Seq(0, 1, 9).foreach {
          size =>
            withPrepared(scenario(name), size, batchSize = 4, seed = seed) {
              prepared =>
                assert(prepared.vanillaRows.length == size)
                assert(prepared.nativeBatchSizes.sum == size)
                prepared.runVanilla()
                prepared.runNative()
                prepared.verify()
            }
        }
    }
  }

  private class RecordingPrepared extends Prepared {
    val calls = ArrayBuffer.empty[String]
    override def vanillaRows: Array[UnsafeRow] = fail("Inputs read inside timing")
    override def nativeBatchSizes: Seq[Int] = fail("Batch sizes read inside timing")
    override def nativeInputValues: Seq[Seq[String]] = fail("Input validation inside timing")
    override def vanillaResults: Seq[String] = fail("Vanilla result materialization inside timing")
    override def evaluateVanilla(input: InternalRow): UTF8String =
      fail("Unexpected individual Vanilla evaluation")
    override def nativeResults: Seq[String] = fail("Native result materialization inside timing")
    override def verify(): Unit = fail("Correctness check inside timing")
    override def runVanilla(): Unit = { calls += "vanilla" }
    override def runNative(): Unit = { calls += "native" }
    override def vanillaSignature: Long = fail("Signature read inside timed callback")
    override def nativeSignature: Long = fail("Signature read inside timed callback")
  }

  private def checkRegistered(args: Array[String], expected: Seq[String]): Unit = {
    val options = parseArgs(args, RTrimExpressionBenchmark.cases)
    val prepared = new RecordingPrepared
    val benchmark = new Benchmark("registration only", 1L)
    registerCases(benchmark, prepared, options)
    assert(prepared.calls.isEmpty, "Registration must not execute either engine")
    val labels = Map("vanilla" -> "Vanilla / UnsafeRow", "native" -> "Gluten / Velox vectors")
    assert(benchmark.benchmarks.map(_.name).toSeq == expected.map(labels))
    benchmark.benchmarks.foreach(_.fn(new Benchmark.Timer(0)))
    assert(prepared.calls.toSeq == expected)
  }

  test("CLI defaults preserve historical seed and native batch size") {
    val options = parseArgs(Array("17"), RTrimExpressionBenchmark.cases)
    assert(options.rows == 17 && options.batchSize == 10240 && options.seed == seed)
    assert(options.caseName.isEmpty)
  }

  test("default registration delegates only to both engine passes in vanilla-first order") {
    checkRegistered(Array("17"), Seq("vanilla", "native"))
  }

  for {
    engine <- Seq("both", "vanilla", "native")
    order <- Seq("vanilla-first", "native-first")
  } {
    test(s"CLI $engine $order registers only the selected timed engines in order") {
      val expected = engine match {
        case "vanilla" => Seq("vanilla")
        case "native" => Seq("native")
        case "both" if order == "native-first" => Seq("native", "vanilla")
        case _ => Seq("vanilla", "native")
      }
      checkRegistered(Array("17", "4", "5", "rtrim-custom", engine, order), expected)
    }
  }

  test("CLI omitted engine order defaults to vanilla first") {
    checkRegistered(Array("17", "4", "5", "rtrim-custom", "both"), Seq("vanilla", "native"))
  }

  test("CLI preserves explicit row batch seed and case arguments and rejects invalid selections") {
    val options = parseArgs(Array("17", "4", "5", "rtrim-custom"), RTrimExpressionBenchmark.cases)
    assert(options.rows == 17 && options.batchSize == 4 && options.seed == 5L)
    assert(options.caseName.contains("rtrim-custom"))
    Seq(
      Array.empty[String],
      Array("0"),
      Array("-1"),
      Array("1", "0"),
      Array("1", "-1"),
      Array("1", "1", "0", "unknown"),
      Array("1", "1", "0", "rtrim-custom", "unknown"),
      Array("1", "1", "0", "rtrim-custom", "both", "unknown"),
      Array("1", "1", "0", "rtrim-custom", "both", "vanilla-first", "extra")
    ).foreach {
      args => intercept[IllegalArgumentException](parseArgs(args, RTrimExpressionBenchmark.cases))
    }
  }

  test("unsupported case shape fails explicitly rather than dropping expressions or columns") {
    val selected = example(Seq(Seq("value")), attrs => Seq(StringTrimRight(attrs.head)))
    Seq(
      selected.copy(expressions = _ => Seq.empty),
      selected.copy(expressions = attrs => Seq(attrs.head, attrs.head)),
      selected.copy(inputSchema = new StructType().add("input", IntegerType)),
      selected.copy(expressions = _ => Seq(Literal(1)))
    ).foreach {
      invalid =>
        intercept[IllegalArgumentException] {
          withPrepared(invalid, 1, 1, 0L)(_ => fail("Unsupported case was accepted"))
        }
    }
    intercept[IllegalArgumentException] {
      withPrepared(selected, -1, 1, 0L)(_ => fail("Negative row count was accepted"))
    }
    intercept[IllegalArgumentException] {
      withPrepared(selected, 1, 0, 0L)(_ => fail("Zero batch size was accepted"))
    }
  }

  test("actual JVM terminal returns borrowed no-trim carriers rather than final UnsafeRow bytes") {
    Seq("identity-l10", "rtrim-l10-none", "identity-l256", "rtrim-l256-none").foreach {
      name =>
        withPrepared(scenario(name), 3, 2, seed) {
          prepared =>
            prepared.vanillaRows.foreach {
              row =>
                val input = row.getUTF8String(0)
                val result = prepared.evaluateVanilla(row)
                assert(result == input)
                assert(result.getBaseObject eq input.getBaseObject)
                assert(result.getBaseOffset == input.getBaseOffset)
                assert(result.numBytes() == input.numBytes())
            }
            prepared.runVanilla()
            prepared.verify()
        }
    }
  }

  test("actual JVM terminal overwrites prior values with NULL and keeps empty distinct") {
    val values = Seq("value", null, "", "   ", "tail  ", null, "中文 ", "next")
    val expected = Seq("value", null, "", "", "tail", null, "中文", "next")
    val selected = example(values.map(Seq(_)), attrs => Seq(StringTrimRight(attrs.head)))
    withPrepared(selected, values.size, 3, seed) {
      prepared =>
        (0 until 2).foreach {
          _ =>
            val actual = prepared.vanillaRows.map {
              row =>
                val value = prepared.evaluateVanilla(row)
                if (value == null) null else value.toString
            }.toSeq
            assert(actual == expected)
            assert(prepared.vanillaResults == expected)
            assert(prepared.nativeResults == expected)
            prepared.runVanilla()
            prepared.runNative()
            prepared.verify()
        }
        assert(inputStrings(prepared) == values)
        assert(prepared.nativeInputValues == values.map(Seq(_)))
    }
  }

  test("actual JVM terminal preserves intrinsic trim allocation without final row serialization") {
    val selected = example(Seq(Seq("tail  ")), attrs => Seq(StringTrimRight(attrs.head)))
    withPrepared(selected, 1, 1, seed) {
      prepared =>
        val row = prepared.vanillaRows.head
        val input = row.getUTF8String(0)
        val expected = input.trimRight()
        val actual = prepared.evaluateVanilla(row)
        assert(actual == expected)
        assert(!(actual.getBaseObject eq input.getBaseObject))
        assert(actual.getBaseOffset == expected.getBaseOffset)
        assert(actual.getBaseObject.asInstanceOf[Array[Byte]].length == actual.numBytes())
        prepared.verify()
    }
  }

  test("nested rtrim and concat use the requested expression with multiple bound inputs") {
    val values = Seq(
      Seq("left  ", "-right"),
      Seq(null, "-right"),
      Seq("kept", null),
      Seq("中文 ", "后"),
      Seq("", "suffix"))
    val expected = Seq("left-right", null, null, "中文后", "suffix")
    val selected = example(values, attrs => Seq(Concat(Seq(StringTrimRight(attrs.head), attrs(1)))))
    withPrepared(selected, values.size, 2, seed) {
      prepared =>
        val actual = prepared.vanillaRows.map {
          row =>
            val value = prepared.evaluateVanilla(row)
            if (value == null) null else value.toString
        }.toSeq
        assert(actual == expected)
        assert(prepared.vanillaResults == expected)
        assert(prepared.nativeResults == expected)
        prepared.runVanilla()
        prepared.runNative()
        prepared.verify()
        assert(prepared.nativeInputValues == values)
    }
  }

  private case class CodegenOnlyProbe(child: Expression)
    extends UnaryExpression
    with NonSQLExpression {
    override def dataType: org.apache.spark.sql.types.DataType = StringType
    override def foldable: Boolean = false
    override def eval(input: InternalRow): Any = fail("Interpreted expression was evaluated")
    override protected def withNewChildInternal(newChild: Expression): Expression =
      copy(child = newChild)
    override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
      val initialized = ctx.addMutableState("int", "partition", name => s"$name = -1;")
      ctx.addPartitionInitializationStatement(s"$initialized = partitionIndex;")
      val value = child.genCode(ctx)
      ev.copy(code = code"""
        if ($initialized != 0) {
          throw new IllegalStateException("Terminal did not initialize the generated expression");
        }
        ${value.code}
        boolean ${ev.isNull} = ${value.isNull};
        UTF8String ${ev.value} = ${value.value};
      """)
    }
  }

  test("JVM terminal delegates child codegen binding and partition initialization without eval") {
    val schema = new StructType().add("first", StringType).add("second", StringType)
    val attrs = DataTypeUtils.toAttributes(schema)
    withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> "CODEGEN_ONLY") {
      val evaluate = prepareVanilla(Seq(CodegenOnlyProbe(attrs(1))), attrs)
      val encode = UnsafeProjection.create(schema)
      val row = encode(InternalRow(UTF8String.fromString("wrong"), UTF8String.fromString("value")))
        .copy()
      val value = evaluate(row)
      assert(value.toString == "value")
      val empty = encode(InternalRow(UTF8String.fromString("wrong"), UTF8String.EMPTY_UTF8)).copy()
      assert(evaluate(empty) == UTF8String.EMPTY_UTF8)
      val missing = encode(InternalRow(UTF8String.fromString("wrong"), null)).copy()
      assert(evaluate(missing) == null)
      assert(evaluate(row).toString == "value")
    }
  }

  test("JVM terminal rejects empty multiple and non-string output expressions") {
    val attrs = Seq(AttributeReference("input", StringType, nullable = true)())
    Seq(Seq.empty[Expression], Seq(attrs.head, attrs.head), Seq(Literal(1))).foreach {
      expressions =>
        intercept[IllegalArgumentException] {
          prepareVanilla(expressions, attrs)
        }
    }
  }

  test("representative selector preserves the exact ordered twelve historical cases") {
    val cases = RTrimExpressionBenchmark.cases
    val groups = RTrimExpressionBenchmark.collections
    val options = parseArgs(Array("17", "8", seed.toString, "representative"), cases, groups)
    val selected = selectedCases(options, cases, groups)
    assert(selected.map(_.name) == representative.map(_._1))
    assert(selected.size == 12 && selected.distinct.size == 12)
    assert(!selected.exists(_.name == "rtrim-custom"))
    assert(!selected.exists(_.name == "rtrim-leading-boundary"))
  }

  test("single-case and full catalog selection remain distinct from named collections") {
    val cases = RTrimExpressionBenchmark.cases
    val groups = RTrimExpressionBenchmark.collections
    val full = parseArgs(Array("17"), cases, groups)
    assert(selectedCases(full, cases, groups) == cases)
    assert(cases.map(_.name) == alignedNames)
    val single = parseArgs(Array("17", "8", seed.toString, "rtrim-custom"), cases, groups)
    assert(selectedCases(single, cases, groups).map(_.name) == Seq("rtrim-custom"))
    assert(groups("representative").map(_.name) == representative.map(_._1))
  }

  Seq("vanilla", "native").foreach {
    engine =>
      test(s"representative $engine suite registers exactly one timed engine for each case") {
        val cases = RTrimExpressionBenchmark.cases
        val groups = RTrimExpressionBenchmark.collections
        val options = parseArgs(
          Array("17", "8", seed.toString, "representative", engine, "vanilla-first"),
          cases,
          groups)
        val registered = selectedCases(options, cases, groups).map {
          selected =>
            val prepared = new RecordingPrepared
            val benchmark = new Benchmark(selected.name, options.rows.toLong)
            registerCases(benchmark, prepared, options)
            assert(prepared.calls.isEmpty)
            assert(benchmark.benchmarks.size == 1)
            val label = if (engine == "vanilla") "Vanilla / UnsafeRow" else "Gluten / Velox vectors"
            assert(benchmark.benchmarks.head.name == label)
            benchmark.benchmarks.head.fn(new Benchmark.Timer(0))
            assert(prepared.calls.toSeq == Seq(engine))
            selected.name
        }
        assert(registered == representative.map(_._1))
      }
  }

  test("ambiguous case and collection names are rejected instead of selecting silently") {
    val collision = scenario("rtrim-custom").copy(name = "representative")
    intercept[IllegalArgumentException] {
      parseArgs(
        Array("17", "8", seed.toString, "representative"),
        RTrimExpressionBenchmark.cases :+ collision,
        RTrimExpressionBenchmark.collections)
    }
  }

  test("invalid named collections fail without losing missing or duplicate members") {
    val cases = RTrimExpressionBenchmark.cases
    val first = cases.head
    Seq(
      Seq.empty[Case],
      Seq(first, first),
      Seq(first.copy(name = "unknown-member"))
    ).foreach {
      members =>
        intercept[IllegalArgumentException] {
          parseArgs(
            Array("17", "8", seed.toString, "collection"),
            cases,
            Map("collection" -> members))
        }
    }
    intercept[IllegalArgumentException] {
      parseArgs(
        Array("17", "8", seed.toString, "unknown-collection"),
        cases,
        RTrimExpressionBenchmark.collections)
    }
  }

  test("primitive passes sum result bytes and nulls then reset across repetitions and batches") {
    val values = Seq("value", null, "", "   ", "tail  ", "中文 ", "unchanged")
    val expected = Seq("value", null, "", "", "tail", "中文", "unchanged")
    val selected = example(values.map(Seq(_)), attrs => Seq(StringTrimRight(attrs.head)))
    Seq(1, 3, 8).foreach {
      batchSize =>
        withPrepared(selected, values.size, batchSize, seed) {
          prepared =>
            (0 until 3).foreach {
              _ =>
                prepared.runVanilla()
                prepared.runNative()
                assert(prepared.vanillaSignature == 23L)
                assert(prepared.nativeSignature == 23L)
            }
            assert(prepared.vanillaResults == expected)
            assert(prepared.nativeResults == expected)
            assert(inputStrings(prepared) == values)
            assert(prepared.nativeInputValues == values.map(Seq(_)))
            prepared.verify()
        }
    }
  }

  test("primitive passes preserve zero rows and distinguish null from empty results") {
    Seq(Seq("value"), Seq[String](null), Seq(""), Seq("   ")).foreach {
      values =>
        val selected = example(values.map(Seq(_)), attrs => Seq(StringTrimRight(attrs.head)))
        Seq(0, 1).foreach {
          rows =>
            withPrepared(selected, rows, 3, seed) {
              prepared =>
                val expected =
                  if (rows == 0) 0L
                  else if (values.head == null) -1L
                  else if (values.head == "value") 5L
                  else 0L
                prepared.runVanilla()
                prepared.runNative()
                assert(prepared.vanillaSignature == expected)
                assert(prepared.nativeSignature == expected)
                prepared.runNative()
                prepared.runVanilla()
                assert(prepared.vanillaSignature == expected)
                assert(prepared.nativeSignature == expected)
                prepared.verify()
            }
        }
    }
  }

  test("primitive multi-input nested expression consumes the final result not input lengths") {
    val values = Seq(
      Seq("left  ", "-right"),
      Seq(null, "suffix"),
      Seq("a ", null),
      Seq("中文 ", "后"),
      Seq("", "suffix"))
    val expected = Seq("left-right", null, null, "中文后", "suffix")
    val selected = example(values, attrs => Seq(Concat(Seq(StringTrimRight(attrs.head), attrs(1)))))
    withPrepared(selected, values.size, 2, seed) {
      prepared =>
        prepared.runVanilla()
        prepared.runNative()
        assert(prepared.vanillaSignature == 23L)
        assert(prepared.nativeSignature == 23L)
        assert(prepared.vanillaResults == expected)
        assert(prepared.nativeResults == expected)
        prepared.verify()
    }
  }

  test("primitive signatures match the full content oracle for each representative case") {
    RTrimExpressionBenchmark.representativeCases.foreach {
      selected =>
        withClue(s"${selected.name}: ") {
          withPrepared(selected, 17, 8, seed) {
            prepared =>
              val expected = prepared.vanillaResults.map {
                value => if (value == null) -1L else value.getBytes(UTF_8).length.toLong
              }.sum
              prepared.runVanilla()
              prepared.runNative()
              assert(prepared.vanillaSignature == expected)
              assert(prepared.nativeSignature == expected)
              prepared.verify()
          }
        }
    }
  }

  test("primitive JVM consumer binds and initializes real child codegen") {
    val schema = new StructType().add("first", StringType).add("second", StringType)
    val attrs = DataTypeUtils.toAttributes(schema)
    withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> "CODEGEN_ONLY") {
      val (predicate, lengths) = prepareVanillaLengths(Seq(CodegenOnlyProbe(attrs(1))), attrs)
      val encode = UnsafeProjection.create(schema)
      val values = Seq("中文", null, "", "x")
      lengths.reset()
      values.foreach {
        value =>
          val row = InternalRow(UTF8String.fromString("wrong"), UTF8String.fromString(value))
          predicate.eval(encode(row))
      }
      assert(lengths.total == 6L)
      lengths.reset()
      assert(lengths.total == 0L)
      val one = InternalRow(UTF8String.fromString("wrong"), UTF8String.fromString("x"))
      predicate.eval(encode(one))
      assert(lengths.total == 1L)
    }
  }

  test("primitive accumulator widens contributions before addition and resets independently") {
    val lengths = new ExpressionBenchmarkLengths
    lengths.reset()
    lengths.acceptLength(Int.MaxValue)
    lengths.acceptLength(Int.MaxValue)
    lengths.acceptLength(-1)
    lengths.acceptLength(0)
    assert(lengths.total == 4294967293L)
    lengths.reset()
    assert(lengths.total == 0L)
    lengths.acceptLength(-1)
    assert(lengths.total == -1L)
  }

  test("primitive JVM consumer fails closed for unsupported shapes and interpreted mode") {
    val attrs = Seq(AttributeReference("input", StringType, nullable = true)())
    withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> "CODEGEN_ONLY") {
      Seq(Seq.empty[Expression], Seq(attrs.head, attrs.head), Seq(Literal(1))).foreach {
        expressions =>
          intercept[IllegalArgumentException] {
            prepareVanillaLengths(expressions, attrs)
          }
      }
    }
    withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> "NO_CODEGEN") {
      intercept[IllegalArgumentException] {
        prepareVanillaLengths(Seq(attrs.head), attrs)
      }
    }
  }

  test("aligned RTrim catalog has the ordered five-width seven-pattern grid and six extras") {
    val cases = RTrimExpressionBenchmark.cases
    assert(cases.map(_.name) == alignedNames)
    assert(cases.size == 41 && cases.map(_.name).distinct.size == 41)
    val options = parseArgs(
      Array("17", "8", seed.toString, "aligned"),
      cases,
      RTrimExpressionBenchmark.collections)
    assert(selectedCases(options, cases, RTrimExpressionBenchmark.collections) == cases)
    assert(
      RTrimExpressionBenchmark.collections("representative").map(_.name) ==
        representative.map(_._1))
  }

  patterns.foreach {
    pattern =>
      test(s"aligned RTrim $pattern uses actual batch-local rows at every width") {
        widths.foreach {
          width =>
            Seq(1, 8, 17).foreach {
              batchSize =>
                withClue(s"width=$width, batchSize=$batchSize: ") {
                  withPrepared(scenario(s"rtrim-l$width-$pattern"), 17, batchSize, seed) {
                    prepared =>
                      val values = inputStrings(prepared)
                      val expected = values.indices.map {
                        index =>
                          val local = index % batchSize
                          val count = math.min(batchSize, values.size - (index - local))
                          val sourceLocal =
                            if (pattern == "penultimate" && count > 1 && local == count - 2) {
                              count - 1
                            } else if (
                              pattern == "penultimate" && count > 1 && local == count - 1
                            ) {
                              count - 2
                            } else local
                          val sourceRow = index - local + sourceLocal
                          val selected = pattern match {
                            case "none" => false
                            case "first" => sourceLocal == 0
                            case "cluster" => sourceLocal < 16
                            case "half-even" => sourceLocal % 2 == 0
                            case "last" | "penultimate" => sourceLocal == count - 1
                            case "all" => true
                          }
                          val value = values(index)
                          assert(value != null && value.getBytes(UTF_8).length == width)
                          assert(!value.startsWith(" "))
                          assert(value.endsWith("  ") == selected)
                          assert(value.take(8) == f"${(sourceRow.toLong ^ seed) & 0xffffffffL}%08x")
                          if (selected) value.dropRight(2) else value
                      }
                      assert(prepared.nativeInputValues == values.map(Seq(_)))
                      assert(prepared.vanillaResults == expected)
                      assert(prepared.nativeResults == expected)
                      prepared.runVanilla()
                      prepared.runNative()
                      val signature = expected.map(_.getBytes(UTF_8).length.toLong).sum
                      assert(prepared.vanillaSignature == signature)
                      assert(prepared.nativeSignature == signature)
                      prepared.verify()
                  }
                }
            }
        }
      }
  }

  test("aligned RTrim first and cluster restart in the 6400-row partial batch") {
    Seq("first" -> 1, "cluster" -> 16).foreach {
      case (pattern, selectedCount) =>
        withPrepared(scenario(s"rtrim-l64-$pattern"), 16640, 10240, seed) {
          prepared =>
            assert(prepared.nativeBatchSizes == Seq(10240, 6400))
            val values = inputStrings(prepared)
            Seq(0, 10240).foreach {
              start =>
                val batch = values.slice(start, start + 10240)
                assert(
                  batch.indices.filter(i => batch(i).endsWith("  ")) ==
                    (0 until selectedCount))
            }
            assert(values(10240).take(8) == "01350030")
            prepared.verify()
        }
    }
  }

  test("every aligned RTrim case verifies full values and repeated primitive signatures") {
    assert(RTrimExpressionBenchmark.cases.map(_.name) == alignedNames)
    RTrimExpressionBenchmark.cases.foreach {
      selected =>
        withClue(s"${selected.name}: ") {
          withPrepared(selected, 17, 8, seed) {
            prepared =>
              val expected = prepared.vanillaResults
              assert(prepared.nativeResults == expected)
              val signature = expected.map {
                value => if (value == null) -1L else value.getBytes(UTF_8).length.toLong
              }.sum
              (0 until 2).foreach {
                _ =>
                  prepared.runNative()
                  prepared.runVanilla()
                  assert(prepared.nativeSignature == signature)
                  assert(prepared.vanillaSignature == signature)
              }
              prepared.verify()
          }
        }
    }
  }

  test("combined trim catalog qualifies both independent catalogs in RTrim then LTrim order") {
    val right = RTrimExpressionBenchmark.cases
    val left = LTrimExpressionBenchmark.cases
    val combined = TrimExpressionBenchmark.cases
    assert(right.size == 41 && left.size == 41)
    assert(combined.size == 82 && combined.map(_.name).distinct.size == 82)
    assert(combined.take(41).map(_.name) == right.map(c => s"rtrim/${c.name}"))
    assert(combined.drop(41).map(_.name) == left.map(c => s"ltrim/${c.name}"))
    combined.zip(right ++ left).foreach {
      case (qualified, original) =>
        assert(qualified.copy(name = original.name) == original)
    }
    val groups = TrimExpressionBenchmark.collections
    val options = parseArgs(Array("17", "8", seed.toString, "aligned"), combined, groups)
    assert(selectedCases(options, combined, groups) == combined)
  }

  test("combined selectors accept qualified identity cases and reject ambiguous short names") {
    val cases = TrimExpressionBenchmark.cases
    val groups = TrimExpressionBenchmark.collections
    Seq("rtrim/identity-l10", "ltrim/identity-l10").foreach {
      name =>
        val options = parseArgs(Array("17", "8", seed.toString, name), cases, groups)
        assert(selectedCases(options, cases, groups).map(_.name) == Seq(name))
    }
    Seq("identity-l10", "rtrim-l10-none", "unknown/identity-l10").foreach {
      name =>
        intercept[IllegalArgumentException] {
          parseArgs(Array("17", "8", seed.toString, name), cases, groups)
        }
    }
  }

  Seq("vanilla", "native").foreach {
    engine =>
      test(s"combined aligned $engine process registers exactly 82 single-engine groups") {
        val cases = TrimExpressionBenchmark.cases
        val groups = TrimExpressionBenchmark.collections
        val options = parseArgs(
          Array("17", "8", seed.toString, "aligned", engine, "vanilla-first"),
          cases,
          groups)
        val selected = selectedCases(options, cases, groups)
        assert(selected.size == 82)
        selected.foreach {
          scenario =>
            val prepared = new RecordingPrepared
            val benchmark = new Benchmark(scenario.name, options.rows.toLong)
            registerCases(benchmark, prepared, options)
            assert(prepared.calls.isEmpty)
            assert(benchmark.benchmarks.size == 1)
            val expected =
              if (engine == "vanilla") "Vanilla / UnsafeRow"
              else "Gluten / Velox vectors"
            assert(benchmark.benchmarks.head.name == expected)
            benchmark.benchmarks.head.fn(new Benchmark.Timer(0))
            assert(prepared.calls.toSeq == Seq(engine))
        }
      }
  }
}
// scalastyle:on nonascii
