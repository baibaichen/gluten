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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

import org.scalatest.BeforeAndAfterAll

import java.nio.charset.StandardCharsets.UTF_8

// scalastyle:off nonascii
class LTrimExpressionBenchmarkSuite extends SparkFunSuite with BeforeAndAfterAll {
  import ExpressionBenchmark._

  private val seed = 20260912L
  private val widths = Seq(10, 12, 13, 64, 256)
  private val patterns = Seq("none", "first", "cluster", "half-even", "last", "penultimate", "all")
  private val alignedNames =
    (for {
      width <- widths
      pattern <- patterns
    } yield s"ltrim-l$width-$pattern") ++ Seq(
      "ltrim-l64-null50",
      "ltrim-l64-utf8-half-even",
      "identity-l10",
      "identity-l256",
      "ltrim-trailing-boundary",
      "ltrim-custom")
  private val representativeNames = Seq(
    "ltrim-l10-none",
    "ltrim-l12-half-even",
    "ltrim-l13-half-even",
    "ltrim-l64-half-even",
    "ltrim-l256-none",
    "ltrim-l256-last",
    "ltrim-l256-penultimate",
    "ltrim-l256-all",
    "ltrim-l64-null50",
    "ltrim-l64-utf8-half-even",
    "identity-l10",
    "identity-l256"
  )

  private def scenario(name: String): Case = {
    val selected = LTrimExpressionBenchmark.cases.find(_.name == name)
    assert(selected.isDefined, s"Missing aligned LTrim case: $name")
    selected.get
  }

  private def inputStrings(prepared: Prepared): Seq[String] = prepared.vanillaRows.map {
    row => if (row.isNullAt(0)) null else row.getUTF8String(0).toString
  }.toVector

  override def afterAll(): Unit = {
    try {
      shutdown()
    } finally {
      super.afterAll()
    }
  }

  test("ltrim preserves trailing text using the shared UnsafeRow and native vector runner") {
    val values = Seq(null, "", "   ", "  left", "right  ", "  中文", "  " + "x" * 64)
    val scenario = Case(
      "golden-left",
      new StructType().add("input", StringType, nullable = true),
      attributes => Seq(StringTrimLeft(attributes.head)),
      (_, row) => InternalRow(UTF8String.fromString(values(row)))
    )
    withPrepared(scenario, values.size, batchSize = 3, seed = 0L) {
      prepared =>
        val expected = Seq(null, "", "", "left", "right  ", "中文", "x" * 64)
        assert(prepared.vanillaRows.forall(_.isInstanceOf[UnsafeRow]))
        assert(prepared.nativeBatchSizes == Seq(3, 3, 1))
        assert(prepared.nativeInputValues == values.map(value => Seq(value)))
        assert(prepared.vanillaResults == expected)
        assert(prepared.nativeResults == expected)
        prepared.runVanilla()
        prepared.runNative()
        prepared.verify()
    }
  }

  test("leading and trailing generators share seeded bodies but trim opposite ends") {
    val left = scenario("ltrim-l12-half-even")
    val right = RTrimExpressionBenchmark.cases.find(_.name == "rtrim-l12-half-even").get
    val trailingResults = withPrepared(right, 3, 3, seed) {
      trailing =>
        assert(
          inputStrings(trailing) ==
            Seq("01352830ct  ", "01352831hypg", "01352832md  "))
        trailing.verify()
        trailing.nativeResults
    }
    withPrepared(left, 3, 3, seed) {
      leading =>
        assert(
          inputStrings(leading) ==
            Seq("  01352830ct", "01352831hypg", "  01352832md"))
        assert(leading.vanillaResults == trailingResults)
        assert(leading.nativeResults == trailingResults)
        assert(leading.vanillaRows.forall(_.getUTF8String(0).numBytes() == 12))
        leading.verify()
    }
    withPrepared(scenario("ltrim-l64-cluster"), 145, 128, seed) {
      prepared =>
        val values = inputStrings(prepared)
        assert(
          values.indices.filter(i => values(i).startsWith("  ")) ==
            ((0 until 16) ++ (128 until 144)))
        assert(values(127).nonEmpty && values(144).nonEmpty)
        assert(values(128).drop(2).take(8) != values(0).drop(2).take(8))
        prepared.verify()
    }
  }

  test("ltrim boundary cases preserve the opposite side and custom trailing characters") {
    val boundary = LTrimExpressionBenchmark.cases.find(_.name == "ltrim-trailing-boundary").get
    withPrepared(boundary, 2, 1, 1L) {
      prepared =>
        val expected = "x" * 60 + "  "
        assert(prepared.vanillaResults == Seq(expected, expected))
        assert(prepared.nativeResults == Seq(expected, expected))
    }
    val custom = LTrimExpressionBenchmark.cases.find(_.name == "ltrim-custom").get
    withPrepared(custom, 1, 1, 2L) {
      prepared =>
        assert(prepared.nativeInputValues == Seq(Seq("xyvaluexy", "xy")))
        assert(prepared.vanillaResults == Seq("valuexy"))
        assert(prepared.nativeResults == Seq("valuexy"))
    }
  }

  test("each aligned leading case has identical engine inputs across actual partial batches") {
    assert(LTrimExpressionBenchmark.cases.map(_.name) == alignedNames)
    LTrimExpressionBenchmark.cases.foreach {
      selected =>
        Seq(1, 8, 17).foreach {
          batchSize =>
            withClue(s"${selected.name}, batchSize=$batchSize: ") {
              withPrepared(selected, 17, batchSize, seed) {
                prepared =>
                  val expectedInputs = prepared.vanillaRows.map {
                    row =>
                      selected.inputSchema.indices.map {
                        ordinal =>
                          if (row.isNullAt(ordinal)) null else row.getUTF8String(ordinal).toString
                      }.toVector
                  }.toVector
                  assert(prepared.nativeInputValues == expectedInputs)
                  val expected = prepared.vanillaResults
                  assert(prepared.nativeResults == expected)
                  val signature = expected.map {
                    value => if (value == null) -1L else value.getBytes(UTF_8).length.toLong
                  }.sum
                  (0 until 2).foreach {
                    _ =>
                      prepared.runVanilla()
                      prepared.runNative()
                      assert(prepared.vanillaSignature == signature)
                      assert(prepared.nativeSignature == signature)
                  }
                  prepared.verify()
              }
            }
        }
    }
    val options = parseArgs(Array("17", "4", "5", "ltrim-custom"), LTrimExpressionBenchmark.cases)
    assert(options.rows == 17 && options.batchSize == 4 && options.seed == 5L)
    intercept[IllegalArgumentException] {
      parseArgs(Array("17", "4", "5", "rtrim-custom"), LTrimExpressionBenchmark.cases)
    }
  }

  test("actual ltrim terminal preserves no-trim carriers and clears nullable multi-input results") {
    val values = Seq(
      Seq("kept  ", "xy"),
      Seq(null, "xy"),
      Seq("", "xy"),
      Seq("xyxy", "xy"),
      Seq("xy中文", "xy"),
      Seq("xyvalue", null),
      Seq("xyvalue", "xy"))
    val expected = Seq("kept  ", null, "", "", "中文", null, "value")
    val selected = Case(
      "terminal-ltrim-custom",
      new StructType().add("input", StringType).add("trim", StringType),
      attrs => Seq(StringTrimLeft(attrs.head, Some(attrs(1)))),
      (_, row) => InternalRow.fromSeq(values(row).map(UTF8String.fromString))
    )
    withPrepared(selected, values.size, 3, 0L) {
      prepared =>
        val actual = prepared.vanillaRows.map {
          row =>
            val value = prepared.evaluateVanilla(row)
            if (value == null) null else value.toString
        }.toSeq
        assert(actual == expected)
        assert(prepared.vanillaResults == expected)
        assert(prepared.nativeResults == expected)
        val input = prepared.vanillaRows.head.getUTF8String(0)
        val result = prepared.evaluateVanilla(prepared.vanillaRows.head)
        assert(result.getBaseObject eq input.getBaseObject)
        assert(result.getBaseOffset == input.getBaseOffset)
        prepared.runVanilla()
        prepared.runNative()
        prepared.verify()
        assert(prepared.nativeInputValues == values)
    }
  }

  test("LTrim owns corresponding aligned and representative collections without RTrim leakage") {
    val cases = LTrimExpressionBenchmark.cases
    val groups = LTrimExpressionBenchmark.collections
    val full = parseArgs(Array("17"), cases, groups)
    assert(selectedCases(full, cases, groups) == cases)
    val single = parseArgs(Array("17", "8", "7", "ltrim-custom", "native"), cases, groups)
    assert(selectedCases(single, cases, groups).map(_.name) == Seq("ltrim-custom"))
    val aligned = parseArgs(Array("17", "8", "7", "aligned", "native"), cases, groups)
    assert(selectedCases(aligned, cases, groups).map(_.name) == alignedNames)
    val representative = parseArgs(Array("17", "8", "7", "representative", "native"), cases, groups)
    assert(selectedCases(representative, cases, groups).map(_.name) == representativeNames)
    assert(representativeNames == RTrimExpressionBenchmark.representativeCases.map {
      selected => selected.name.replace("rtrim-", "ltrim-")
    })
    intercept[IllegalArgumentException] {
      parseArgs(Array("17", "8", "7", "rtrim-l10-none", "native"), cases, groups)
    }
  }

  test("primitive ltrim signatures use result bytes for unary and custom multi-input cases") {
    val values = Seq(Seq("  中文", " "), Seq(null, " "), Seq("", " "), Seq("xyvalue", "xy"))
    val schema = new StructType().add("input", StringType).add("trim", StringType)
    Seq(false, true).foreach {
      custom =>
        val selected = Case(
          "ltrim-lengths",
          schema,
          attrs => Seq(StringTrimLeft(attrs.head, if (custom) Some(attrs(1)) else None)),
          (_, row) => InternalRow.fromSeq(values(row).map(UTF8String.fromString))
        )
        withPrepared(selected, values.size, 3, 0L) {
          prepared =>
            val expected = if (custom) 10L else 12L
            (0 until 2).foreach {
              _ =>
                prepared.runVanilla()
                prepared.runNative()
                assert(prepared.vanillaSignature == expected)
                assert(prepared.nativeSignature == expected)
            }
            assert(prepared.nativeInputValues == values)
            prepared.verify()
        }
    }
  }

  test("aligned LTrim catalog matches the ordered grid and has no obsolete 128-row case IDs") {
    val names = LTrimExpressionBenchmark.cases.map(_.name)
    assert(names == alignedNames)
    assert(names.size == 41 && names.distinct.size == 41)
    assert(!names.contains("ltrim-all-l12"))
    assert(!names.contains("ltrim-null-l64"))
    val normalizedRight = RTrimExpressionBenchmark.cases.map {
      selected =>
        selected.name.replace("rtrim-", "ltrim-").replace("leading-boundary", "trailing-boundary")
    }
    assert(names == normalizedRight)
  }

  patterns.foreach {
    pattern =>
      test(s"aligned LTrim $pattern shares RTrim bodies and actual batch-local selection") {
        widths.foreach {
          width =>
            Seq(1, 8, 17).foreach {
              batchSize =>
                withClue(s"width=$width, batchSize=$batchSize: ") {
                  val rightName = s"rtrim-l$width-$pattern"
                  val right = RTrimExpressionBenchmark.cases.find(_.name == rightName)
                  assert(right.isDefined, s"Missing aligned RTrim case: $rightName")
                  val (rightInputs, rightResults) = withPrepared(right.get, 17, batchSize, seed) {
                    prepared =>
                      prepared.verify()
                      (inputStrings(prepared), prepared.nativeResults)
                  }
                  withPrepared(scenario(s"ltrim-l$width-$pattern"), 17, batchSize, seed) {
                    prepared =>
                      val leftInputs = inputStrings(prepared)
                      leftInputs.indices.foreach {
                        index =>
                          val local = index % batchSize
                          val count = math.min(batchSize, leftInputs.size - (index - local))
                          val sourceLocal =
                            if (pattern == "penultimate" && count > 1 && local == count - 2) {
                              count - 1
                            } else if (
                              pattern == "penultimate" && count > 1 && local == count - 1
                            ) {
                              count - 2
                            } else local
                          val selected = pattern match {
                            case "none" => false
                            case "first" => sourceLocal == 0
                            case "cluster" => sourceLocal < 16
                            case "half-even" => sourceLocal % 2 == 0
                            case "last" | "penultimate" => sourceLocal == count - 1
                            case "all" => true
                          }
                          val left = leftInputs(index)
                          val right = rightInputs(index)
                          assert(left != null && left.getBytes(UTF_8).length == width)
                          assert(!left.endsWith(" "))
                          assert(left.startsWith("  ") == selected)
                          assert(right.endsWith("  ") == selected)
                          val body = if (selected) left.drop(2) else left
                          assert(body == (if (selected) right.dropRight(2) else right))
                          val sourceRow = index - local + sourceLocal
                          assert(body.take(8) ==
                            f"${(sourceRow.toLong ^ seed) & 0xffffffffL}%08x")
                      }
                      assert(prepared.nativeInputValues == leftInputs.map(Seq(_)))
                      assert(prepared.vanillaResults == rightResults)
                      assert(prepared.nativeResults == rightResults)
                      prepared.verify()
                  }
                }
            }
        }
      }
  }

  test("aligned LTrim no-trim and identity data stay nonconstant without injected empty strings") {
    Seq(10, 256).foreach {
      width =>
        val regular = withPrepared(scenario(s"ltrim-l$width-none"), 300, 7, seed) {
          prepared =>
            val values = inputStrings(prepared)
            assert(values.distinct.size == 300)
            assert(values.forall(value => value.getBytes(UTF_8).length == width))
            assert(prepared.nativeResults == values)
            prepared.verify()
            values
        }
        val identity = withPrepared(scenario(s"identity-l$width"), 300, 16, seed) {
          prepared =>
            val values = inputStrings(prepared)
            assert(prepared.vanillaResults == values)
            assert(prepared.nativeResults == values)
            prepared.verify()
            values
        }
        assert(identity == regular)
        val otherSeed =
          withPrepared(scenario(s"ltrim-l$width-none"), 300, 7, seed + 1)(inputStrings)
        assert(otherSeed != regular)
        if (width == 10) {
          assert(regular.take(4) == Seq("01352830ct", "01352831hy", "01352832md", "01352833ri"))
          assert(Seq(10239, 10240, 3999999).map {
            i => scenario("ltrim-l10-none").input(seed, i).getUTF8String(0).toString
          } == Seq("01350fcfdu", "01350030iz", "010820cfri"))
        }
    }
  }

  test("aligned LTrim last and penultimate swap complete rows including the 6400-row tail") {
    Seq((1, 1), (2, 2), (17, 8), (16640, 10240)).foreach {
      case (rows, batchSize) =>
        def values(pattern: String): Seq[String] =
          withPrepared(scenario(s"ltrim-l256-$pattern"), rows, batchSize, seed) {
            prepared =>
              assert(
                prepared.nativeBatchSizes ==
                  (0 until rows by batchSize).map(start => math.min(batchSize, rows - start)))
              prepared.verify()
              inputStrings(prepared)
          }
        val last = values("last")
        val penultimate = values("penultimate")
        (0 until rows by batchSize).foreach {
          start =>
            val original = last.slice(start, start + batchSize)
            val swapped = penultimate.slice(start, start + batchSize)
            val expected =
              if (original.size < 2) original
              else original.dropRight(2) ++ Seq(original.last, original(original.size - 2))
            assert(swapped == expected && swapped.sorted == original.sorted)
            assert(original.count(_.startsWith("  ")) == 1)
            assert(swapped.count(_.startsWith("  ")) == 1)
            assert(
              original.last.drop(2).take(8) ==
                f"${(start + original.size - 1L) ^ seed}%08x")
        }
        if (rows == 16640) {
          assert(last.last.drop(2).take(8) == "013568cf")
          assert(penultimate(penultimate.size - 2) == last.last)
          assert(penultimate.last.take(8) == "013568ce")
        }
    }
  }

  test("aligned LTrim first and cluster restart in full and partial native batches") {
    Seq("first" -> 1, "cluster" -> 16).foreach {
      case (pattern, selectedCount) =>
        withPrepared(scenario(s"ltrim-l64-$pattern"), 16640, 10240, seed) {
          prepared =>
            val values = inputStrings(prepared)
            assert(prepared.nativeBatchSizes == Seq(10240, 6400))
            Seq(0, 10240).foreach {
              start =>
                val batch = values.slice(start, start + 10240)
                assert(
                  batch.indices.filter(i => batch(i).startsWith("  ")) ==
                    (0 until selectedCount))
            }
            assert(values(10240).drop(2).take(8) == "01350030")
            prepared.verify()
        }
    }
  }

  test("aligned LTrim NULL and UTF8 cases use the historical RTrim sampling and byte bodies") {
    val right = RTrimExpressionBenchmark.cases.find(_.name == "rtrim-l64-null50").get
    val rightValues = withPrepared(right, 200, 200, seed)(inputStrings)
    withPrepared(scenario("ltrim-l64-null50"), 200, 200, seed) {
      prepared =>
        val values = inputStrings(prepared)
        val nullRows = values.indices.filter(i => values(i) == null)
        assert(nullRows == values.indices.filter(i => ((i % 200) * 73 + 37) % 200 < 100))
        assert(nullRows.size == 100 && nullRows.count(_ % 2 == 0) == 50)
        assert(values.count(value => value != null && value.startsWith("  ")) == 50)
        assert(
          values.map(v => if (v == null) null else v.dropWhile(_ == ' ')) ==
            rightValues.map(v => if (v == null) null else v.reverse.dropWhile(_ == ' ').reverse))
        prepared.verify()
    }
    withPrepared(scenario("ltrim-l64-utf8-half-even"), 2, 2, seed) {
      prepared =>
        val expected = Seq("  01352830" + "丰" * 18, "01352831" + "丱" * 18 + "pg")
        assert(inputStrings(prepared) == expected)
        assert(expected.forall(_.getBytes(UTF_8).length == 64))
        assert(prepared.nativeResults == Seq(expected.head.drop(2), expected(1)))
        prepared.verify()
    }
  }

  test("aligned boundary and custom generators share NULL positions and preserve the other end") {
    Seq("rtrim-leading-boundary" -> "ltrim-trailing-boundary", "rtrim-custom" -> "ltrim-custom")
      .foreach {
        case (rightName, leftName) =>
          val right = RTrimExpressionBenchmark.cases.find(_.name == rightName).get
          val rightInputs = withPrepared(right, 202, 17, seed)(_.nativeInputValues)
          withPrepared(scenario(leftName), 202, 17, seed) {
            prepared =>
              assert(prepared.nativeInputValues == rightInputs)
              assert(prepared.nativeResults == prepared.vanillaResults)
              val expected = rightInputs.map {
                input =>
                  val value = input.head
                  if (value == null || (input.size == 2 && input(1) == null)) null
                  else {
                    val chars = if (input.size == 2) input(1) else " "
                    value.dropWhile(chars.contains(_))
                  }
              }
              assert(prepared.nativeResults == expected)
              assert(
                expected.exists(value => value != null && value.endsWith("  ")) ||
                  leftName == "ltrim-custom")
              if (leftName == "ltrim-custom") {
                assert(expected.exists(value => value != null && value.endsWith("xy")))
              } else {
                assert(rightInputs.exists(_.head == ""))
              }
              prepared.verify()
          }
      }
  }

  test("aligned LTrim cases retain empty correctness input support") {
    Seq("ltrim-l10-none", "ltrim-l256-last", "ltrim-l256-penultimate", "identity-l10").foreach {
      name =>
        withPrepared(scenario(name), 0, 8, seed) {
          prepared =>
            assert(prepared.vanillaRows.isEmpty && prepared.nativeBatchSizes.isEmpty)
            prepared.runVanilla()
            prepared.runNative()
            assert(prepared.vanillaSignature == 0L && prepared.nativeSignature == 0L)
            prepared.verify()
        }
    }
  }
}
// scalastyle:on nonascii
