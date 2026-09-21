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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._

// scalastyle:off nonascii
class CatalogSuite extends SparkFunSuite {
  import Catalog._

  private def table(
      inputs: String = "`input = standard.string(length = 10)`",
      sql: String = "`trim(input)`",
      description: String = "Plain description"): String =
    s"""# Inline fixture
       |
       || case | inputs | expression | description |
       || --- | --- | --- | --- |
       || example | $inputs | $sql | $description |
       |""".stripMargin

  test("Markdown preserves one complete code span including SQL punctuation and escapes") {
    val sql = """concat(`input`, 'a,b;(x)', 'quote''s', '\\d+', 'a\|b', '\\\|')"""
    val parsed = parseMarkdown("trim", "inline.md", table(sql = s"``$sql``"))
    assert(parsed.size == 1)
    assert(parsed.head.id == "trim/example")
    assert(parsed.head.sql == sql.replace("\\|", "|"))
    assert(parsed.head.inputs.head.names == Seq("input"))
    assert(parsed.head.sourceLocation.file == "inline.md")
    assert(parsed.head.sourceLocation.line == 5)
    assert(parsed.head.sourceLocation.column > 0)
  }

  test("Markdown retains Unicode JSON escapes literal backslashes and escaped table pipes") {
    val sql = """concat(input, '\\d+', 'a\|b', '\\\|', '中文')"""
    val description = "中文说明"
    val result = parseMarkdown(
      "trim",
      "inline.md",
      table(
        sql = s"```$sql```",
        description = description)).head
    assert(result.description == description)
    assert(result.sql == """concat(input, '\\d+', 'a|b', '\\|', '中文')""")
    val binding = parseInputs(
      """input = rtrimPattern(length = 10, pattern = "none")""",
      SourceLocation("inline.md", 5, 13),
      "trim/example").head
    assert(binding.arguments == Seq(
      "length" -> LongArgument(10),
      "pattern" -> StringArgument("none")))
  }

  test("Markdown accepts lowercase ASCII case and function IDs with underscores and hyphens") {
    val names = Seq("trim_variant-2", "0_variant", "_variant", "-variant", "substr", "substring")
    names.foreach {
      name =>
        val markdown = table().replace("| example |", s"| $name |")
        assert(parseMarkdown(name, s"$name.md", markdown).head.id == s"$name/$name")
        assert(parseIndex(s"$name.md\n", "index.txt") == Seq(s"$name.md"))
    }
    Seq("TRIM", "trim/variant", "../trim", "trim.variant", "中文").foreach {
      function =>
        val error = intercept[IllegalArgumentException] {
          parseMarkdown(function, "inline.md", table())
        }
        assert(error.getMessage.contains("inline.md:"))
        assert(error.getCause != null)
    }
  }

  test("Markdown accepts plain explanation paragraphs before and after the case table") {
    val markdown = "Before the table.\nAnother plain line.\n\n" + table() +
      "\n表后普通说明。\n"
    val cases = parseMarkdown("trim", "inline.md", markdown)
    assert(cases.size == 1 && cases.head.id == "trim/example")
    assert(cases.head.sourceLocation.line == 8)
    Seq(
      "<b>HTML</b>",
      "[link](https://example.com)",
      "`input = standard.int()`",
      "*markup*",
      "| broken | table row",
      "broken | table row").foreach {
      paragraph =>
        Seq(paragraph + "\n\n" + table(), table() + "\n" + paragraph).foreach {
          text =>
            val error = intercept[IllegalArgumentException] {
              parseMarkdown("trim", "inline.md", text)
            }
            assert(error.getMessage.contains("inline.md:"))
            assert(error.getCause != null)
        }
    }
  }

  test("registered trim pattern generators have explicit names distinct from SQL functions") {
    Seq("rtrim", "ltrim").foreach {
      direction =>
        val text = s"""input = ${direction}Pattern(length = 10, pattern = "none")"""
        val binding = parseInputs(text, SourceLocation("inline.md", 1, 1), "trim/example")
        assert(binding.head.generator == s"${direction}Pattern")
        val plan = Data.compile(binding)
        assert(plan.row(Data.Context(1), 0L, 0, 1).getUTF8String(0)
          .toString == "01352830ct")
        intercept[IllegalArgumentException] {
          parseInputs(
            text.replace(s"${direction}Pattern", direction),
            SourceLocation("inline.md", 1, 1),
            "trim/example")
        }
    }
  }

  test("Markdown rejects duplicate empty and invalid case IDs with physical row context") {
    Seq("example", "", "trim/example", "bad.name").foreach {
      id =>
        val extra = s"| $id | `input = standard.int()` | `input` | extra |\n"
        val error = intercept[IllegalArgumentException] {
          parseMarkdown("trim", "inline.md", table() + extra)
        }
        assert(error.getMessage.contains("inline.md:6:"))
        assert(error.getMessage.contains("case=trim/"))
        assert(error.getCause != null)
    }
  }

  test("input syntax errors report their physical file column and original cause") {
    val error = intercept[IllegalArgumentException] {
      parseInputs("input = standard.int(1)", SourceLocation("inline.md", 5, 13), "trim/example")
    }
    assert(error.getMessage.startsWith("inline.md:5:34 case=trim/example:"))
    assert(error.getCause.isInstanceOf[IllegalArgumentException])
  }

  test("Markdown input errors retain physical columns after escaped pipes") {
    val fixtures = Seq(
      """input = rtrimPattern(length = 10, pattern = "a\|b")?""",
      """input = rtrimPattern(length = 10, pattern = "a\|b\|c")?""",
      """input = rtrimPattern(length = 10, pattern = "a\\\|b\\\\\|c")?""",
      """input = rtrimPattern(length = 10, pattern = "中文𠮷\|a\|b")?"""
    )
    fixtures.zipWithIndex.foreach {
      case (input, index) =>
        val delimiter = "`" * (index + 1)
        val markdown = table(inputs = s"$delimiter$input$delimiter")
        val row = markdown.linesIterator.toVector(4)
        val column = row.indexOf('?') + 1
        val error = intercept[IllegalArgumentException] {
          parseMarkdown("trim", "inline.md", markdown)
        }
        withClue(row) {
          assert(error.getMessage.startsWith(s"inline.md:5:$column case=trim/example:"))
          assert(error.getMessage.contains("Invalid inputs:"))
          assert(error.getCause.isInstanceOf[IllegalArgumentException])
        }
    }
  }

  test("input grammar decodes strict JSON strings without splitting their punctuation") {
    val text = """(input, trimChars) = rtrimCustom(); value = standard.int()"""
    val bindings = parseInputs(text, SourceLocation("inline.md", 5, 13), "trim/example")
    assert(bindings.map(_.names) == Seq(Seq("input", "trimChars"), Seq("value")))
    val escapedUnicode = "\\" + "u4e2d"
    val weird = """input = rtrimPattern(length = 10, pattern = "a,b;(x)\"\\""" +
      escapedUnicode + "\", utf8 = true)"
    val error = intercept[IllegalArgumentException] {
      parseInputs(weird, SourceLocation("inline.md", 5, 13), "trim/example")
    }
    assert(error.getCause != null)
    assert(error.getMessage.contains("a,b;(x)\"\\\u4e2d"))
  }

  test("Markdown rejects malformed table and non-code or mixed cells with source context") {
    val invalid = Seq(
      table(inputs = "input = standard.int()"),
      table(inputs = "prefix `input = standard.int()`"),
      table(sql = "[trim](https://example.com)"),
      table(sql = "<b>trim(input)</b>"),
      table(sql = "`trim(input)` extra"),
      table(sql = "``"),
      table(description = "<br>"),
      table(description = "[link](https://example.com)"),
      table().replace("| Plain description |", "| Plain description | extra |"),
      table().replace(" | Plain description |", " |"),
      table().replace("| `trim(input)` |", "||"),
      table() + "\n" + table(),
      table().replace("trim(input)", "trim(\ninput)"),
      table().replace("| inputs | expression |", "| expression | inputs |"),
      "not a table"
    )
    invalid.foreach {
      markdown =>
        withClue(markdown) {
          val error = intercept[IllegalArgumentException] {
            parseMarkdown("trim", "inline.md", markdown)
          }
          assert(error.getMessage.contains("inline.md:"))
          assert(error.getMessage.contains("case="))
          assert(error.getCause != null)
        }
    }
  }

  test("input grammar rejects arbitrary syntax and invalid registered arguments") {
    val invalid = Seq(
      "",
      "input = unknown()",
      "input = standard.int(value = 1)",
      "input = standard.int();",
      "input = standard.int() // comment",
      "input = standard.int(); input = standard.int()",
      "input = standard.int(); INPUT = standard.int()",
      "(input, INPUT) = rtrimCustom()",
      "input = rtrimCustom()",
      "(input, chars) = standard.int()",
      "input = standard.string(length = null)",
      "input = standard.string(length = true)",
      "input = standard.string(length = \"10\")",
      "input = standard.string(length = -1)",
      "input = standard.string(length = 2147483648)",
      "input = standard.string(length = 9223372036854775808)",
      "input = standard.string(length = 10, length = 12)",
      "input = standard.string(length = 1.5)",
      "input = standard.int(foo = standard.int())",
      "input = standard.int().toString",
      "in-put = standard.int()",
      "input = rtrimPattern(length = 10, pattern = 'none')",
      "input = rtrimPattern(length = 10, pattern = \"\\q\")",
      "input = rtrimPattern(length = 10, pattern = \"none\", nullPercent = 101)",
      "input = rtrimPattern(length = 1, pattern = \"all\")",
      "input = rtrimPattern(length = 10)",
      "input = rtrimPattern(pattern = \"none\")",
      "input = rtrimPattern(length = 10, pattern = \"none\", utf8 = 1)",
      "input = standard.int(/* comment */)",
      "input = standard.int(); // comment",
      "input = standard.int(); \u4e2d = standard.int()",
      "input = standard.int(x = +1)",
      "input = standard.string(length = 01)",
      "input = standard.string(length = 1L)",
      "input = standard.string(length = 1e1)",
      "input = standard.string(length = 10,)",
      "() = standard.int()",
      "(input,) = standard.int()",
      "((input, chars)) = rtrimCustom()",
      "input = rtrimPattern(length = 10, pattern = \"none\", utf8 = True)",
      "input = rtrimPattern(length = 10, pattern = \"line\tbreak\")",
      "input = rtrimPattern(length = 10, pattern = \"\\uZZZZ\")",
      "input = rtrimPattern(length = 10, pattern = \"none\", nullPercent = -1)"
    )
    invalid.foreach {
      text =>
        withClue(text) {
          val error = intercept[IllegalArgumentException] {
            parseInputs(text, SourceLocation("inline.md", 5, 13), "trim/example")
          }
          assert(error.getMessage.contains("inline.md:5:"))
          assert(error.getMessage.contains("case=trim/example"))
          assert(error.getCause != null)
        }
    }
  }

  test("standard projections preserve calibration values correlations and nested schemas") {
    import Data._
    import org.apache.spark.sql.types._

    val names = Seq(
      "long",
      "int",
      "string",
      "double",
      "timestamp",
      "date",
      "binary",
      "intArray",
      "struct",
      "boundedInt",
      "stringArray",
      "timestampString")
    val input = names.zipWithIndex.map {
      case (name, i) =>
        val args = if (name == "string") "length = 3" else ""
        s"c$i = standard.$name($args)"
    }.mkString("; ")
    val plan = compile(parseInputs(input, SourceLocation("inline.md", 1, 1), "standard/all"))
    val expected = StructType(Seq(
      StructField("c0", LongType, nullable = true),
      StructField("c1", IntegerType, nullable = true),
      StructField("c2", StringType, nullable = true),
      StructField("c3", DoubleType, nullable = true),
      StructField("c4", TimestampType, nullable = true),
      StructField("c5", DateType, nullable = true),
      StructField("c6", BinaryType, nullable = false),
      StructField("c7", ArrayType(IntegerType, containsNull = false), nullable = false),
      StructField(
        "c8",
        StructType(Seq(
          StructField("f1", LongType, nullable = false),
          StructField("f2", StringType, nullable = false))),
        nullable = false),
      StructField("c9", IntegerType, nullable = false),
      StructField("c10", ArrayType(StringType, containsNull = false), nullable = false),
      StructField("c11", StringType, nullable = false)
    ))
    assert(plan.inputSchema == expected)
    val context = Context(1005, Some(1001), seed = 7L)
    Seq(0L, 1L, 1000L, 1001L, 1002L).foreach {
      i =>
        val row = plan.row(context, i, 0, 1)
        val key = i % 1001
        val padded = if (key < 1000) f"$key%03d" else key.toString
        assert(row.getLong(0) == key)
        assert(row.getInt(1) == key.toInt)
        assert(row.getUTF8String(2).toString == padded)
        assert(row.getDouble(3) == key.toDouble)
        assert(row.getLong(4) == key * 1000000L)
        assert(row.getInt(5) == 18262 + key.toInt)
        assert(new String(
          row.getBinary(6),
          java.nio.charset.StandardCharsets.UTF_8) == f"$key%010d")
        assert(row.getArray(7).toIntArray().toSeq == Seq(
          key.toInt,
          ((i + 1) % 1001).toInt,
          ((i + 2) % 1001).toInt))
        assert(row.getStruct(8, 2).getLong(0) == key)
        assert(row.getStruct(8, 2).getUTF8String(1).toString == f"$key%010d")
        assert(row.getInt(9) == (key % 20).toInt)
        assert(row.getArray(10).getUTF8String(0).toString == f"$key%010d")
        assert(row.getArray(10).getUTF8String(1).toString == "fixed")
        assert(row.getUTF8String(11).toString == "2020-01-02 03:04:05")
        assert((0 until 12).forall(!row.isNullAt(_)))
        assert(row == plan.row(context.copy(seed = 999L), i, 0, 1))
    }
    assert(Context(0).keys == 1)
    assert(Context(8).keys == 8)
    assert(Context(2, Some(100)).keys == 100)
    assert(plan.row(Context(2, Some(100)), 1L, 0, 1).getArray(7).getInt(2) == 3)
    intercept[IllegalArgumentException](Context(-1))
    intercept[IllegalArgumentException](Context(1, Some(0)))
    intercept[IllegalArgumentException](plan.row(Context(0), 0L, 0, 1))
  }

  test("standard projections use independent string widths and defaults regardless of order") {
    import Data._
    val source = SourceLocation("inline.md", 1, 1)
    val inputs = "input = standard.string(); values = standard.stringArray(length = 20); " +
      "needle = standard.int(); items = standard.intArray()"
    val bindings = parseInputs(inputs, source, "standard/order")
    val forward = compile(bindings)
    val reverse = compile(bindings.reverse)
    val context = Context(25, Some(7), seed = Long.MinValue)
    Seq(8L, 0L, 6L, 7L, 1L, 8L).foreach {
      i =>
        val row = forward.row(context, i, 0, 1)
        val other = reverse.row(context, i, 0, 1)
        assert(row.getUTF8String(0).toString == f"${i % 7}%010d")
        assert(row.getArray(1).getUTF8String(0).toString == f"${i % 7}%020d")
        assert(row.getInt(2) == row.getArray(3).getInt(0))
        assert(row.getUTF8String(0) == other.getUTF8String(3))
        assert(row.getArray(1) == other.getArray(2))
        assert(row.getInt(2) == other.getInt(1))
        assert(row.getArray(3) == other.getArray(0))
    }
    val defaults = compile(parseInputs(
      "a = standard.string(); b = standard.stringArray()",
      source,
      "standard/defaults"))
    assert(defaults.row(Context(1), 0, 0, 1).getUTF8String(0).toString == "0000000000")
    assert(defaults.row(Context(1), 0, 0, 1).getArray(1).getUTF8String(0).toString == "0000000000")
    val zero = compile(parseInputs("input = standard.string(length = 0)", source, "standard/zero"))
    assert(zero.row(Context(1), 0, 0, 1).getUTF8String(0).toString == "0")
    val int = compile(parseInputs("input = standard.int()", source, "standard/int"))
    assert(int.row(Context(Long.MaxValue), 2147483648L, 0, 1).getInt(0) == Int.MinValue)
    intercept[IllegalArgumentException](forward.row(context, 0L, 1, 2))
    intercept[IllegalArgumentException](forward.row(context, 24L, 0, 2))
    intercept[IllegalArgumentException](forward.row(context, 0L, 0, 0))
  }

  test("standard string and stringArray bindings accept different explicit lengths") {
    import Data._
    val inputs = "a = standard.string(length = 20); b = standard.string(length = 10); " +
      "c = standard.stringArray(length = 20); d = standard.stringArray(length = 10)"
    val plan = compile(parseInputs(inputs, SourceLocation("inline.md", 1, 1), "standard/widths"))
    val row = plan.row(Context(1), 0, 0, 1)
    assert(row.getUTF8String(0).toString == "0" * 20)
    assert(row.getUTF8String(1).toString == "0" * 10)
    assert(row.getArray(2).getUTF8String(0).toString == "0" * 20)
    assert(row.getArray(3).getUTF8String(0).toString == "0" * 10)
  }

  test("single scalar and tuple bindings match merged rows and retain position validation") {
    import Data._
    Seq(
      "input = standard.int()",
      "(input, chars) = rtrimCustom()",
      "(input, chars) = ltrimCustom()").foreach {
      text =>
        val bindings = parseInputs(text, SourceLocation("inline.md", 1, 1), "single/binding")
        val single = compile(bindings)
        val merged = compile(bindings :+ Binding(Seq("extra"), "standard.long", Nil))
        assert(single.inputSchema.fields.toSeq == merged.inputSchema.fields.toSeq.dropRight(1))
        val context = Context(10, seed = 0L)
        (0 until 10).foreach {
          i =>
            val local = i % 4
            val count = math.min(4, 10 - i + local)
            val row = single.row(context, i, local, count)
            val combined = merged.row(context, i, local, count)
            assert(row.numFields == single.inputSchema.length)
            single.inputSchema.fields.zipWithIndex.foreach {
              case (field, ordinal) =>
                assert(row.get(ordinal, field.dataType) == combined.get(ordinal, field.dataType))
            }
            assert(combined.getLong(row.numFields) == i.toLong)
        }
        Seq(single, merged).foreach {
          plan =>
            Seq((-1L, 0, 1), (10L, 0, 1), (0L, 1, 2), (9L, 0, 2), (0L, 0, 0)).foreach {
              case (id, local, count) =>
                intercept[IllegalArgumentException](plan.row(context, id, local, count))
            }
        }
    }
  }

  private val seed = 20260912L
  private val widths = Seq(10, 12, 13, 64, 256)
  private val patterns = Seq("none", "first", "cluster", "half-even", "last", "penultimate", "all")

  private def trimInputs(id: String): String = {
    val Array(direction, name) = id.split("/")
    val grid = "l([0-9]+)-(.+)".r
    name match {
      case "custom" => s"(input, trimChars) = ${direction}Custom()"
      case "leading-boundary" | "trailing-boundary" => s"input = ${direction}Boundary()"
      case grid(width, "null50") =>
        s"""input = ${direction}Pattern(length = $width, pattern = "half-even", nullPercent = 50)"""
      case grid(width, "utf8-half-even") =>
        s"""input = ${direction}Pattern(length = $width, pattern = "half-even", utf8 = true)"""
      case grid(width, pattern) =>
        s"""input = ${direction}Pattern(length = $width, pattern = "$pattern")"""
      case identity if identity.startsWith("identity-l") =>
        val width = identity.stripPrefix("identity-l")
        s"""input = ${direction}Pattern(length = $width, pattern = "none")"""
    }
  }

  private def trimPlan(id: String): Data.Plan =
    Data.compile(parseInputs(
      trimInputs(id),
      SourceLocation("fixture", 1, 1),
      id))

  private def rows(
      plan: Data.Plan,
      count: Int,
      batch: Int,
      runSeed: Long = seed): Seq[InternalRow] = {
    val context = Data.Context(count, seed = runSeed)
    (0 until count).map {
      i =>
        val local = i % batch
        val row = plan.row(context, i.toLong, local, math.min(batch, count - (i - local)))
        assert(row.numFields == plan.inputSchema.length)
        assert(plan.inputSchema.indices.forall(
          i => plan.inputSchema(i).nullable || !row.isNullAt(i)))
        row
    }
  }

  test("trim grids use actual full partial and singleton batch positions at every width") {
    for {
      direction <- Seq("rtrim", "ltrim")
      width <- widths
      pattern <- patterns
      (count, batch) <- Seq((1, 1), (8, 3), (17, 8), (33, 17), (16640, 10240))
    } {
      val values = rows(trimPlan(s"$direction/l$width-$pattern"), count, batch)
        .map(_.getUTF8String(0).toString)
      values.zipWithIndex.foreach {
        case (value, i) =>
          val local = i % batch
          val size = math.min(batch, count - (i - local))
          val source =
            if (pattern == "penultimate" && size > 1 && local == size - 2) {
              size - 1
            } else if (pattern == "penultimate" && size > 1 && local == size - 1) {
              size - 2
            } else {
              local
            }
          val selected = pattern match {
            case "none" => false
            case "first" => source == 0
            case "cluster" => source < 16
            case "half-even" => source % 2 == 0
            case "last" | "penultimate" => source == size - 1
            case "all" => true
          }
          assert(value.getBytes(UTF_8).length == width)
          assert((if (direction == "rtrim") value.endsWith("  ")
                  else value.startsWith("  ")) == selected)
          val body = if (direction == "rtrim") value.stripSuffix("  ")
          else value.stripPrefix("  ")
          assert(body.take(8) == f"${(i - local + source).toLong ^ seed}%08x")
      }
    }
  }

  test("trim patterns fill short bodies and retain direction-specific spaces") {
    for (width <- 2 to 9; pattern <- Seq("none", "all"); unicode <- Seq(false, true)) {
      val values = Seq("rtrim", "ltrim").map {
        direction =>
          val input = s"""input = ${direction}Pattern(length = $width, """ +
            s"""pattern = "$pattern", utf8 = $unicode)"""
          val plan = Data.compile(
            parseInputs(input, SourceLocation("short-width", 1, 1), direction))
          rows(plan, 3, 2).map(_.getUTF8String(0).toString)
      }
      values.head.zip(values(1)).foreach {
        case (right, left) =>
          assert(right.getBytes(UTF_8).length == width && left.getBytes(UTF_8).length == width)
          assert(!right.contains(0.toChar) && !left.contains(0.toChar))
          if (pattern == "all") {
            assert(right.endsWith("  ") && left.startsWith("  "))
            assert(right.dropRight(2) == left.drop(2))
          } else assert(right == left)
      }
    }
  }

  test("trim golden values preserve NULL sampling UTF8 and whole-row penultimate swaps") {
    Seq("rtrim", "ltrim").foreach {
      direction =>
        val plain = rows(trimPlan(s"$direction/l10-none"), 300, 7).map(_.getUTF8String(0).toString)
        assert(plain.take(4) == Seq("01352830ct", "01352831hy", "01352832md", "01352833ri"))
        assert(plain.distinct.size == 300)
        assert(rows(
          trimPlan(s"$direction/identity-l10"),
          300,
          16).map(_.getUTF8String(0).toString) == plain)
        assert(rows(
          trimPlan(s"$direction/l10-none"),
          300,
          7,
          seed + 1).map(_.getUTF8String(0).toString) != plain)
        val nullRows = rows(trimPlan(s"$direction/l64-null50"), 200, 200)
        val missing = nullRows.indices.filter(nullRows(_).isNullAt(0))
        assert(missing.take(11) == Seq(0, 3, 5, 6, 8, 9, 11, 14, 16, 17, 19))
        assert(missing == nullRows.indices.filter(i => ((i % 200) * 73 + 37) % 200 < 100))
        assert(missing.size == 100 && missing.count(_ % 2 == 0) == 50)
        val utf = rows(trimPlan(s"$direction/l64-utf8-half-even"), 2, 2)
          .map(_.getUTF8String(0).toString)
        val body = "01352830" + "丰" * 18
        assert(utf == Seq(
          if (direction == "rtrim") body + "  " else "  " + body,
          "01352831" + "丱" * 18 + "pg"))
        Seq((1, 1), (2, 2), (17, 8), (16640, 10240)).foreach {
          case (count, batch) =>
            val last = rows(trimPlan(s"$direction/l256-last"), count, batch)
            val swapped = rows(trimPlan(s"$direction/l256-penultimate"), count, batch)
            last.grouped(batch).zip(swapped.grouped(batch)).foreach {
              case (before, after) =>
                assert(after == (if (before.size < 2) before
                                 else before.dropRight(2) ++ Seq(
                                   before.last,
                                   before(before.size - 2))))
            }
        }
        val boundary = if (direction == "rtrim") "leading-boundary" else "trailing-boundary"
        val edges = rows(trimPlan(s"$direction/$boundary"), 1000, 17)
        assert(edges.count(_.getUTF8String(0).numBytes() == 0) == 10)
        assert(edges.filter(_.getUTF8String(0).numBytes() != 0)
          .forall(_.getUTF8String(0).toString == "  " + "x" * 60 + "  "))
        val custom = rows(trimPlan(s"$direction/custom"), 6, 2, 0L)
        assert(custom.head.isNullAt(0) && !custom.head.isNullAt(1))
        assert(!custom(1).isNullAt(0) && custom(1).isNullAt(1))
        assert(custom(2).getUTF8String(0).toString == "xyvaluexy")
        assert(custom(2).getUTF8String(1).toString == "xy")
    }
  }

  test("resources contain exactly 51 canonical functions and 141 unique cases") {
    val cases = load()
    assert(cases.size == 141 && cases.map(_.id).distinct.size == 141)
    assert(cases.map(_.id.takeWhile(_ != '/')).distinct.size == 51)
    assert(cases.count(_.id.startsWith("rtrim/")) == 42)
    assert(cases.count(_.id.startsWith("ltrim/")) == 42)
    assert(cases.count(_.id.startsWith("trim/")) == 1)
    val directory =
      Paths.get(getClass.getClassLoader.getResource("expression-benchmark/cases").toURI)
    val index =
      parseIndex(new String(Files.readAllBytes(directory.resolve("index.txt")), UTF_8), "index.txt")
    val files = Files.list(directory)
    try assert(files.iterator().asScala.map(_.getFileName.toString).filter(_.endsWith(
        ".md")).toSet == index.toSet)
    finally files.close()
    val byId = cases.map(c => c.id -> c).toMap
    cases.filter(
      c =>
        (c.id.startsWith("rtrim/") || c.id.startsWith("ltrim/")) &&
          !c.id.endsWith("/standard-string")).foreach {
      c => assert(c.inputs == parseInputs(trimInputs(c.id), SourceLocation("catalog", 1, 1), c.id))
    }
    assert(byId("array_sort/int-array-lambda").sql ==
      "array_sort(input, (left, right) -> left - right)")
    assert(byId("map_from_arrays/standard-string-int").sql ==
      "map_from_arrays(keys, array(value, value + 1))")
    assert(byId("flatten/int-array").sql == "flatten(array(input, input))")
    assert(byId("between/standard-long").sql ==
      "input BETWEEN CAST(1 AS BIGINT) AND CAST(100 AS BIGINT)")
    assert(byId("timestamp_millis/standard-long").inputs.head.generator == "standard.long")
    assert(byId("date_trunc/standard-timestamp").inputs.head.generator == "standard.timestamp")
    assert(
      byId("unix_timestamp/standard-string").inputs.head.generator == "standard.timestampString")
    assert(cases.count(_.id.startsWith("cast/")) == 3)
    assert(cases.count(_.id.startsWith("concat/")) == 5)
    assert(cases.count(_.id.startsWith("substring/")) == 2)
    assert(cases.count(_.id.startsWith("length/")) == 2)
    cases.foreach {
      c =>
        val plan = Data.compile(c.inputs)
        assert(plan.inputSchema.nonEmpty)
        rows(plan, 2, 1)
        if (c.id.endsWith("/custom")) {
          val side = if (c.id.startsWith("rtrim")) "TRAILING" else "LEADING"
          assert(c.sql == s"TRIM($side trimChars FROM input)")
        }
        if (c.id.contains("/identity-")) assert(c.sql == "input")
    }
  }

  test("loader rejects missing indexed resources and malformed UTF8 without dropping cases") {
    def resources(values: Map[String, Array[Byte]]): ClassLoader = new ClassLoader(null) {
      override def getResourceAsStream(name: String): java.io.InputStream =
        values.get(name).map(new java.io.ByteArrayInputStream(_)).orNull
    }
    val missing = intercept[IllegalArgumentException] {
      load(resources(Map("test/index.txt" -> "trim.md\n".getBytes(UTF_8))), "test")
    }
    assert(missing.getMessage.contains("test/trim.md"))
    assert(missing.getCause.isInstanceOf[java.io.FileNotFoundException])
    val utf8 = intercept[IllegalArgumentException] {
      load(resources(Map("test/index.txt" -> Array(0xc3.toByte, 0x28.toByte))), "test")
    }
    assert(utf8.getCause.isInstanceOf[java.nio.charset.CharacterCodingException])
  }

  test("index rejects duplicates path traversal empty entries and invalid filenames") {
    assert(parseIndex("trim.md\nrtrim.md\n", "index.txt") == Seq("trim.md", "rtrim.md"))
    Seq(
      "",
      "trim.md\ntrim.md",
      "../trim.md",
      "dir/trim.md",
      "TRIM.md",
      "trim.md\n\nrtrim.md",
      "trim.md ").foreach {
      index =>
        val error = intercept[IllegalArgumentException](parseIndex(index, "index.txt"))
        assert(error.getMessage.contains("index.txt:"))
        assert(error.getCause != null)
    }
    val missing = intercept[IllegalArgumentException] {
      load(new ClassLoader(null) {}, "missing")
    }
    assert(missing.getMessage.contains("missing/index.txt"))
    assert(missing.getCause != null)
  }
}
