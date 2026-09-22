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
      direction <- Seq("rtrim", "ltrim", "trim")
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
          assert(
            (if (direction == "rtrim") value.endsWith("  ")
             else if (direction == "ltrim") value.startsWith("  ")
             else value.startsWith(" ") && value.endsWith(" ")) == selected)
          val body =
            if (direction == "rtrim") value.stripSuffix("  ")
            else if (direction == "ltrim") value.stripPrefix("  ")
            else value.stripPrefix(" ").stripSuffix(" ")
          assert(body.take(8) == f"${(i - local + source).toLong ^ seed}%08x")
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
        val body = "01352830" + "\u4e30" * 18
        assert(utf == Seq(
          if (direction == "rtrim") body + "  " else "  " + body,
          "01352831" + "\u4e31" * 18 + "pg"))
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

  test("two-sided trim preserves NULL sampling and UTF8 body bytes") {
    val right = rows(trimPlan("rtrim/l64-null50"), 200, 17)
    val both = rows(trimPlan("trim/l64-null50"), 200, 17)
    assert(both.count(_.isNullAt(0)) == 100)
    right.zip(both).foreach {
      case (r, b) =>
        assert(r.isNullAt(0) == b.isNullAt(0))
        if (!r.isNullAt(0)) {
          assert(r.getUTF8String(0).toString.trim == b.getUTF8String(0).toString.trim)
        }
    }
    val utf = rows(trimPlan("trim/l64-utf8-half-even"), 2, 2)
      .map(_.getUTF8String(0).toString)
    assert(utf == Seq(" " + "01352830" + "\u4e30" * 18 + " ", "01352831" + "\u4e31" * 18 + "pg"))
    assert(utf.forall(_.getBytes(UTF_8).length == 64))
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
