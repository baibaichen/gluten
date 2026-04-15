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
package org.apache.spark.sql

import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.util.matching.Regex

/**
 * Translates Velox cast error messages into the corresponding Spark exception types.
 *
 * Velox cast errors follow the format: "Cannot cast FROMTYPE 'VALUE' to TOTYPE. DETAILS" (produced
 * by makeErrorMessage in CastExpr-inl.h). This translator parses that format and constructs the
 * correct Spark exception using QueryExecutionErrors factory methods, so that the exception type
 * and message match what Spark's native Cast expression would produce.
 *
 * Velox errors arrive via JNI as GlutenException with a structured message:
 * {{{
 * Exception: VeloxUserError
 * Error Source: USER
 * Error Code: INVALID_ARGUMENT
 * Reason: Cannot cast VARCHAR 'abc' to BOOLEAN. ...
 * Retriable: False
 * Context: ...
 * }}}
 * The translator extracts the "Reason:" line to find the actual error message.
 *
 * Also translates Velox arithmetic errors (division by zero, overflow) into
 * SparkArithmeticException, and Velox datetime/collection errors into the corresponding Spark
 * exception types.
 *
 * Must be in org.apache.spark.sql package to access private[sql] QueryExecutionErrors.
 */
object VeloxCastExceptionTranslator {

  private val CastErrorRegex: Regex =
    """(?s)Cannot cast (\S+) '(.*?)' to (\S+(?:\(\d+,\s*\d+\))?)\.?\s*(.*)""".r

  private val DecimalTypeRegex: Regex = """DECIMAL\((\d+),\s*(\d+)\)""".r

  private val ReasonLineRegex: Regex = """(?m)^Reason:\s*(.*)""".r

  private val DivisionByZeroRegex: Regex =
    """(?i)(?:\([^)]*\)\s*)?[Dd]ivision by zero""".r

  private val ArithmeticOverflowRegex: Regex =
    """(?i)(?:Arithmetic overflow|Overflow in .+?)(?::?\s*(.+))?""".r

  // Velox: "Invalid value for MAKE_DATE: year=X, month=Y, day=Z"
  private val MakeDateRegex: Regex =
    """Invalid value for MAKE_DATE: year=(-?\d+), month=(-?\d+), day=(-?\d+)""".r

  // Velox: "map key cannot be null"
  private val NullMapKeyRegex: Regex =
    """(?i)map key cannot be null""".r

  // Velox: "Timestamp seconds out of range" or "Could not convert Timestamp... to microseconds"
  private val TimestampOverflowRegex: Regex =
    """(?i)(?:Timestamp seconds out of range|Could not convert Timestamp.+to microseconds)""".r

  // Velox: "Integer overflow in make_ym_interval(X, Y)"
  private val MakeYMIntervalOverflowRegex: Regex =
    """Integer overflow in make_ym_interval""".r

  private val VeloxTypeMap: Map[String, DataType] = Map(
    "BOOLEAN" -> BooleanType,
    "TINYINT" -> ByteType,
    "SMALLINT" -> ShortType,
    "INTEGER" -> IntegerType,
    "BIGINT" -> LongType,
    "REAL" -> FloatType,
    "DOUBLE" -> DoubleType,
    "TIMESTAMP" -> TimestampType,
    "DATE" -> DateType,
    "VARCHAR" -> StringType
  )

  private val NumericTypes: Set[DataType] =
    Set(ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType)

  /**
   * Translates a Velox cast error message into the corresponding Spark exception. Returns null if
   * the message is not a recognized Velox cast error.
   *
   * Called from ColumnarBatchOutIterator.translateException() (Java).
   */
  def translate(msg: String): RuntimeException = {
    if (msg == null) {
      return null
    }

    val reason = extractReason(msg)

    translateCast(reason)
      .orElse(translateArithmetic(reason, msg))
      .orElse(translateOther(reason))
      .orNull
  }

  private def extractReason(msg: String): String = {
    ReasonLineRegex.findFirstMatchIn(msg) match {
      case Some(m) =>
        val reason = m.group(1).trim
        // Velox sometimes produces "Reason: Reason: ..." -- strip the inner prefix
        if (reason.startsWith("Reason:")) reason.substring("Reason:".length).trim
        else reason
      case None => msg
    }
  }

  private def translateCast(reason: String): Option[RuntimeException] = {
    if (!reason.startsWith("Cannot cast ")) {
      return None
    }

    reason match {
      case CastErrorRegex(fromTypeStr, value, toTypeStr, _) =>
        val fromType = resolveType(fromTypeStr)
        val toType = resolveType(toTypeStr)
        if (toType == null) return None

        (fromType, toType) match {
          case (StringType, BooleanType) =>
            Some(
              QueryExecutionErrors
                .invalidInputSyntaxForBooleanError(UTF8String.fromString(value), null))

          case (StringType, _: IntegralType | FloatType | DoubleType | _: DecimalType) =>
            Some(
              QueryExecutionErrors
                .invalidInputInCastToNumberError(toType, UTF8String.fromString(value), null))

          case (StringType, TimestampType | DateType) =>
            Some(
              QueryExecutionErrors
                .invalidInputInCastToDatetimeError(UTF8String.fromString(value), toType, null))

          case (_: DecimalType, dt: DecimalType) =>
            Some(
              QueryExecutionErrors
                .cannotChangeDecimalPrecisionError(Decimal(value), dt.precision, dt.scale, null))

          case (ft, tt) if ft != null && ft != StringType && NumericTypes.contains(tt) =>
            Some(QueryExecutionErrors.castingCauseOverflowError(value, ft, tt))

          case _ => None
        }

      case _ => None
    }
  }

  private val ContextLineRegex: Regex =
    """(?m)^Context:.*?(checked_remainder|checked_modulus)""".r

  private def translateArithmetic(reason: String, fullMsg: String): Option[RuntimeException] = {
    if (DivisionByZeroRegex.findFirstIn(reason).isDefined) {
      if (ContextLineRegex.findFirstIn(fullMsg).isDefined) {
        Some(QueryExecutionErrors.remainderByZeroError(null))
      } else {
        Some(QueryExecutionErrors.divideByZeroError(null))
      }
    } else {
      reason match {
        case ArithmeticOverflowRegex(_) =>
          Some(QueryExecutionErrors.overflowInIntegralDivideError(null))
        case _ => None
      }
    }
  }

  private def translateOther(reason: String): Option[RuntimeException] = {
    // MapFromEntries: "map key cannot be null" -> SparkRuntimeException(NULL_MAP_KEY)
    if (NullMapKeyRegex.findFirstIn(reason).isDefined) {
      return Some(QueryExecutionErrors.nullAsMapKeyNotAllowedError())
    }

    // make_date: "Invalid value for MAKE_DATE: year=X, month=Y, day=Z" -> SparkDateTimeException
    reason match {
      case MakeDateRegex(year, month, day) =>
        val dtException = new java.time.DateTimeException(
          s"Invalid value for MAKE_DATE: year=$year, month=$month, day=$day")
        return Some(QueryExecutionErrors.ansiDateTimeArgumentOutOfRange(dtException))
      case _ =>
    }

    // SecondsToTimestamp/MillisToTimestamp overflow -> ArithmeticException("long overflow")
    if (TimestampOverflowRegex.findFirstIn(reason).isDefined) {
      return Some(new ArithmeticException("long overflow"))
    }

    // make_ym_interval overflow -> SparkArithmeticException(INTERVAL_ARITHMETIC_OVERFLOW)
    if (MakeYMIntervalOverflowRegex.findFirstIn(reason).isDefined) {
      return Some(QueryExecutionErrors.withoutSuggestionIntervalArithmeticOverflowError(null))
    }

    None
  }

  private def resolveType(veloxTypeName: String): DataType = {
    veloxTypeName match {
      case DecimalTypeRegex(p, s) => DecimalType(p.toInt, s.toInt)
      case other => VeloxTypeMap.getOrElse(other, null)
    }
  }
}
