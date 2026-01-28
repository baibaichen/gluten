# Gluten Suite Mapping Report - Spark 40

Generated at: /home/chang/SourceCode/gluten1

## Summary Statistics

- **Total Gluten Suites**: 228
- **Mapped to Spark 40**: 217
- **Not Mapped**: 11
- **Unique Packages Covered**: 19
- **Total Missing Suite Files**: 166
- **Total Missing Suite Classes**: 183

---

## Missing Suites by Package

*Review this section to decide which packages to process*

### Package: `org/apache/spark/sql`

**Missing Suite Files**: 37

- **RowJsonSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/RowJsonSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenRowJsonSuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (1):
    - `RowJsonSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **RandomDataGeneratorSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/RandomDataGeneratorSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenRandomDataGeneratorSuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (1):
    - `RandomDataGeneratorSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **SparkSessionBuilderSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/SparkSessionBuilderSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenSparkSessionBuilderSuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (1):
    - `SparkSessionBuilderSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **PercentileQuerySuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/PercentileQuerySuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenPercentileQuerySuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (1):
    - `PercentileQuerySuite` extends `QueryTest` → `GlutenSQLTestsTrait`
- **UnsafeRowSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/UnsafeRowSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenUnsafeRowSuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (1):
    - `UnsafeRowSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **InlineTableParsingImprovementsSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/InlineTableParsingImprovementsSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenInlineTableParsingImprovementsSuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (1):
    - `InlineTableParsingImprovementsSuite` extends `QueryTest` → `GlutenSQLTestsTrait`
- **ICUCollationsMapSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/ICUCollationsMapSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenICUCollationsMapSuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (1):
    - `ICUCollationsMapSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **DefaultANSIValueSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/DefaultANSIValueSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenDefaultANSIValueSuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (1):
    - `DefaultANSIValueSuite` extends `QueryTest` → `GlutenSQLTestsTrait`
- **VariantSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/VariantSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenVariantSuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (1):
    - `VariantSuite` extends `QueryTest` → `GlutenSQLTestsTrait`
- **CacheManagerSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/CacheManagerSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenCacheManagerSuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (1):
    - `CacheManagerSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **VariantShreddingSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/VariantShreddingSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenVariantShreddingSuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (1):
    - `VariantShreddingSuite` extends `QueryTest` → `GlutenSQLTestsTrait`
- **AggregateHashMapSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/AggregateHashMapSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenAggregateHashMapSuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (3):
    - `SingleLevelAggregateHashMapSuite` extends `DataFrameAggregateSuite` → `GlutenTestsCommonTrait`
    - `TwoLevelAggregateHashMapSuite` extends `DataFrameAggregateSuite` → `GlutenTestsCommonTrait`
    - `TwoLevelAggregateHashMapWithVectorizedMapSuite` extends `DataFrameAggregateSuite` → `GlutenTestsCommonTrait`
- **XmlFunctionsSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/XmlFunctionsSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenXmlFunctionsSuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (1):
    - `XmlFunctionsSuite` extends `QueryTest` → `GlutenSQLTestsTrait`
- **DataFrameShowSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/DataFrameShowSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenDataFrameShowSuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (1):
    - `DataFrameShowSuite` extends `QueryTest` → `GlutenSQLTestsTrait`
- **UDFSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/UDFSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenUDFSuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (1):
    - `UDFSuite` extends `QueryTest` → `GlutenSQLTestsTrait`
- **JoinHintSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/JoinHintSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenJoinHintSuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (1):
    - `JoinHintSuite` extends `PlanTest` → `GlutenTestsCommonTrait`
- **SSBQuerySuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/SSBQuerySuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenSSBQuerySuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (1):
    - `SSBQuerySuite` extends `BenchmarkQueryTest` → `GlutenSQLTestsTrait`
- **VariantWriteShreddingSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/VariantWriteShreddingSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenVariantWriteShreddingSuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (1):
    - `VariantWriteShreddingSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **UDTRegistrationSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/UDTRegistrationSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenUDTRegistrationSuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (1):
    - `UDTRegistrationSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **LogQuerySuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/LogQuerySuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenLogQuerySuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (1):
    - `LogQuerySuite` extends `QueryTest` → `GlutenSQLTestsTrait`
- **TPCDSQueryTestSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/TPCDSQueryTestSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenTPCDSQueryTestSuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (1):
    - `TPCDSQueryTestSuite` extends `QueryTest` → `GlutenSQLTestsTrait`
- **RuntimeConfigSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/RuntimeConfigSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenRuntimeConfigSuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (1):
    - `RuntimeConfigSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **TPCHQuerySuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/TPCHQuerySuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenTPCHQuerySuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (1):
    - `TPCHQuerySuite` extends `BenchmarkQueryTest` → `GlutenSQLTestsTrait`
- **SetCommandSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/SetCommandSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenSetCommandSuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (1):
    - `SetCommandSuite` extends `QueryTest` → `GlutenSQLTestsTrait`
- **UserDefinedTypeSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/UserDefinedTypeSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenUserDefinedTypeSuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (1):
    - `UserDefinedTypeSuite` extends `QueryTest` → `GlutenSQLTestsTrait`
- **SessionStateSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/SessionStateSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenSessionStateSuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (1):
    - `SessionStateSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **PlanStabilitySuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/PlanStabilitySuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenPlanStabilitySuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (7):
    - `TPCDSV1_4_PlanStabilitySuite` extends `PlanStabilitySuite` → `GlutenTestsCommonTrait`
    - `TPCDSV1_4_PlanStabilityWithStatsSuite` extends `PlanStabilitySuite` → `GlutenTestsCommonTrait`
    - `TPCDSV2_7_PlanStabilitySuite` extends `PlanStabilitySuite` → `GlutenTestsCommonTrait`
    - `TPCDSV2_7_PlanStabilityWithStatsSuite` extends `PlanStabilitySuite` → `GlutenTestsCommonTrait`
    - `TPCDSModifiedPlanStabilitySuite` extends `PlanStabilitySuite` → `GlutenTestsCommonTrait`
    - `TPCDSModifiedPlanStabilityWithStatsSuite` extends `PlanStabilitySuite` → `GlutenTestsCommonTrait`
    - `TPCHPlanStabilitySuite` extends `PlanStabilitySuite` → `GlutenTestsCommonTrait`
- **DataFrameTableValuedFunctionsSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/DataFrameTableValuedFunctionsSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenDataFrameTableValuedFunctionsSuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (1):
    - `DataFrameTableValuedFunctionsSuite` extends `QueryTest` → `GlutenSQLTestsTrait`
- **DeprecatedDatasetAggregatorSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/DeprecatedDatasetAggregatorSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenDeprecatedDatasetAggregatorSuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (1):
    - `DeprecatedDatasetAggregatorSuite` extends `QueryTest` → `GlutenSQLTestsTrait`
- **DataFrameSubquerySuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/DataFrameSubquerySuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenDataFrameSubquerySuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (1):
    - `DataFrameSubquerySuite` extends `QueryTest` → `GlutenSQLTestsTrait`
- **SparkSessionJobTaggingAndCancellationSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/SparkSessionJobTaggingAndCancellationSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenSparkSessionJobTaggingAndCancellationSuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (1):
    - `SparkSessionJobTaggingAndCancellationSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **VariantEndToEndSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/VariantEndToEndSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenVariantEndToEndSuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (1):
    - `VariantEndToEndSuite` extends `QueryTest` → `GlutenSQLTestsTrait`
- **RowSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/RowSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenRowSuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (1):
    - `RowSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **TPCDSCollationQueryTestSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/TPCDSCollationQueryTestSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenTPCDSCollationQueryTestSuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (1):
    - `TPCDSCollationQueryTestSuite` extends `QueryTest` → `GlutenSQLTestsTrait`
- **DataFrameTransposeSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/DataFrameTransposeSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenDataFrameTransposeSuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (1):
    - `DataFrameTransposeSuite` extends `QueryTest` → `GlutenSQLTestsTrait`
- **ExplainSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/ExplainSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenExplainSuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (1):
    - `ExplainSuite` extends `ExplainSuiteHelper` → `GlutenTestsCommonTrait`
- **TPCDSQuerySuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/TPCDSQuerySuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/GlutenTPCDSQuerySuite.scala`
  - Package: `org.apache.spark.sql`
  - Classes (3):
    - `TPCDSQuerySuite` extends `BenchmarkQueryTest` → `GlutenSQLTestsTrait`
    - `TPCDSQueryWithStatsSuite` extends `TPCDSQuerySuite` → `GlutenTestsCommonTrait`
    - `TPCDSQueryANSISuite` extends `TPCDSQuerySuite` → `GlutenTestsCommonTrait`

### Package: `org/apache/spark/sql/catalyst/expressions`

**Missing Suite Files**: 35

- **XmlExpressionsSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/XmlExpressionsSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/GlutenXmlExpressionsSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions`
  - Classes (1):
    - `XmlExpressionsSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **HexSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/HexSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/GlutenHexSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions`
  - Classes (1):
    - `HexSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **CsvExpressionsSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/CsvExpressionsSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/GlutenCsvExpressionsSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions`
  - Classes (1):
    - `CsvExpressionsSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **ExpressionSetSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/ExpressionSetSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/GlutenExpressionSetSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions`
  - Classes (1):
    - `ExpressionSetSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **TimeWindowSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/TimeWindowSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/GlutenTimeWindowSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions`
  - Classes (1):
    - `TimeWindowSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **UnsafeRowConverterSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/UnsafeRowConverterSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/GlutenUnsafeRowConverterSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions`
  - Classes (1):
    - `UnsafeRowConverterSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **ObjectExpressionsSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/ObjectExpressionsSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/GlutenObjectExpressionsSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions`
  - Classes (1):
    - `ObjectExpressionsSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **V2ExpressionUtilsSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/V2ExpressionUtilsSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/GlutenV2ExpressionUtilsSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions`
  - Classes (1):
    - `V2ExpressionUtilsSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **ExprIdSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/ExprIdSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/GlutenExprIdSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions`
  - Classes (1):
    - `ExprIdSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **CodeGenerationSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/CodeGenerationSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/GlutenCodeGenerationSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions`
  - Classes (1):
    - `CodeGenerationSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **NamedExpressionSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/NamedExpressionSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/GlutenNamedExpressionSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions`
  - Classes (1):
    - `NamedExpressionSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **ToPrettyStringSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/ToPrettyStringSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/GlutenToPrettyStringSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions`
  - Classes (1):
    - `ToPrettyStringSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **SelectedFieldSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/SelectedFieldSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/GlutenSelectedFieldSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions`
  - Classes (1):
    - `SelectedFieldSuite` extends `AnalysisTest` → `GlutenTestsCommonTrait`
- **CallMethodViaReflectionSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/CallMethodViaReflectionSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/GlutenCallMethodViaReflectionSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions`
  - Classes (1):
    - `CallMethodViaReflectionSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **SchemaPruningSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/SchemaPruningSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/GlutenSchemaPruningSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions`
  - Classes (1):
    - `SchemaPruningSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **CollationRegexpExpressionsSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/CollationRegexpExpressionsSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/GlutenCollationRegexpExpressionsSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions`
  - Classes (1):
    - `CollationRegexpExpressionsSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **AttributeSetSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/AttributeSetSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/GlutenAttributeSetSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions`
  - Classes (1):
    - `AttributeSetSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **ExpressionEvalHelperSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/ExpressionEvalHelperSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/GlutenExpressionEvalHelperSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions`
  - Classes (1):
    - `ExpressionEvalHelperSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **MutableProjectionSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/MutableProjectionSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/GlutenMutableProjectionSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions`
  - Classes (1):
    - `MutableProjectionSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **SubexpressionEliminationSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/SubexpressionEliminationSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/GlutenSubexpressionEliminationSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions`
  - Classes (1):
    - `SubexpressionEliminationSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **CollationExpressionSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/CollationExpressionSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/GlutenCollationExpressionSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions`
  - Classes (1):
    - `CollationExpressionSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **CastWithAnsiOnSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/CastWithAnsiOnSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/GlutenCastWithAnsiOnSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions`
  - Classes (1):
    - `CastWithAnsiOnSuite` extends `CastSuiteBase` → `GlutenTestsCommonTrait`
- **DynamicPruningSubquerySuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/DynamicPruningSubquerySuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/GlutenDynamicPruningSubquerySuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions`
  - Classes (1):
    - `DynamicPruningSubquerySuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **ExpressionSQLBuilderSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/ExpressionSQLBuilderSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/GlutenExpressionSQLBuilderSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions`
  - Classes (1):
    - `ExpressionSQLBuilderSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **ScalaUDFSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/ScalaUDFSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/GlutenScalaUDFSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions`
  - Classes (1):
    - `ScalaUDFSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **BitmapExpressionUtilsSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/BitmapExpressionUtilsSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/GlutenBitmapExpressionUtilsSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions`
  - Classes (1):
    - `BitmapExpressionUtilsSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **ExtractPredicatesWithinOutputSetSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/ExtractPredicatesWithinOutputSetSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/GlutenExtractPredicatesWithinOutputSetSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions`
  - Classes (1):
    - `ExtractPredicatesWithinOutputSetSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **CodeGeneratorWithInterpretedFallbackSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/CodeGeneratorWithInterpretedFallbackSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/GlutenCodeGeneratorWithInterpretedFallbackSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions`
  - Classes (1):
    - `CodeGeneratorWithInterpretedFallbackSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **AttributeResolutionSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/AttributeResolutionSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/GlutenAttributeResolutionSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions`
  - Classes (1):
    - `AttributeResolutionSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **ExpressionImplUtilsSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/ExpressionImplUtilsSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/GlutenExpressionImplUtilsSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions`
  - Classes (1):
    - `ExpressionImplUtilsSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **CanonicalizeSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/CanonicalizeSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/GlutenCanonicalizeSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions`
  - Classes (1):
    - `CanonicalizeSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **SubExprEvaluationRuntimeSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/SubExprEvaluationRuntimeSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/GlutenSubExprEvaluationRuntimeSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions`
  - Classes (1):
    - `SubExprEvaluationRuntimeSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **UnwrapUDTExpressionSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/UnwrapUDTExpressionSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/GlutenUnwrapUDTExpressionSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions`
  - Classes (1):
    - `UnwrapUDTExpressionSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **OrderingSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/OrderingSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/GlutenOrderingSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions`
  - Classes (1):
    - `OrderingSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **ValidateExternalTypeSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/catalyst/expressions/ValidateExternalTypeSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/GlutenValidateExternalTypeSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions`
  - Classes (1):
    - `ValidateExternalTypeSuite` extends `QueryTest` → `GlutenSQLTestsTrait`

### Package: `org/apache/spark/sql/catalyst/expressions/aggregate`

**Missing Suite Files**: 11

- **FirstLastTestSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/aggregate/FirstLastTestSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/aggregate/GlutenFirstLastTestSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions.aggregate`
  - Classes (1):
    - `FirstLastTestSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **ApproxCountDistinctForIntervalsSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/aggregate/ApproxCountDistinctForIntervalsSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/aggregate/GlutenApproxCountDistinctForIntervalsSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions.aggregate`
  - Classes (1):
    - `ApproxCountDistinctForIntervalsSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **HistogramNumericSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/aggregate/HistogramNumericSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/aggregate/GlutenHistogramNumericSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions.aggregate`
  - Classes (1):
    - `HistogramNumericSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **CentralMomentAggSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/aggregate/CentralMomentAggSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/aggregate/GlutenCentralMomentAggSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions.aggregate`
  - Classes (1):
    - `CentralMomentAggSuite` extends `TestWithAndWithoutCodegen` → `GlutenTestsCommonTrait`
- **DatasketchesHllSketchSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/aggregate/DatasketchesHllSketchSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/aggregate/GlutenDatasketchesHllSketchSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions.aggregate`
  - Classes (1):
    - `DatasketchesHllSketchSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **ApproximatePercentileSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/aggregate/ApproximatePercentileSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/aggregate/GlutenApproximatePercentileSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions.aggregate`
  - Classes (1):
    - `ApproximatePercentileSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **CovarianceAggSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/aggregate/CovarianceAggSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/aggregate/GlutenCovarianceAggSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions.aggregate`
  - Classes (1):
    - `CovarianceAggSuite` extends `TestWithAndWithoutCodegen` → `GlutenTestsCommonTrait`
- **CountMinSketchAggSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/aggregate/CountMinSketchAggSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/aggregate/GlutenCountMinSketchAggSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions.aggregate`
  - Classes (1):
    - `CountMinSketchAggSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **HyperLogLogPlusPlusSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/aggregate/HyperLogLogPlusPlusSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/aggregate/GlutenHyperLogLogPlusPlusSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions.aggregate`
  - Classes (1):
    - `HyperLogLogPlusPlusSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **ProductAggSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/aggregate/ProductAggSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/aggregate/GlutenProductAggSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions.aggregate`
  - Classes (1):
    - `ProductAggSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **AggregateExpressionSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/aggregate/AggregateExpressionSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/aggregate/GlutenAggregateExpressionSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions.aggregate`
  - Classes (1):
    - `AggregateExpressionSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`

### Package: `org/apache/spark/sql/connector`

**Missing Suite Files**: 11

- **V2CommandsCaseSensitivitySuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/connector/V2CommandsCaseSensitivitySuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/connector/GlutenV2CommandsCaseSensitivitySuite.scala`
  - Package: `org.apache.spark.sql.connector`
  - Classes (1):
    - `V2CommandsCaseSensitivitySuite` extends `SharedSparkSession` → `GlutenTestsCommonTrait`
- **DataSourceV2OptionSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/connector/DataSourceV2OptionSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/connector/GlutenDataSourceV2OptionSuite.scala`
  - Package: `org.apache.spark.sql.connector`
  - Classes (1):
    - `DataSourceV2OptionSuite` extends `DatasourceV2SQLBase` → `GlutenTestsCommonTrait`
- **V1WriteFallbackSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/connector/V1WriteFallbackSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/connector/GlutenV1WriteFallbackSuite.scala`
  - Package: `org.apache.spark.sql.connector`
  - Classes (2):
    - `V1WriteFallbackSuite` extends `QueryTest` → `GlutenSQLTestsTrait`
    - `V1WriteFallbackSessionCatalogSuite` extends `InsertIntoTests` → `GlutenTestsCommonTrait`
- **StaticProcedureSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/connector/StaticProcedureSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/connector/GlutenStaticProcedureSuite.scala`
  - Package: `org.apache.spark.sql.connector`
  - Classes (1):
    - `StaticProcedureSuite` extends `SharedSparkSession` → `GlutenTestsCommonTrait`
- **DataSourceV2MetricsSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/connector/DataSourceV2MetricsSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/connector/GlutenDataSourceV2MetricsSuite.scala`
  - Package: `org.apache.spark.sql.connector`
  - Classes (1):
    - `DataSourceV2MetricsSuite` extends `DatasourceV2SQLBase` → `GlutenTestsCommonTrait`
- **V1ReadFallbackSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/connector/V1ReadFallbackSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/connector/GlutenV1ReadFallbackSuite.scala`
  - Package: `org.apache.spark.sql.connector`
  - Classes (2):
    - `V1ReadFallbackWithDataFrameReaderSuite` extends `V1ReadFallbackSuite` → `GlutenTestsCommonTrait`
    - `V1ReadFallbackWithCatalogSuite` extends `V1ReadFallbackSuite` → `GlutenTestsCommonTrait`
- **GroupBasedUpdateTableSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/connector/GroupBasedUpdateTableSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/connector/GlutenGroupBasedUpdateTableSuite.scala`
  - Package: `org.apache.spark.sql.connector`
  - Classes (1):
    - `GroupBasedUpdateTableSuite` extends `UpdateTableSuiteBase` → `GlutenTestsCommonTrait`
- **PushablePredicateSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/connector/PushablePredicateSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/connector/GlutenPushablePredicateSuite.scala`
  - Package: `org.apache.spark.sql.connector`
  - Classes (1):
    - `PushablePredicateSuite` extends `QueryTest` → `GlutenSQLTestsTrait`
- **MergeIntoDataFrameSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/connector/MergeIntoDataFrameSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/connector/GlutenMergeIntoDataFrameSuite.scala`
  - Package: `org.apache.spark.sql.connector`
  - Classes (1):
    - `MergeIntoDataFrameSuite` extends `RowLevelOperationSuiteBase` → `GlutenTestsCommonTrait`
- **ProcedureSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/connector/ProcedureSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/connector/GlutenProcedureSuite.scala`
  - Package: `org.apache.spark.sql.connector`
  - Classes (1):
    - `ProcedureSuite` extends `QueryTest` → `GlutenSQLTestsTrait`
- **DataSourceV2UtilsSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/connector/DataSourceV2UtilsSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/connector/GlutenDataSourceV2UtilsSuite.scala`
  - Package: `org.apache.spark.sql.connector`
  - Classes (1):
    - `DataSourceV2UtilsSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`

### Package: `org/apache/spark/sql/errors`

**Missing Suite Files**: 2

- **QueryExecutionAnsiErrorsSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/errors/QueryExecutionAnsiErrorsSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/errors/GlutenQueryExecutionAnsiErrorsSuite.scala`
  - Package: `org.apache.spark.sql.errors`
  - Classes (1):
    - `QueryExecutionAnsiErrorsSuite` extends `QueryTest` → `GlutenSQLTestsTrait`
- **QueryContextSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/errors/QueryContextSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/errors/GlutenQueryContextSuite.scala`
  - Package: `org.apache.spark.sql.errors`
  - Classes (1):
    - `QueryContextSuite` extends `QueryTest` → `GlutenSQLTestsTrait`

### Package: `org/apache/spark/sql/execution`

**Missing Suite Files**: 34

- **UnsafeRowSerializerSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/UnsafeRowSerializerSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenUnsafeRowSerializerSuite.scala`
  - Package: `org.apache.spark.sql.execution`
  - Classes (1):
    - `UnsafeRowSerializerSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **ShufflePartitionsUtilSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/ShufflePartitionsUtilSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenShufflePartitionsUtilSuite.scala`
  - Package: `org.apache.spark.sql.execution`
  - Classes (1):
    - `ShufflePartitionsUtilSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **SQLViewSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/SQLViewSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenSQLViewSuite.scala`
  - Package: `org.apache.spark.sql.execution`
  - Classes (1):
    - `SimpleSQLViewSuite` extends `SQLViewSuite` → `GlutenTestsCommonTrait`
- **RemoveRedundantSortsSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/RemoveRedundantSortsSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenRemoveRedundantSortsSuite.scala`
  - Package: `org.apache.spark.sql.execution`
  - Classes (1):
    - `RemoveRedundantSortsSuite` extends `RemoveRedundantSortsSuiteBase` → `GlutenTestsCommonTrait`
- **UnsafeKVExternalSorterSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/UnsafeKVExternalSorterSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenUnsafeKVExternalSorterSuite.scala`
  - Package: `org.apache.spark.sql.execution`
  - Classes (1):
    - `UnsafeKVExternalSorterSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **ColumnarRulesSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/ColumnarRulesSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenColumnarRulesSuite.scala`
  - Package: `org.apache.spark.sql.execution`
  - Classes (1):
    - `ColumnarRulesSuite` extends `PlanTest` → `GlutenTestsCommonTrait`
- **RemoveRedundantProjectsSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/RemoveRedundantProjectsSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenRemoveRedundantProjectsSuite.scala`
  - Package: `org.apache.spark.sql.execution`
  - Classes (1):
    - `RemoveRedundantProjectsSuite` extends `RemoveRedundantProjectsSuiteBase` → `GlutenTestsCommonTrait`
- **RowToColumnConverterSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/RowToColumnConverterSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenRowToColumnConverterSuite.scala`
  - Package: `org.apache.spark.sql.execution`
  - Classes (1):
    - `RowToColumnConverterSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **SparkScriptTransformationSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/SparkScriptTransformationSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenSparkScriptTransformationSuite.scala`
  - Package: `org.apache.spark.sql.execution`
  - Classes (1):
    - `SparkScriptTransformationSuite` extends `BaseScriptTransformationSuite` → `GlutenTestsCommonTrait`
- **LogicalPlanTagInSparkPlanSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/LogicalPlanTagInSparkPlanSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenLogicalPlanTagInSparkPlanSuite.scala`
  - Package: `org.apache.spark.sql.execution`
  - Classes (1):
    - `LogicalPlanTagInSparkPlanSuite` extends `TPCDSQuerySuite` → `GlutenTestsCommonTrait`
- **CoGroupedIteratorSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/CoGroupedIteratorSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenCoGroupedIteratorSuite.scala`
  - Package: `org.apache.spark.sql.execution`
  - Classes (1):
    - `CoGroupedIteratorSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **SQLViewTestSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/SQLViewTestSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenSQLViewTestSuite.scala`
  - Package: `org.apache.spark.sql.execution`
  - Classes (3):
    - `LocalTempViewTestSuite` extends `TempViewTestSuite` → `GlutenTestsCommonTrait`
    - `GlobalTempViewTestSuite` extends `TempViewTestSuite` → `GlutenTestsCommonTrait`
    - `PersistedViewTestSuite` extends `SQLViewTestSuite` → `GlutenTestsCommonTrait`
- **HiveResultSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/HiveResultSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenHiveResultSuite.scala`
  - Package: `org.apache.spark.sql.execution`
  - Classes (1):
    - `HiveResultSuite` extends `SharedSparkSession` → `GlutenTestsCommonTrait`
- **WholeStageCodegenSparkSubmitSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/WholeStageCodegenSparkSubmitSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenWholeStageCodegenSparkSubmitSuite.scala`
  - Package: `org.apache.spark.sql.execution`
  - Classes (1):
    - `WholeStageCodegenSparkSubmitSuite` extends `SparkSubmitTestUtils` → `GlutenTestsCommonTrait`
- **SQLExecutionSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/SQLExecutionSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenSQLExecutionSuite.scala`
  - Package: `org.apache.spark.sql.execution`
  - Classes (1):
    - `SQLExecutionSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **SQLFunctionSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/SQLFunctionSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenSQLFunctionSuite.scala`
  - Package: `org.apache.spark.sql.execution`
  - Classes (1):
    - `SQLFunctionSuite` extends `QueryTest` → `GlutenSQLTestsTrait`
- **WholeStageCodegenSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/WholeStageCodegenSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenWholeStageCodegenSuite.scala`
  - Package: `org.apache.spark.sql.execution`
  - Classes (1):
    - `WholeStageCodegenSuite` extends `QueryTest` → `GlutenSQLTestsTrait`
- **OptimizeMetadataOnlyQuerySuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/OptimizeMetadataOnlyQuerySuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenOptimizeMetadataOnlyQuerySuite.scala`
  - Package: `org.apache.spark.sql.execution`
  - Classes (1):
    - `OptimizeMetadataOnlyQuerySuite` extends `QueryTest` → `GlutenSQLTestsTrait`
- **GlobalTempViewSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/GlobalTempViewSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenGlobalTempViewSuite.scala`
  - Package: `org.apache.spark.sql.execution`
  - Classes (1):
    - `GlobalTempViewSuite` extends `QueryTest` → `GlutenSQLTestsTrait`
- **QueryPlanningTrackerEndToEndSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/QueryPlanningTrackerEndToEndSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenQueryPlanningTrackerEndToEndSuite.scala`
  - Package: `org.apache.spark.sql.execution`
  - Classes (1):
    - `QueryPlanningTrackerEndToEndSuite` extends `StreamTest` → `GlutenTestsCommonTrait`
- **SparkPlanSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/SparkPlanSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenSparkPlanSuite.scala`
  - Package: `org.apache.spark.sql.execution`
  - Classes (1):
    - `SparkPlanSuite` extends `QueryTest` → `GlutenSQLTestsTrait`
- **SparkPlannerSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/SparkPlannerSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenSparkPlannerSuite.scala`
  - Package: `org.apache.spark.sql.execution`
  - Classes (1):
    - `SparkPlannerSuite` extends `SharedSparkSession` → `GlutenTestsCommonTrait`
- **SparkSqlParserSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/SparkSqlParserSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenSparkSqlParserSuite.scala`
  - Package: `org.apache.spark.sql.execution`
  - Classes (1):
    - `SparkSqlParserSuite` extends `AnalysisTest` → `GlutenTestsCommonTrait`
- **UnsafeFixedWidthAggregationMapSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/UnsafeFixedWidthAggregationMapSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenUnsafeFixedWidthAggregationMapSuite.scala`
  - Package: `org.apache.spark.sql.execution`
  - Classes (1):
    - `UnsafeFixedWidthAggregationMapSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **GroupedIteratorSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/GroupedIteratorSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenGroupedIteratorSuite.scala`
  - Package: `org.apache.spark.sql.execution`
  - Classes (1):
    - `GroupedIteratorSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **ExternalAppendOnlyUnsafeRowArraySuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/ExternalAppendOnlyUnsafeRowArraySuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenExternalAppendOnlyUnsafeRowArraySuite.scala`
  - Package: `org.apache.spark.sql.execution`
  - Classes (1):
    - `ExternalAppendOnlyUnsafeRowArraySuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **AggregatingAccumulatorSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/AggregatingAccumulatorSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenAggregatingAccumulatorSuite.scala`
  - Package: `org.apache.spark.sql.execution`
  - Classes (1):
    - `AggregatingAccumulatorSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **ProjectedOrderingAndPartitioningSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/ProjectedOrderingAndPartitioningSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenProjectedOrderingAndPartitioningSuite.scala`
  - Package: `org.apache.spark.sql.execution`
  - Classes (1):
    - `ProjectedOrderingAndPartitioningSuite` extends `SharedSparkSession` → `GlutenTestsCommonTrait`
- **PlannerSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/PlannerSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenPlannerSuite.scala`
  - Package: `org.apache.spark.sql.execution`
  - Classes (1):
    - `PlannerSuite` extends `SharedSparkSession` → `GlutenTestsCommonTrait`
- **DeprecatedWholeStageCodegenSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/DeprecatedWholeStageCodegenSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenDeprecatedWholeStageCodegenSuite.scala`
  - Package: `org.apache.spark.sql.execution`
  - Classes (1):
    - `DeprecatedWholeStageCodegenSuite` extends `QueryTest` → `GlutenSQLTestsTrait`
- **SQLJsonProtocolSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/SQLJsonProtocolSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenSQLJsonProtocolSuite.scala`
  - Package: `org.apache.spark.sql.execution`
  - Classes (1):
    - `SQLJsonProtocolSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **InsertSortForLimitAndOffsetSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/InsertSortForLimitAndOffsetSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenInsertSortForLimitAndOffsetSuite.scala`
  - Package: `org.apache.spark.sql.execution`
  - Classes (1):
    - `InsertSortForLimitAndOffsetSuite` extends `QueryTest` → `GlutenSQLTestsTrait`
- **DataSourceScanExecRedactionSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/DataSourceScanExecRedactionSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenDataSourceScanExecRedactionSuite.scala`
  - Package: `org.apache.spark.sql.execution`
  - Classes (2):
    - `DataSourceScanExecRedactionSuite` extends `DataSourceScanRedactionTest` → `GlutenTestsCommonTrait`
    - `DataSourceV2ScanExecRedactionSuite` extends `DataSourceScanRedactionTest` → `GlutenTestsCommonTrait`
- **ExecuteImmediateEndToEndSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/ExecuteImmediateEndToEndSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenExecuteImmediateEndToEndSuite.scala`
  - Package: `org.apache.spark.sql.execution`
  - Classes (1):
    - `ExecuteImmediateEndToEndSuite` extends `QueryTest` → `GlutenSQLTestsTrait`

### Package: `org/apache/spark/sql/execution/datasources`

**Missing Suite Files**: 10

- **DataSourceManagerSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/datasources/DataSourceManagerSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/datasources/GlutenDataSourceManagerSuite.scala`
  - Package: `org.apache.spark.sql.execution.datasources`
  - Classes (1):
    - `DataSourceManagerSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **FileResolverSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/datasources/FileResolverSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/datasources/GlutenFileResolverSuite.scala`
  - Package: `org.apache.spark.sql.execution.datasources`
  - Classes (1):
    - `FileResolverSuite` extends `QueryTest` → `GlutenSQLTestsTrait`
- **DataSourceResolverSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/datasources/DataSourceResolverSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/datasources/GlutenDataSourceResolverSuite.scala`
  - Package: `org.apache.spark.sql.execution.datasources`
  - Classes (1):
    - `DataSourceResolverSuite` extends `QueryTest` → `GlutenSQLTestsTrait`
- **BasicWriteJobStatsTrackerMetricSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/datasources/BasicWriteJobStatsTrackerMetricSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/datasources/GlutenBasicWriteJobStatsTrackerMetricSuite.scala`
  - Package: `org.apache.spark.sql.execution.datasources`
  - Classes (1):
    - `BasicWriteJobStatsTrackerMetricSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **InMemoryTableMetricSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/datasources/InMemoryTableMetricSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/datasources/GlutenInMemoryTableMetricSuite.scala`
  - Package: `org.apache.spark.sql.execution.datasources`
  - Classes (1):
    - `InMemoryTableMetricSuite` extends `QueryTest` → `GlutenSQLTestsTrait`
- **RowDataSourceStrategySuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/datasources/RowDataSourceStrategySuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/datasources/GlutenRowDataSourceStrategySuite.scala`
  - Package: `org.apache.spark.sql.execution.datasources`
  - Classes (1):
    - `RowDataSourceStrategySuite` extends `SharedSparkSession` → `GlutenTestsCommonTrait`
- **SaveIntoDataSourceCommandSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/datasources/SaveIntoDataSourceCommandSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/datasources/GlutenSaveIntoDataSourceCommandSuite.scala`
  - Package: `org.apache.spark.sql.execution.datasources`
  - Classes (1):
    - `SaveIntoDataSourceCommandSuite` extends `QueryTest` → `GlutenSQLTestsTrait`
- **CustomWriteTaskStatsTrackerSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/datasources/CustomWriteTaskStatsTrackerSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/datasources/GlutenCustomWriteTaskStatsTrackerSuite.scala`
  - Package: `org.apache.spark.sql.execution.datasources`
  - Classes (1):
    - `CustomWriteTaskStatsTrackerSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **PushVariantIntoScanSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/datasources/PushVariantIntoScanSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/datasources/GlutenPushVariantIntoScanSuite.scala`
  - Package: `org.apache.spark.sql.execution.datasources`
  - Classes (1):
    - `PushVariantIntoScanSuite` extends `SharedSparkSession` → `GlutenTestsCommonTrait`
- **BasicWriteTaskStatsTrackerSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/datasources/BasicWriteTaskStatsTrackerSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/datasources/GlutenBasicWriteTaskStatsTrackerSuite.scala`
  - Package: `org.apache.spark.sql.execution.datasources`
  - Classes (1):
    - `BasicWriteTaskStatsTrackerSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`

### Package: `org/apache/spark/sql/execution/datasources/csv`

**Missing Suite Files**: 1

- **CSVParsingOptionsSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/datasources/csv/CSVParsingOptionsSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/datasources/csv/GlutenCSVParsingOptionsSuite.scala`
  - Package: `org.apache.spark.sql.execution.datasources.csv`
  - Classes (1):
    - `CSVParsingOptionsSuite` extends `QueryTest` → `GlutenSQLTestsTrait`

### Package: `org/apache/spark/sql/execution/datasources/json`

**Missing Suite Files**: 1

- **JsonParsingOptionsSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/datasources/json/JsonParsingOptionsSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/datasources/json/GlutenJsonParsingOptionsSuite.scala`
  - Package: `org.apache.spark.sql.execution.datasources.json`
  - Classes (1):
    - `JsonParsingOptionsSuite` extends `QueryTest` → `GlutenSQLTestsTrait`

### Package: `org/apache/spark/sql/execution/datasources/orc`

**Missing Suite Files**: 1

- **OrcEncryptionSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/datasources/orc/OrcEncryptionSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/datasources/orc/GlutenOrcEncryptionSuite.scala`
  - Package: `org.apache.spark.sql.execution.datasources.orc`
  - Classes (1):
    - `OrcEncryptionSuite` extends `OrcTest` → `GlutenTestsCommonTrait`

### Package: `org/apache/spark/sql/execution/datasources/parquet`

**Missing Suite Files**: 5

- **ParquetVariantShreddingSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetVariantShreddingSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/datasources/parquet/GlutenParquetVariantShreddingSuite.scala`
  - Package: `org.apache.spark.sql.execution.datasources.parquet`
  - Classes (1):
    - `ParquetVariantShreddingSuite` extends `QueryTest` → `GlutenSQLTestsTrait`
- **ParquetAvroCompatibilitySuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetAvroCompatibilitySuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/datasources/parquet/GlutenParquetAvroCompatibilitySuite.scala`
  - Package: `org.apache.spark.sql.execution.datasources.parquet`
  - Classes (1):
    - `ParquetAvroCompatibilitySuite` extends `ParquetCompatibilityTest` → `GlutenTestsCommonTrait`
- **ParquetCommitterSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetCommitterSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/datasources/parquet/GlutenParquetCommitterSuite.scala`
  - Package: `org.apache.spark.sql.execution.datasources.parquet`
  - Classes (1):
    - `ParquetCommitterSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **ParquetTypeWideningSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetTypeWideningSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/datasources/parquet/GlutenParquetTypeWideningSuite.scala`
  - Package: `org.apache.spark.sql.execution.datasources.parquet`
  - Classes (1):
    - `ParquetTypeWideningSuite` extends `QueryTest` → `GlutenSQLTestsTrait`
- **ParquetFieldIdSchemaSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetFieldIdSchemaSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/datasources/parquet/GlutenParquetFieldIdSchemaSuite.scala`
  - Package: `org.apache.spark.sql.execution.datasources.parquet`
  - Classes (1):
    - `ParquetFieldIdSchemaSuite` extends `ParquetSchemaTest` → `GlutenTestsCommonTrait`

### Package: `org/apache/spark/sql/execution/datasources/text`

**Missing Suite Files**: 1

- **WholeTextFileSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/datasources/text/WholeTextFileSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/datasources/text/GlutenWholeTextFileSuite.scala`
  - Package: `org.apache.spark.sql.execution.datasources.text`
  - Classes (2):
    - `WholeTextFileV1Suite` extends `WholeTextFileSuite` → `GlutenTestsCommonTrait`
    - `WholeTextFileV2Suite` extends `WholeTextFileSuite` → `GlutenTestsCommonTrait`

### Package: `org/apache/spark/sql/execution/datasources/v2`

**Missing Suite Files**: 2

- **FileWriterFactorySuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/datasources/v2/FileWriterFactorySuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/datasources/v2/GlutenFileWriterFactorySuite.scala`
  - Package: `org.apache.spark.sql.execution.datasources.v2`
  - Classes (1):
    - `FileWriterFactorySuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **V2SessionCatalogSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/datasources/v2/V2SessionCatalogSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/datasources/v2/GlutenV2SessionCatalogSuite.scala`
  - Package: `org.apache.spark.sql.execution.datasources.v2`
  - Classes (2):
    - `V2SessionCatalogTableSuite` extends `V2SessionCatalogBaseSuite` → `GlutenTestsCommonTrait`
    - `V2SessionCatalogNamespaceSuite` extends `V2SessionCatalogBaseSuite` → `GlutenTestsCommonTrait`

### Package: `org/apache/spark/sql/execution/joins`

**Missing Suite Files**: 2

- **HashedRelationSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/joins/HashedRelationSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/joins/GlutenHashedRelationSuite.scala`
  - Package: `org.apache.spark.sql.execution.joins`
  - Classes (1):
    - `HashedRelationSuite` extends `SharedSparkSession` → `GlutenTestsCommonTrait`
- **SingleJoinSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/joins/SingleJoinSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/joins/GlutenSingleJoinSuite.scala`
  - Package: `org.apache.spark.sql.execution.joins`
  - Classes (1):
    - `SingleJoinSuite` extends `SparkPlanTest` → `GlutenTestsCommonTrait`

### Package: `org/apache/spark/sql/execution/metric`

**Missing Suite Files**: 1

- **CustomMetricsSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/metric/CustomMetricsSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/metric/GlutenCustomMetricsSuite.scala`
  - Package: `org.apache.spark.sql.execution.metric`
  - Classes (1):
    - `CustomMetricsSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`

### Package: `org/apache/spark/sql/execution/python`

**Missing Suite Files**: 4

- **RowQueueSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/python/RowQueueSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/python/GlutenRowQueueSuite.scala`
  - Package: `org.apache.spark.sql.execution.python`
  - Classes (1):
    - `RowQueueSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **PythonUDFSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/python/PythonUDFSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/python/GlutenPythonUDFSuite.scala`
  - Package: `org.apache.spark.sql.execution.python`
  - Classes (1):
    - `PythonUDFSuite` extends `QueryTest` → `GlutenSQLTestsTrait`
- **PythonDataSourceSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/python/PythonDataSourceSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/python/GlutenPythonDataSourceSuite.scala`
  - Package: `org.apache.spark.sql.execution.python`
  - Classes (1):
    - `PythonDataSourceSuite` extends `PythonDataSourceSuiteBase` → `GlutenTestsCommonTrait`
- **PythonUDTFSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/execution/python/PythonUDTFSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/python/GlutenPythonUDTFSuite.scala`
  - Package: `org.apache.spark.sql.execution.python`
  - Classes (1):
    - `PythonUDTFSuite` extends `QueryTest` → `GlutenSQLTestsTrait`

### Package: `org/apache/spark/sql/sources`

**Missing Suite Files**: 8

- **DataSourceAnalysisSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/sources/DataSourceAnalysisSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/sources/GlutenDataSourceAnalysisSuite.scala`
  - Package: `org.apache.spark.sql.sources`
  - Classes (1):
    - `DataSourceAnalysisSuite` extends `SparkFunSuite` → `GlutenTestsCommonTrait`
- **DisableUnnecessaryBucketedScanWithHiveSupportSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/hive/src/test/scala/org/apache/spark/sql/sources/DisableUnnecessaryBucketedScanWithHiveSupportSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/sources/GlutenDisableUnnecessaryBucketedScanWithHiveSupportSuite.scala`
  - Package: `org.apache.spark.sql.sources`
  - Classes (1):
    - `DisableUnnecessaryBucketedScanWithHiveSupportSuite` extends `DisableUnnecessaryBucketedScanSuite` → `GlutenTestsCommonTrait`
- **SimpleTextHadoopFsRelationSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/hive/src/test/scala/org/apache/spark/sql/sources/SimpleTextHadoopFsRelationSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/sources/GlutenSimpleTextHadoopFsRelationSuite.scala`
  - Package: `org.apache.spark.sql.sources`
  - Classes (1):
    - `SimpleTextHadoopFsRelationSuite` extends `HadoopFsRelationTest` → `GlutenTestsCommonTrait`
- **BucketedReadWithHiveSupportSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/hive/src/test/scala/org/apache/spark/sql/sources/BucketedReadWithHiveSupportSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/sources/GlutenBucketedReadWithHiveSupportSuite.scala`
  - Package: `org.apache.spark.sql.sources`
  - Classes (1):
    - `BucketedReadWithHiveSupportSuite` extends `BucketedReadSuite` → `GlutenTestsCommonTrait`
- **ParquetHadoopFsRelationSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/hive/src/test/scala/org/apache/spark/sql/sources/ParquetHadoopFsRelationSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/sources/GlutenParquetHadoopFsRelationSuite.scala`
  - Package: `org.apache.spark.sql.sources`
  - Classes (1):
    - `ParquetHadoopFsRelationSuite` extends `HadoopFsRelationTest` → `GlutenTestsCommonTrait`
- **JsonHadoopFsRelationSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/hive/src/test/scala/org/apache/spark/sql/sources/JsonHadoopFsRelationSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/sources/GlutenJsonHadoopFsRelationSuite.scala`
  - Package: `org.apache.spark.sql.sources`
  - Classes (1):
    - `JsonHadoopFsRelationSuite` extends `HadoopFsRelationTest` → `GlutenTestsCommonTrait`
- **CommitFailureTestRelationSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/hive/src/test/scala/org/apache/spark/sql/sources/CommitFailureTestRelationSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/sources/GlutenCommitFailureTestRelationSuite.scala`
  - Package: `org.apache.spark.sql.sources`
  - Classes (1):
    - `CommitFailureTestRelationSuite` extends `SQLTestUtils` → `GlutenTestsCommonTrait`
- **BucketedWriteWithHiveSupportSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/hive/src/test/scala/org/apache/spark/sql/sources/BucketedWriteWithHiveSupportSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/sources/GlutenBucketedWriteWithHiveSupportSuite.scala`
  - Package: `org.apache.spark.sql.sources`
  - Classes (1):
    - `BucketedWriteWithHiveSupportSuite` extends `BucketedWriteSuite` → `GlutenTestsCommonTrait`

---

## Recommended Processing Order

Process packages in this order (smallest to largest):

1. `org/apache/spark/sql/execution/datasources/csv` (1 files, 1 classes)
2. `org/apache/spark/sql/execution/datasources/json` (1 files, 1 classes)
3. `org/apache/spark/sql/execution/datasources/orc` (1 files, 1 classes)
4. `org/apache/spark/sql/execution/datasources/text` (1 files, 2 classes)
5. `org/apache/spark/sql/execution/metric` (1 files, 1 classes)
6. `org/apache/spark/sql/errors` (2 files, 2 classes)
7. `org/apache/spark/sql/execution/datasources/v2` (2 files, 3 classes)
8. `org/apache/spark/sql/execution/joins` (2 files, 2 classes)
9. `org/apache/spark/sql/execution/python` (4 files, 4 classes)
10. `org/apache/spark/sql/execution/datasources/parquet` (5 files, 5 classes)
11. `org/apache/spark/sql/sources` (8 files, 8 classes)
12. `org/apache/spark/sql/execution/datasources` (10 files, 10 classes)
13. `org/apache/spark/sql/catalyst/expressions/aggregate` (11 files, 11 classes)
14. `org/apache/spark/sql/connector` (11 files, 13 classes)
15. `org/apache/spark/sql/execution` (34 files, 37 classes)
16. `org/apache/spark/sql/catalyst/expressions` (35 files, 35 classes)
17. `org/apache/spark/sql` (37 files, 47 classes)

---

## Next Steps

1. Review the missing suites above
2. Choose packages to process
3. Run for each package:
   ```bash
   conda run -n base python /tmp/generate_gluten_suites.py \
     --spark-version 40 \
     --package <package_name>
   ```