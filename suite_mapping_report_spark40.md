# Gluten Suite Mapping Report - Spark 40

Generated at: /home/chang/SourceCode/gluten1

## Summary Statistics

- **Total Gluten Suites**: 439
- **Mapped to Spark 40**: 428
- **Not Mapped**: 11
- **Unique Packages Covered**: 20
- **Total Missing Suite Files**: 3
- **Total Missing Suite Classes**: 3

---

## Missing Suites by Package

*Review this section to decide which packages to process*

### Package: `org/apache/spark/sql/catalyst/expressions/aggregate`

**Missing Suite Files**: 2

- **CentralMomentAggSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/aggregate/CentralMomentAggSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/aggregate/GlutenCentralMomentAggSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions.aggregate`
  - Classes (1):
    - `CentralMomentAggSuite` extends `TestWithAndWithoutCodegen` → `GlutenTestsCommonTrait`
- **CovarianceAggSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/aggregate/CovarianceAggSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/catalyst/expressions/aggregate/GlutenCovarianceAggSuite.scala`
  - Package: `org.apache.spark.sql.catalyst.expressions.aggregate`
  - Classes (1):
    - `CovarianceAggSuite` extends `TestWithAndWithoutCodegen` → `GlutenTestsCommonTrait`

### Package: `org/apache/spark/sql/connector`

**Missing Suite Files**: 1

- **StaticProcedureSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/connector/StaticProcedureSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/connector/GlutenStaticProcedureSuite.scala`
  - Package: `org.apache.spark.sql.connector`
  - Classes (1):
    - `StaticProcedureSuite` extends `SharedSparkSession` → `GlutenTestsCommonTrait`

---

## Recommended Processing Order

Process packages in this order (smallest to largest):

1. `org/apache/spark/sql/connector` (1 files, 1 classes)
2. `org/apache/spark/sql/catalyst/expressions/aggregate` (2 files, 2 classes)

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