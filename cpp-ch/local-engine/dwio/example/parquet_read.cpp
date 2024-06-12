//
// Created by chang on 23-9-22.
//
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/expression/ExprToSubfieldFilter.h"
#include "velox/expression/Expr.h"
#include "velox/parse/TypeResolver.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

using namespace facebook::velox;
using namespace facebook::velox::common;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::parquet;

namespace {
auto defaultPool = memory::addDefaultLeafMemoryPool();
}

ParquetReader createReader(
    const std::string& path,
    const ReaderOptions& opts) {
  return ParquetReader(
      std::make_unique<BufferedInput>(
          std::make_shared<LocalReadFile>(path), opts.getMemoryPool()),
      opts);
}

dwio::common::RowReaderOptions getReaderOpts(
    const RowTypePtr& rowType,
    bool fileColumnNamesReadAsLowerCase = false) {
  dwio::common::RowReaderOptions row_reader_opts;
  row_reader_opts.select(
      std::make_shared<facebook::velox::dwio::common::ColumnSelector>(
          rowType,
          rowType->names(),
          nullptr,
          fileColumnNamesReadAsLowerCase));

  return row_reader_opts;
}

std::shared_ptr<facebook::velox::common::ScanSpec> makeScanSpec(
    const RowTypePtr& rowType) {
  auto scan_spec = std::make_shared<facebook::velox::common::ScanSpec>("root");
  scan_spec->addAllChildFields(*rowType);
  return scan_spec;
}

core::TypedExprPtr parseExpr(
    const std::string& text,
    const RowTypePtr& rowType,
    const parse::ParseOptions& options,
    memory::MemoryPool* pool) {
  auto untyped = parse::parseExpr(text, options);
  return core::Expressions::inferTypes(untyped, rowType, pool);
}

int main(int /*argc*/, char** /*argv*/) {

  functions::prestosql::registerAllScalarFunctions("");
  parse::registerTypeResolver();
   
  const std::string sample("/home/hongbin/code/gluten-velox/velox-chenchang/velox/dwio/parquet/tests/examples/sample.parquet");
  ReaderOptions reader_options{defaultPool.get()};
  ParquetReader reader = createReader(sample, reader_options);

  auto schema = ROW({"a", "b"}, {BIGINT(), DOUBLE()});
  auto row_reader_opts = getReaderOpts(schema);
  auto scan_spec = makeScanSpec(schema);

  // set filters:
  parse::ParseOptions options;
  core::QueryCtx query_ctx;
  exec::SimpleExpressionEvaluator evaluator(&query_ctx, defaultPool.get());
  auto filter_expr = parseExpr("a > 5", schema, options, defaultPool.get());
  auto [subfield, subfieldFilter] = exec::toSubfieldFilter(filter_expr, &evaluator);
  scan_spec->childByName("a")->setFilter(std::move(subfieldFilter));


  row_reader_opts.setScanSpec(scan_spec);
  auto row_reader = reader.createRowReader(row_reader_opts);
  uint64_t total = 0;
  uint64_t returned = 0;
  VectorPtr result = BaseVector::create(schema, 0, defaultPool.get());
  returned = row_reader->next(1000, result);
  std::cout << "returened: " << returned << '\n';
  total += result->size();
  std::cout << total << '\n';

  returned  = row_reader->next(1000, result);
  std::cout << "returened: " << returned << '\n';
  total += result->size();
  std::cout << total << '\n';

  returned  = row_reader->next(1000, result);
  std::cout << "returened: " << returned << '\n';
  total += result->size();
  std::cout << total << '\n';
  return 0;
}
