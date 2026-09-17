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
#include "compute/VeloxExpressionEvaluator.h"

#include <initializer_list>

#include "config/VeloxConfig.h"
#include "substrait/SubstraitToVeloxExpr.h"
#include "substrait/extended_expression.pb.h"
#include "utils/Exception.h"
#include "utils/ObjectStore.h"
#include "velox/expression/RegisterSpecialForm.h"
#include "velox/functions/sparksql/SparkQueryConfig.h"
#include "velox/functions/sparksql/registration/Register.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace gluten {
using namespace facebook::velox;

class VeloxExpressionEvaluatorTest : public testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    functions::sparksql::registerFunctions("");
    exec::registerFunctionCallToSpecialForms();
  }

  static ::substrait::ExtendedExpression rtrim() {
    ::substrait::ExtendedExpression expression;
    expression.mutable_base_schema()->add_names("input");
    expression.mutable_base_schema()->mutable_struct_()->add_types()->mutable_string();
    auto* mapping = expression.add_extensions()->mutable_extension_function();
    mapping->set_function_anchor(1);
    mapping->set_name("rtrim:str");
    auto* reference = expression.add_referred_expr();
    reference->add_output_names("result");
    auto* function = reference->mutable_expression()->mutable_scalar_function();
    function->set_function_reference(1);
    function->mutable_output_type()->mutable_string();
    auto* selection = function->add_arguments()->mutable_value()->mutable_selection();
    selection->mutable_root_reference();
    selection->mutable_direct_reference()->mutable_struct_field()->set_field(0);
    return expression;
  }

  static ::substrait::Expression field(int32_t ordinal, std::initializer_list<int32_t> children = {}) {
    ::substrait::Expression expression;
    auto* reference = expression.mutable_selection();
    reference->mutable_root_reference();
    auto* segment = reference->mutable_direct_reference()->mutable_struct_field();
    segment->set_field(ordinal);
    for (auto child : children) {
      segment = segment->mutable_child()->mutable_struct_field();
      segment->set_field(child);
    }
    return expression;
  }

  std::unique_ptr<VeloxExpressionEvaluator> compile(const ::substrait::ExtendedExpression& expression) {
    auto bytes = expression.SerializeAsString();
    return std::make_unique<VeloxExpressionEvaluator>(
        pool_, core::QueryCtx::create(), reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size());
  }

  std::shared_ptr<VeloxColumnarBatch> batch(const VectorPtr& column) {
    return std::make_shared<VeloxColumnarBatch>(makeRowVector({"different_input_name"}, {column}));
  }
};

TEST_F(VeloxExpressionEvaluatorTest, reusesColumnarInputsAcrossBatches) {
  auto evaluator = compile(rtrim());
  auto values = makeNullableFlatVector<std::string>(
      {"0123456789", "data  ", "", "   ", "\t", "\xC2\xA0", std::nullopt, std::string(64, 'x')});
  auto input = batch(values);
  auto output = evaluator->evaluate(input);
  EXPECT_EQ(output->numColumns(), 1);
  EXPECT_EQ(output->numRows(), values->size());
  EXPECT_EQ(output->getRowVector()->type()->asRow().nameOf(0), "result");
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>(
          {"0123456789", "data", "", "", "\t", "\xC2\xA0", std::nullopt, std::string(64, 'x')}),
      output->getRowVector()->childAt(0));
  auto smaller = evaluator->evaluate(batch(makeFlatVector<std::string>({"next "})));
  EXPECT_EQ(smaller->numRows(), 1);
  test::assertEqualVectors(makeFlatVector<std::string>({"next"}), smaller->getRowVector()->childAt(0));
  auto empty = evaluator->evaluate(batch(makeFlatVector<std::string>(std::vector<std::string>{})));
  EXPECT_EQ(empty->numRows(), 0);
  EXPECT_EQ(empty->numColumns(), 1);
  EXPECT_EQ(empty->getRowVector()->size(), 0);
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>(
          {"0123456789", "data  ", "", "   ", "\t", "\xC2\xA0", std::nullopt, std::string(64, 'x')}),
      input->getRowVector()->childAt(0));
  EXPECT_EQ(output->numRows(), 8);
}

TEST_F(VeloxExpressionEvaluatorTest, distinctSameTypeRootColumns) {
  auto expression = rtrim();
  auto* schema = expression.mutable_base_schema();
  schema->clear_names();
  schema->add_names("duplicate");
  schema->add_names("duplicate");
  schema->mutable_struct_()->clear_types();
  schema->mutable_struct_()->add_types()->mutable_i32();
  schema->mutable_struct_()->add_types()->mutable_i32();
  expression.mutable_extensions(0)->mutable_extension_function()->set_name("add:i32_i32");
  auto* function = expression.mutable_referred_expr(0)->mutable_expression()->mutable_scalar_function();
  function->mutable_output_type()->clear_kind();
  function->mutable_output_type()->mutable_i32();
  function->clear_arguments();
  function->add_arguments()->mutable_value()->CopyFrom(field(0));
  function->add_arguments()->mutable_value()->CopyFrom(field(1));
  auto evaluator = compile(expression);
  auto input = std::make_shared<VeloxColumnarBatch>(
      makeRowVector({"same", "same"}, {makeFlatVector<int32_t>({3, 10}), makeFlatVector<int32_t>({4, 20})}));
  auto output = evaluator->evaluate(input);
  test::assertEqualVectors(makeFlatVector<int32_t>({7, 30}), output->getRowVector()->childAt(0));

  auto second = evaluator->evaluate(std::make_shared<VeloxColumnarBatch>(
      makeRowVector({"other", "names"}, {makeFlatVector<int32_t>({17}), makeFlatVector<int32_t>({2})})));
  test::assertEqualVectors(makeFlatVector<int32_t>({19}), second->getRowVector()->childAt(0));
  test::assertEqualVectors(makeFlatVector<int32_t>({7, 30}), output->getRowVector()->childAt(0));
  test::assertEqualVectors(makeFlatVector<int32_t>({3, 10}), input->getRowVector()->childAt(0));
  test::assertEqualVectors(makeFlatVector<int32_t>({4, 20}), input->getRowVector()->childAt(1));
}

TEST_F(VeloxExpressionEvaluatorTest, mixedStringAndIntegerRepeatArguments) {
  auto expression = rtrim();
  expression.mutable_base_schema()->add_names("count");
  expression.mutable_base_schema()->mutable_struct_()->add_types()->mutable_i32();
  expression.mutable_extensions(0)->mutable_extension_function()->set_name("repeat:str_i32");
  auto* function = expression.mutable_referred_expr(0)->mutable_expression()->mutable_scalar_function();
  function->add_arguments()->mutable_value()->CopyFrom(field(1));
  auto evaluator = compile(expression);
  auto input = std::make_shared<VeloxColumnarBatch>(makeRowVector(
      {makeNullableFlatVector<std::string>({"hi", std::nullopt, "x", "ab"}), makeFlatVector<int32_t>({2, 1, 0, 3})}));
  auto output = evaluator->evaluate(input);
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"hihi", std::nullopt, "", "ababab"}), output->getRowVector()->childAt(0));
  auto second = evaluator->evaluate(std::make_shared<VeloxColumnarBatch>(
      makeRowVector({makeFlatVector<std::string>({"xy"}), makeFlatVector<int32_t>({3})})));
  test::assertEqualVectors(makeFlatVector<std::string>({"xyxyxy"}), second->getRowVector()->childAt(0));
  test::assertEqualVectors(makeFlatVector<int32_t>({2, 1, 0, 3}), input->getRowVector()->childAt(1));
}

TEST_F(VeloxExpressionEvaluatorTest, reorderedAndRepeatedRootReferences) {
  auto expression = rtrim();
  expression.mutable_base_schema()->add_names("right");
  expression.mutable_base_schema()->mutable_struct_()->add_types()->mutable_string();
  expression.mutable_extensions(0)->mutable_extension_function()->set_name("concat:str_str_str");
  auto* function = expression.mutable_referred_expr(0)->mutable_expression()->mutable_scalar_function();
  function->clear_arguments();
  for (auto ordinal : {1, 0, 1}) {
    function->add_arguments()->mutable_value()->CopyFrom(field(ordinal));
  }
  auto evaluator = compile(expression);
  auto output = evaluator->evaluate(std::make_shared<VeloxColumnarBatch>(
      makeRowVector({makeFlatVector<std::string>({"left", "a"}), makeFlatVector<std::string>({"RIGHT", "b"})})));
  test::assertEqualVectors(makeFlatVector<std::string>({"RIGHTleftRIGHT", "bab"}), output->getRowVector()->childAt(0));
  auto second = evaluator->evaluate(std::make_shared<VeloxColumnarBatch>(
      makeRowVector({makeFlatVector<std::string>({"x"}), makeFlatVector<std::string>({"YZ"})})));
  test::assertEqualVectors(makeFlatVector<std::string>({"YZxYZ"}), second->getRowVector()->childAt(0));
  test::assertEqualVectors(makeFlatVector<std::string>({"RIGHTleftRIGHT", "bab"}), output->getRowVector()->childAt(0));
}

TEST_F(VeloxExpressionEvaluatorTest, outputSurvivesEvaluatorAndInputRelease) {
  const std::string value(64, 'x');
  std::shared_ptr<VeloxColumnarBatch> output;
  {
    auto evaluator = compile(rtrim());
    auto input = batch(makeFlatVector<std::string>({value, value}));
    output = evaluator->evaluate(input);
    EXPECT_EQ(
        output->getRowVector()->childAt(0)->as<SimpleVector<StringView>>()->valueAt(0).data(),
        input->getRowVector()->childAt(0)->as<SimpleVector<StringView>>()->valueAt(0).data());
    auto next = evaluator->evaluate(batch(makeFlatVector<std::string>({"changed ", "other "})));
    test::assertEqualVectors(makeFlatVector<std::string>({"changed", "other"}), next->getRowVector()->childAt(0));
  }
  test::assertEqualVectors(makeFlatVector<std::string>({value, value}), output->getRowVector()->childAt(0));
}

TEST_F(VeloxExpressionEvaluatorTest, constantDictionaryAndNullBatches) {
  auto evaluator = compile(rtrim());
  auto base = makeNullableFlatVector<std::string>({" x ", std::nullopt, "keep"});
  auto dictionary = wrapInDictionary(makeIndices({2, 0, 1, 0, 2}), base);
  auto output = evaluator->evaluate(batch(dictionary));
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"keep", " x", std::nullopt, " x", "keep"}),
      output->getRowVector()->childAt(0));
  for (vector_size_t index = 0; index < base->size(); ++index) {
    auto constant = BaseVector::wrapInConstant(9, index, base);
    auto result = evaluator->evaluate(batch(constant));
    EXPECT_EQ(result->numRows(), 9);
    auto expected = makeNullableFlatVector<std::string>(
        index == 1 ? std::vector<std::optional<std::string>>(9, std::nullopt)
                   : std::vector<std::optional<std::string>>(9, index == 0 ? " x" : "keep"));
    test::assertEqualVectors(expected, result->getRowVector()->childAt(0));
  }
}

TEST_F(VeloxExpressionEvaluatorTest, literalWithoutColumnsAndZeroRows) {
  ::substrait::ExtendedExpression expression;
  expression.mutable_base_schema()->mutable_struct_();
  auto* ref = expression.add_referred_expr();
  ref->add_output_names("result");
  const std::string value(64, 'z');
  ref->mutable_expression()->mutable_literal()->set_string(value);
  auto evaluator = compile(expression);
  std::vector<std::shared_ptr<VeloxColumnarBatch>> outputs;
  for (auto size : {5, 1, 0, 9}) {
    auto input = std::make_shared<VeloxColumnarBatch>(
        std::make_shared<RowVector>(pool(), ROW({}, {}), nullptr, size, std::vector<VectorPtr>{}));
    auto result = evaluator->evaluate(input);
    EXPECT_EQ(result->numRows(), size);
    outputs.push_back(result);
  }
  evaluator.reset();
  for (const auto& output : outputs) {
    test::assertEqualVectors(
        makeFlatVector<std::string>(std::vector<std::string>(output->numRows(), value)),
        output->getRowVector()->childAt(0));
  }
}

TEST_F(VeloxExpressionEvaluatorTest, validatesSchemaAndProtocol) {
  auto valid = rtrim();
  auto evaluator = compile(valid);
  EXPECT_ANY_THROW(evaluator->evaluate(nullptr));
  EXPECT_ANY_THROW(evaluator->evaluate(batch(makeFlatVector<int32_t>({1, 2}))));
  EXPECT_ANY_THROW(evaluator->evaluate(std::make_shared<VeloxColumnarBatch>(
      makeRowVector({makeFlatVector<std::string>({"x"}), makeFlatVector<std::string>({"y"})}))));
  auto invalid = valid;
  invalid.clear_base_schema();
  EXPECT_ANY_THROW(compile(invalid));
  invalid = valid;
  invalid.add_referred_expr()->CopyFrom(valid.referred_expr(0));
  EXPECT_ANY_THROW(compile(invalid));
  invalid = valid;
  invalid.mutable_referred_expr(0)->mutable_measure();
  EXPECT_ANY_THROW(compile(invalid));
  invalid = valid;
  invalid.mutable_referred_expr(0)->clear_output_names();
  EXPECT_ANY_THROW(compile(invalid));
  invalid = valid;
  invalid.add_extensions()->CopyFrom(valid.extensions(0));
  EXPECT_ANY_THROW(compile(invalid));
  invalid = valid;
  invalid.mutable_extensions(0)->mutable_extension_function()->set_name("missing_expression_function:str");
  EXPECT_ANY_THROW(compile(invalid));
  invalid = valid;
  invalid.mutable_referred_expr(0)->mutable_expression()->mutable_scalar_function()->set_function_reference(999);
  EXPECT_ANY_THROW(compile(invalid));
  invalid = valid;
  invalid.mutable_referred_expr(0)
      ->mutable_expression()
      ->mutable_scalar_function()
      ->mutable_arguments(0)
      ->mutable_value()
      ->mutable_selection()
      ->mutable_direct_reference()
      ->mutable_struct_field()
      ->set_field(99);
  EXPECT_ANY_THROW(compile(invalid));
  invalid = valid;
  invalid.mutable_referred_expr(0)
      ->mutable_expression()
      ->mutable_scalar_function()
      ->mutable_arguments(0)
      ->mutable_value()
      ->mutable_selection()
      ->mutable_direct_reference()
      ->mutable_list_element()
      ->set_offset(0);
  EXPECT_ANY_THROW(compile(invalid));
  const std::string corrupt("\xff\xff", 2);
  EXPECT_ANY_THROW(VeloxExpressionEvaluator(
      pool_, core::QueryCtx::create(), reinterpret_cast<const uint8_t*>(corrupt.data()), corrupt.size()));
}

TEST_F(VeloxExpressionEvaluatorTest, queryConfigUsesSparkSemanticsWithoutTask) {
  using functions::sparksql::SparkQueryConfig;
  auto sparkConfig = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>{{kAnsiEnabled, "true"}, {kSessionTimezone, "Asia/Shanghai"}});
  auto translated = createVeloxQueryConfig(sparkConfig, 7, "none");
  EXPECT_EQ(translated.at(SparkQueryConfig::qualify(SparkQueryConfig::kAnsiEnabled)), "true");
  EXPECT_EQ(translated.at(core::QueryConfig::kSessionTimezone), "Asia/Shanghai");
  EXPECT_EQ(translated.at(SparkQueryConfig::qualify(SparkQueryConfig::kPartitionId)), "7");
  EXPECT_EQ(translated.at(core::QueryConfig::kSpillEnabled), "false");
}

TEST_F(VeloxExpressionEvaluatorTest, duplicateNestedFieldNamesArePositional) {
  for (auto nestedPath : {false, true}) {
    SCOPED_TRACE(nestedPath);
    auto expression = rtrim();
    expression.clear_extensions();
    auto* inputType = expression.mutable_base_schema()->mutable_struct_()->mutable_types(0);
    inputType->clear_kind();
    for (int i = 0; i < 2; ++i) {
      inputType->mutable_struct_()->add_names("duplicate");
      inputType->mutable_struct_()->add_types()->mutable_i32();
    }
    auto leaf =
        makeRowVector({"duplicate", "duplicate"}, {makeFlatVector<int32_t>({1, 10}), makeFlatVector<int32_t>({3, 30})});
    VectorPtr nested = leaf;
    if (nestedPath) {
      const auto leafType = *inputType;
      for (auto& child : *inputType->mutable_struct_()->mutable_types()) {
        child.CopyFrom(leafType);
      }
      auto other = makeRowVector(
          {"duplicate", "duplicate"}, {makeFlatVector<int32_t>({2, 20}), makeFlatVector<int32_t>({4, 40})});
      nested = makeRowVector({"duplicate", "duplicate"}, {other, leaf});
    }
    expression.mutable_referred_expr(0)->mutable_expression()->CopyFrom(nestedPath ? field(0, {1, 1}) : field(0, {1}));
    SubstraitVeloxExprConverter legacyConverter(pool_.get(), {});
    auto legacyExpression = legacyConverter.toVeloxExpr(
        expression.referred_expr(0).expression().selection(), ROW({"input_0"}, {nested->type()}));
    EXPECT_TRUE(legacyExpression->isFieldAccessKind());
    const auto serialized = expression.SerializeAsString();
    auto evaluator = compile(expression);
    const auto inputTypeBefore = nested->type();
    const auto leafTypeBefore = leaf->type();
    auto expectedInput = BaseVector::copy(*nested, pool_.get());
    auto output = evaluator->evaluate(batch(nested));
    test::assertEqualVectors(makeFlatVector<int32_t>({3, 30}), output->getRowVector()->childAt(0));

    auto dictionary = wrapInDictionary(makeIndices({1, 0, 1}), nested);
    auto dictionaryOutput = evaluator->evaluate(batch(dictionary));
    test::assertEqualVectors(makeFlatVector<int32_t>({30, 3, 30}), dictionaryOutput->getRowVector()->childAt(0));
    auto constant = BaseVector::wrapInConstant(3, 0, nested);
    auto constantOutput = evaluator->evaluate(batch(constant));
    test::assertEqualVectors(makeFlatVector<int32_t>({3, 3, 3}), constantOutput->getRowVector()->childAt(0));

    test::assertEqualVectors(makeFlatVector<int32_t>({3, 30}), output->getRowVector()->childAt(0));
    test::assertEqualVectors(expectedInput, nested);
    EXPECT_EQ(nested->type(), inputTypeBefore);
    EXPECT_EQ(leaf->type(), leafTypeBefore);
    EXPECT_EQ(dictionary->type(), inputTypeBefore);
    EXPECT_EQ(constant->type(), inputTypeBefore);
    EXPECT_EQ(expression.SerializeAsString(), serialized);
  }
}

TEST_F(VeloxExpressionEvaluatorTest, nestedFieldReferencesArePositional) {
  auto expression = rtrim();
  auto* inputType = expression.mutable_base_schema()->mutable_struct_()->mutable_types(0);
  inputType->clear_kind();
  inputType->mutable_struct_()->add_types()->mutable_string();
  expression.mutable_referred_expr(0)
      ->mutable_expression()
      ->mutable_scalar_function()
      ->mutable_arguments(0)
      ->mutable_value()
      ->mutable_selection()
      ->mutable_direct_reference()
      ->mutable_struct_field()
      ->mutable_child()
      ->mutable_struct_field()
      ->set_field(0);
  auto evaluator = compile(expression);
  auto nested = makeRowVector({"nested_name"}, {makeFlatVector<std::string>({"nested ", "keep"})});
  auto output = evaluator->evaluate(batch(nested));
  test::assertEqualVectors(makeFlatVector<std::string>({"nested", "keep"}), output->getRowVector()->childAt(0));
  EXPECT_EQ(nested->type()->asRow().nameOf(0), "nested_name");
  auto dictionary = wrapInDictionary(makeIndices({1, 0, 1}), nested);
  auto encodedOutput = evaluator->evaluate(batch(dictionary));
  test::assertEqualVectors(
      makeFlatVector<std::string>({"keep", "nested", "keep"}), encodedOutput->getRowVector()->childAt(0));
  auto constant = BaseVector::wrapInConstant(3, 0, nested);
  auto constantOutput = evaluator->evaluate(batch(constant));
  test::assertEqualVectors(
      makeFlatVector<std::string>({"nested", "nested", "nested"}), constantOutput->getRowVector()->childAt(0));
  EXPECT_EQ(nested->type()->asRow().nameOf(0), "nested_name");
}

TEST_F(VeloxExpressionEvaluatorTest, toJsonPreservesNestedFieldNames) {
  for (const auto& names : {std::vector<std::string>{"x", "x"}, std::vector<std::string>{"x", "y"}}) {
    SCOPED_TRACE(names[1]);
    auto expression = rtrim();
    expression.mutable_extensions(0)->mutable_extension_function()->set_name("to_json:struct");
    auto* inputType = expression.mutable_base_schema()->mutable_struct_()->mutable_types(0);
    inputType->clear_kind();
    for (const auto& name : names) {
      inputType->mutable_struct_()->add_names(name);
      inputType->mutable_struct_()->add_types()->mutable_i32();
    }
    auto input = makeRowVector(names, {makeFlatVector<int32_t>({1}), makeFlatVector<int32_t>({3})});
    auto evaluator = compile(expression);
    auto output = evaluator->evaluate(batch(input));
    test::assertEqualVectors(
        makeFlatVector<std::string>({"{\"x\":1,\"" + names[1] + "\":3}"}), output->getRowVector()->childAt(0));
    EXPECT_EQ(input->type()->asRow().names(), names);
  }
}

TEST_F(VeloxExpressionEvaluatorTest, nestedCollectionNamesDoNotMutateInputs) {
  auto expression = rtrim();
  expression.clear_extensions();
  expression.mutable_referred_expr(0)->mutable_expression()->CopyFrom(field(0));
  auto* inputType = expression.mutable_base_schema()->mutable_struct_()->mutable_types(0);
  inputType->clear_kind();
  auto* mapType = inputType->mutable_list()->mutable_type()->mutable_map();
  for (auto* type : {mapType->mutable_key(), mapType->mutable_value()}) {
    for (int i = 0; i < 2; ++i) {
      type->mutable_struct_()->add_names("duplicate");
      type->mutable_struct_()->add_types()->mutable_i32();
    }
  }
  auto leaf =
      makeRowVector({"duplicate", "duplicate"}, {makeFlatVector<int32_t>({1, 10}), makeFlatVector<int32_t>({3, 30})});
  auto maps = makeMapVector({0, 1}, leaf, leaf);
  auto arrays = makeArrayVector({0, 1}, maps);
  const auto leafTypeBefore = leaf->type();
  const auto mapTypeBefore = maps->type();
  const auto arrayTypeBefore = arrays->type();
  auto expected = BaseVector::copy(*arrays, pool_.get());
  auto evaluator = compile(expression);
  auto output = evaluator->evaluate(batch(arrays))->getRowVector()->childAt(0);
  test::assertEqualVectors(expected, output);
  test::assertEqualVectors(expected, arrays);
  const auto& outputMapType = output->type()->asArray().elementType()->asMap();
  for (const auto& type : {outputMapType.keyType(), outputMapType.valueType()}) {
    EXPECT_EQ(type->asRow().names(), std::vector<std::string>({"duplicate", "duplicate"}));
  }
  EXPECT_EQ(leaf->type(), leafTypeBefore);
  EXPECT_EQ(maps->type(), mapTypeBefore);
  EXPECT_EQ(arrays->type(), arrayTypeBefore);
}

} // namespace gluten
