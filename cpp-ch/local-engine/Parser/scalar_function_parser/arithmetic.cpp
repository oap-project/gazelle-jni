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
#include <Core/Settings.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Functions/FunctionHelpers.h>
#include <Parser/FunctionParser.h>
#include <Parser/TypeParser.h>
#include <Common/BlockTypeUtils.h>
#include <Common/GlutenSettings.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace local_engine
{
using namespace DB;
class DecimalType
{
    static constexpr Int32 spark_max_precision = 38;
    static constexpr Int32 spark_max_scale = 38;
    static constexpr Int32 minimum_adjusted_scale = 6;

    static constexpr Int32 chickhouse_max_precision = DB::DataTypeDecimal256::maxPrecision();
    static constexpr Int32 chickhouse_max_scale = DB::DataTypeDecimal128::maxPrecision();

public:
    Int32 precision;
    Int32 scale;

private:
    static DecimalType bounded_to_click_house(const Int32 precision, const Int32 scale)
    {
        return DecimalType(std::min(precision, chickhouse_max_precision), std::min(scale, chickhouse_max_scale));
    }

public:
    static DecimalType evalAddSubstractDecimalType(const Int32 p1, const Int32 s1, const Int32 p2, const Int32 s2)
    {
        const Int32 scale = s1;
        const Int32 precision = scale + std::max(p1 - s1, p2 - s2) + 1;
        return bounded_to_click_house(precision, scale);
    }

    static DecimalType evalDividetDecimalType(const Int32 p1, const Int32 s1, const Int32 p2, const Int32 s2)
    {
        const Int32 scale = std::max(minimum_adjusted_scale, s1 + p2 + 1);
        const Int32 precision = p1 - s1 + s2 + scale;
        return bounded_to_click_house(precision, scale);
    }

    static DecimalType evalModuloDecimalType(const Int32 p1, const Int32 s1, const Int32 p2, const Int32 s2)
    {
        const Int32 scale = std::max(s1, s2);
        const Int32 precision = std::min(p1 - s1, p2 - s2) + scale;
        return bounded_to_click_house(precision, scale);
    }

    static DecimalType evalMultiplyDecimalType(const Int32 p1, const Int32 s1, const Int32 p2, const Int32 s2)
    {
        const Int32 scale = s1;
        const Int32 precision = p1 + p2 + 1;
        return bounded_to_click_house(precision, scale);
    }
};

class FunctionParserBinaryArithmetic : public FunctionParser
{
protected:
    ActionsDAG::NodeRawConstPtrs convertBinaryArithmeticFunDecimalArgs(
        ActionsDAG & actions_dag,
        const ActionsDAG::NodeRawConstPtrs & args,
        const DecimalType & eval_type,
        const substrait::Expression_ScalarFunction & arithmeticFun) const
    {
        const Int32 precision = eval_type.precision;
        const Int32 scale = eval_type.scale;

        ActionsDAG::NodeRawConstPtrs new_args;
        new_args.reserve(args.size());

        ActionsDAG::NodeRawConstPtrs cast_args;
        cast_args.reserve(2);
        cast_args.emplace_back(args[0]);
        DataTypePtr ch_type = createDecimal<DataTypeDecimal>(precision, scale);
        ch_type = wrapNullableType(arithmeticFun.output_type().decimal().nullability(), ch_type);
        const String type_name = ch_type->getName();
        const DataTypePtr str_type = std::make_shared<DataTypeString>();
        const ActionsDAG::Node * type_node
            = &actions_dag.addColumn(ColumnWithTypeAndName(str_type->createColumnConst(1, type_name), str_type, getUniqueName(type_name)));
        cast_args.emplace_back(type_node);
        const ActionsDAG::Node * cast_node = toFunctionNode(actions_dag, "CAST", cast_args);
        actions_dag.addOrReplaceInOutputs(*cast_node);
        new_args.emplace_back(cast_node);
        new_args.emplace_back(args[1]);
        return new_args;
    }

    DecimalType getDecimalType(const DataTypePtr & left, const DataTypePtr & right) const
    {
        assert(isDecimal(left) && isDecimal(right));
        const Int32 p1 = getDecimalPrecision(*left);
        const Int32 s1 = getDecimalScale(*left);
        const Int32 p2 = getDecimalPrecision(*right);
        const Int32 s2 = getDecimalScale(*right);
        return internalEvalType(p1, s1, p2, s2);
    }

    virtual DecimalType internalEvalType(Int32 p1, Int32 s1, Int32 p2, Int32 s2) const = 0;

    const ActionsDAG::Node *
    checkDecimalOverflow(ActionsDAG & actions_dag, const ActionsDAG::Node * func_node, Int32 precision, Int32 scale) const
    {
        //TODO: checkDecimalOverflowSpark throw exception per configuration
        const DB::ActionsDAG::NodeRawConstPtrs overflow_args
            = {func_node,
               expression_parser->addConstColumn(actions_dag, std::make_shared<DataTypeInt32>(), precision),
               expression_parser->addConstColumn(actions_dag, std::make_shared<DataTypeInt32>(), scale)};
        return toFunctionNode(actions_dag, "checkDecimalOverflowSparkOrNull", overflow_args);
    }

    virtual const DB::ActionsDAG::Node * createFunctionNode(
        DB::ActionsDAG & actions_dag,
        const String & func_name,
        const DB::ActionsDAG::NodeRawConstPtrs & args,
        DataTypePtr result_type) const
    {
        return toFunctionNode(actions_dag, func_name, args);
    }

public:
    explicit FunctionParserBinaryArithmetic(ParserContextPtr parser_context_) : FunctionParser(parser_context_) { }
    const ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func, ActionsDAG & actions_dag) const override
    {
        const auto ch_func_name = getCHFunctionName(substrait_func);
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);

        if (parsed_args.size() != 2)
            throw Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly two arguments", getName());

        const auto left_type = DB::removeNullable(parsed_args[0]->result_type);
        const auto right_type = DB::removeNullable(parsed_args[1]->result_type);
        const auto result_type = removeNullable(TypeParser::parseType(substrait_func.output_type()));
        const auto * func_node = createFunctionNode(actions_dag, ch_func_name, parsed_args, result_type);
        return convertNodeTypeIfNeeded(substrait_func, func_node, actions_dag);
    }
};

class FunctionParserPlus final : public FunctionParserBinaryArithmetic
{
public:
    explicit FunctionParserPlus(ParserContextPtr parser_context_) : FunctionParserBinaryArithmetic(parser_context_) { }

    static constexpr auto name = "add";
    String getName() const override { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction & substrait_func) const override { return "plus"; }

protected:
    DecimalType internalEvalType(const Int32 p1, const Int32 s1, const Int32 p2, const Int32 s2) const override
    {
        return DecimalType::evalAddSubstractDecimalType(p1, s1, p2, s2);
    }

    const DB::ActionsDAG::Node * createFunctionNode(
        DB::ActionsDAG & actions_dag,
        const String & func_name,
        const DB::ActionsDAG::NodeRawConstPtrs & new_args,
        DataTypePtr result_type) const override
    {
        const auto * left_arg = new_args[0];
        const auto * right_arg = new_args[1];

        if (isDecimal(removeNullable(left_arg->result_type)) && isDecimal(removeNullable(right_arg->result_type)))
        {
            const ActionsDAG::Node * type_node = &actions_dag.addColumn(ColumnWithTypeAndName(
                result_type->createColumnConstWithDefaultValue(1), result_type, getUniqueName(result_type->getName())));

            const auto & settings = parser_context->queryContext()->getSettingsRef();
            auto function_name = settings.has("arithmetic.decimal.mode") && settingsEqual(settings, "arithmetic.decimal.mode", "EFFECT")
                ? "sparkDecimalPlusEffect"
                : "sparkDecimalPlus";

            return toFunctionNode(actions_dag, function_name, {left_arg, right_arg, type_node});
        }

        return toFunctionNode(actions_dag, "plus", {left_arg, right_arg});
    }
};

class FunctionParserMinus final : public FunctionParserBinaryArithmetic
{
public:
    explicit FunctionParserMinus(ParserContextPtr parser_context_) : FunctionParserBinaryArithmetic(parser_context_) { }

    static constexpr auto name = "subtract";
    String getName() const override { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction & substrait_func) const override { return "minus"; }

protected:
    DecimalType internalEvalType(const Int32 p1, const Int32 s1, const Int32 p2, const Int32 s2) const override
    {
        return DecimalType::evalAddSubstractDecimalType(p1, s1, p2, s2);
    }

    const DB::ActionsDAG::Node * createFunctionNode(
        DB::ActionsDAG & actions_dag,
        const String & func_name,
        const DB::ActionsDAG::NodeRawConstPtrs & new_args,
        DataTypePtr result_type) const override
    {
        const auto * left_arg = new_args[0];
        const auto * right_arg = new_args[1];

        if (isDecimal(removeNullable(left_arg->result_type)) && isDecimal(removeNullable(right_arg->result_type)))
        {
            const ActionsDAG::Node * type_node = &actions_dag.addColumn(ColumnWithTypeAndName(
                result_type->createColumnConstWithDefaultValue(1), result_type, getUniqueName(result_type->getName())));

            const auto & settings = parser_context->queryContext()->getSettingsRef();
            auto function_name = settings.has("arithmetic.decimal.mode") && settingsEqual(settings, "arithmetic.decimal.mode", "EFFECT")
                ? "sparkDecimalMinusEffect"
                : "sparkDecimalMinus";

            return toFunctionNode(actions_dag, function_name, {left_arg, right_arg, type_node});
        }

        return toFunctionNode(actions_dag, "minus", {left_arg, right_arg});
    }
};

class FunctionParserMultiply final : public FunctionParserBinaryArithmetic
{
public:
    explicit FunctionParserMultiply(ParserContextPtr parser_context_) : FunctionParserBinaryArithmetic(parser_context_) { }
    static constexpr auto name = "multiply";
    String getName() const override { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction & substrait_func) const override { return "multiply"; }

protected:
    DecimalType internalEvalType(const Int32 p1, const Int32 s1, const Int32 p2, const Int32 s2) const override
    {
        return DecimalType::evalMultiplyDecimalType(p1, s1, p2, s2);
    }

    const DB::ActionsDAG::Node * createFunctionNode(
        DB::ActionsDAG & actions_dag,
        const String & func_name,
        const DB::ActionsDAG::NodeRawConstPtrs & new_args,
        DataTypePtr result_type) const override
    {
        const auto * left_arg = new_args[0];
        const auto * right_arg = new_args[1];

        if (isDecimal(removeNullable(left_arg->result_type)) && isDecimal(removeNullable(right_arg->result_type)))
        {
            const ActionsDAG::Node * type_node = &actions_dag.addColumn(ColumnWithTypeAndName(
                result_type->createColumnConstWithDefaultValue(1), result_type, getUniqueName(result_type->getName())));

            const auto & settings = parser_context->queryContext()->getSettingsRef();
            auto function_name = settings.has("arithmetic.decimal.mode") && settingsEqual(settings, "arithmetic.decimal.mode", "EFFECT")
                ? "sparkDecimalMultiplyEffect"
                : "sparkDecimalMultiply";

            return toFunctionNode(actions_dag, function_name, {left_arg, right_arg, type_node});
        }

        return toFunctionNode(actions_dag, "multiply", {left_arg, right_arg});
    }
};

class FunctionParserModulo final : public FunctionParserBinaryArithmetic
{
public:
    explicit FunctionParserModulo(ParserContextPtr parser_context_) : FunctionParserBinaryArithmetic(parser_context_) { }
    static constexpr auto name = "modulus";
    String getName() const override { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction & substrait_func) const override { return "modulo"; }

protected:
    DecimalType internalEvalType(const Int32 p1, const Int32 s1, const Int32 p2, const Int32 s2) const override
    {
        return DecimalType::evalModuloDecimalType(p1, s1, p2, s2);
    }

    const DB::ActionsDAG::Node * createFunctionNode(
        DB::ActionsDAG & actions_dag,
        const String & func_name,
        const DB::ActionsDAG::NodeRawConstPtrs & new_args,
        DataTypePtr result_type) const override
    {
        const auto * left_arg = new_args[0];
        const auto * right_arg = new_args[1];

        if (isDecimal(removeNullable(left_arg->result_type)) || isDecimal(removeNullable(right_arg->result_type)))
        {
            const ActionsDAG::Node * type_node = &actions_dag.addColumn(ColumnWithTypeAndName(
                result_type->createColumnConstWithDefaultValue(1), result_type, getUniqueName(result_type->getName())));

            const auto & settings = parser_context->queryContext()->getSettingsRef();
            auto function_name = settings.has("arithmetic.decimal.mode") && settingsEqual(settings, "arithmetic.decimal.mode", "EFFECT")
                ? "NameSparkDecimalModuloEffect"
                : "NameSparkDecimalModulo";
            ;
            return toFunctionNode(actions_dag, function_name, {left_arg, right_arg, type_node});
        }

        return toFunctionNode(actions_dag, "spark_modulo", {left_arg, right_arg});
    }
};

class FunctionParserDivide final : public FunctionParserBinaryArithmetic
{
public:
    explicit FunctionParserDivide(ParserContextPtr parser_context_) : FunctionParserBinaryArithmetic(parser_context_) { }
    static constexpr auto name = "divide";
    String getName() const override { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction & substrait_func) const override { return "divide"; }

protected:
    DecimalType internalEvalType(const Int32 p1, const Int32 s1, const Int32 p2, const Int32 s2) const override
    {
        return DecimalType::evalDividetDecimalType(p1, s1, p2, s2);
    }

    const DB::ActionsDAG::Node * createFunctionNode(
        DB::ActionsDAG & actions_dag,
        const String & func_name,
        const DB::ActionsDAG::NodeRawConstPtrs & new_args,
        DataTypePtr result_type) const override
    {
        assert(func_name == name);
        const auto * left_arg = new_args[0];
        const auto * right_arg = new_args[1];

        if (isDecimal(removeNullable(left_arg->result_type)) || isDecimal(removeNullable(right_arg->result_type)))
        {
            const ActionsDAG::Node * type_node = &actions_dag.addColumn(ColumnWithTypeAndName(
                result_type->createColumnConstWithDefaultValue(1), result_type, getUniqueName(result_type->getName())));

            const auto & settings = parser_context->queryContext()->getSettingsRef();
            auto function_name = settings.has("arithmetic.decimal.mode") && settingsEqual(settings, "arithmetic.decimal.mode", "EFFECT")
                ? "sparkDecimalDivideEffect"
                : "sparkDecimalDivide";
            ;
            return toFunctionNode(actions_dag, function_name, {left_arg, right_arg, type_node});
        }

        return toFunctionNode(actions_dag, "sparkDivide", {left_arg, right_arg});
    }
};

static FunctionParserRegister<FunctionParserPlus> register_plus;
static FunctionParserRegister<FunctionParserMinus> register_minus;
static FunctionParserRegister<FunctionParserMultiply> register_mltiply;
static FunctionParserRegister<FunctionParserDivide> register_divide;
static FunctionParserRegister<FunctionParserModulo> register_modulo;

}
