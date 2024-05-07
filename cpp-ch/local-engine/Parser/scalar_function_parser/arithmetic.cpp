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

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Functions/FunctionHelpers.h>
#include <Parser/FunctionParser.h>
#include <Parser/TypeParser.h>
#include <Common/CHUtil.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace local_engine
{

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
    static DecimalType bounded_to_spark(const Int32 precision, const Int32 scale)
    {
        return DecimalType(std::min(precision, spark_max_precision), std::min(scale, spark_max_scale));
    }
    static DecimalType bounded_to_click_house(const Int32 precision, const Int32 scale)
    {
        return DecimalType(std::min(precision, chickhouse_max_precision), std::min(scale, chickhouse_max_scale));
    }
    static void check_negative_scale(const Int32 scale)
    {
        /// only support spark.sql.legacy.allowNegativeScaleOfDecimal == false
        if (scale < 0)
            throw Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Negative scale is not supported");
    }

    static DecimalType adjust_precision_scale(const Int32 precision, const Int32 scale)
    {
        check_negative_scale(scale);
        assert(precision >= scale);

        if (precision <= spark_max_precision)
        {
            // Adjustment only needed when we exceed max precision
            return DecimalType(precision, scale);
        }
        else if (scale < 0)
        {
            // Decimal can have negative scale (SPARK-24468). In this case, we cannot allow a precision
            // loss since we would cause a loss of digits in the integer part.
            // In this case, we are likely to meet an overflow.
            return DecimalType(spark_max_precision, scale);
        }
        else
        {
            // Precision/scale exceed maximum precision. Result must be adjusted to MAX_PRECISION.
            const int intDigits = precision - scale;

            // If original scale is less than MINIMUM_ADJUSTED_SCALE, use original scale value; otherwise
            // preserve at least MINIMUM_ADJUSTED_SCALE fractional digits
            const int minScaleValue = std::min(scale, minimum_adjusted_scale);

            // The resulting scale is the maximum between what is available without causing a loss of
            // digits for the integer part of the decimal and the minimum guaranteed scale, which is
            // computed above
            const int adjusted_scale = std::max(spark_max_precision - intDigits, minScaleValue);
            return DecimalType(spark_max_precision, adjusted_scale);
        }
    }

public:
    /// The formula follows Hive which is based on the SQL standard and MS SQL:
    /// https://cwiki.apache.org/confluence/download/attachments/27362075/Hive_Decimal_Precision_Scale_Support.pdf
    /// https://msdn.microsoft.com/en-us/library/ms190476.aspx
    /// Result Precision: max(s1, s2) + max(p1-s1, p2-s2) + 1
    /// Result Scale:     max(s1, s2)
    ///  +, -
    static DecimalType
    resultAddSubstractDecimalType(const Int32 p1, const Int32 s1, const Int32 p2, const Int32 s2, bool allowPrecisionLoss = true)
    {
        const Int32 scale = std::max(s1, s2);
        const Int32 precision = std::max(p1 - s1, p2 - s2) + scale + 1;

        if (allowPrecisionLoss)
            return adjust_precision_scale(precision, scale);
        else
            return bounded_to_spark(precision, scale);
    }

    /// The formula follows Hive which is based on the SQL standard and MS SQL:
    /// https://cwiki.apache.org/confluence/download/attachments/27362075/Hive_Decimal_Precision_Scale_Support.pdf
    /// https://msdn.microsoft.com/en-us/library/ms190476.aspx
    /// Result Precision: p1 - s1 + s2 + max(6, s1 + p2 + 1)
    /// Result Scale:     max(6, s1 + p2 + 1)
    static DecimalType
    resultDivideDecimalType(const Int32 p1, const Int32 s1, const Int32 p2, const Int32 s2, bool allowPrecisionLoss = true)
    {
        if (allowPrecisionLoss)
        {
            const Int32 Int32Dig = p1 - s1 + s2;
            const Int32 scale = std::max(minimum_adjusted_scale, s1 + p2 + 1);
            const Int32 prec = Int32Dig + scale;
            return adjust_precision_scale(prec, scale);
        }
        else
        {
            Int32 Int32Dig = std::min(spark_max_scale, p1 - s1 + s2);
            Int32 decDig = std::min(spark_max_scale, std::max(minimum_adjusted_scale, s1 + p2 + 1));
            Int32 diff = (Int32Dig + decDig) - spark_max_scale;

            if (diff > 0)
            {
                decDig -= diff / 2 + 1;
                Int32Dig = spark_max_scale - decDig;
            }

            return bounded_to_spark(Int32Dig + decDig, decDig);
        }
    }

    /// The formula follows Hive which is based on the SQL standard and MS SQL:
    /// https://cwiki.apache.org/confluence/download/attachments/27362075/Hive_Decimal_Precision_Scale_Support.pdf
    /// https://msdn.microsoft.com/en-us/library/ms190476.aspx
    /// Result Precision: p1 + p2 + 1
    /// Result Scale:     s1 + s2
    static DecimalType
    resultMultiplyDecimalType(const Int32 p1, const Int32 s1, const Int32 p2, const Int32 s2, bool allowPrecisionLoss = true)
    {
        const Int32 scale = s1 + s2;
        const Int32 precision = p1 + p2 + 1;

        if (allowPrecisionLoss)
            return adjust_precision_scale(precision, scale);
        else
            return bounded_to_spark(precision, scale);
    }

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
        ActionsDAGPtr & actions_dag,
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
            = &actions_dag->addColumn(ColumnWithTypeAndName(str_type->createColumnConst(1, type_name), str_type, getUniqueName(type_name)));
        cast_args.emplace_back(type_node);
        const ActionsDAG::Node * cast_node = toFunctionNode(actions_dag, "CAST", cast_args);
        actions_dag->addOrReplaceInOutputs(*cast_node);
        new_args.emplace_back(cast_node);
        new_args.emplace_back(args[1]);
        return new_args;
    }

    DecimalType getDecimalType(const DataTypePtr & left, const DataTypePtr & right, const bool resultType) const
    {
        assert(isDecimal(left) && isDecimal(right));
        const Int32 p1 = getDecimalPrecision(*left);
        const Int32 s1 = getDecimalScale(*left);
        const Int32 p2 = getDecimalPrecision(*right);
        const Int32 s2 = getDecimalScale(*right);
        return resultType ? internalResultType(p1, s1, p2, s2) : internalEvalType(p1, s1, p2, s2);
    }

    virtual DecimalType internalResultType(Int32 p1, Int32 s1, Int32 p2, Int32 s2) const = 0;
    virtual DecimalType internalEvalType(Int32 p1, Int32 s1, Int32 p2, Int32 s2) const = 0;

    const ActionsDAG::Node *
    checkDecimalOverflow(ActionsDAGPtr & actions_dag, const ActionsDAG::Node * func_node, Int32 precision, Int32 scale) const
    {
        const DB::ActionsDAG::NodeRawConstPtrs overflow_args
            = {func_node,
               plan_parser->addColumn(actions_dag, std::make_shared<DataTypeInt32>(), precision),
               plan_parser->addColumn(actions_dag, std::make_shared<DataTypeInt32>(), scale)};
        return toFunctionNode(actions_dag, "checkDecimalOverflowSparkOrNull", overflow_args);
    }
    const DB::ActionsDAG::Node * convertNodeTypeIfNeeded(
        const substrait::Expression_ScalarFunction & substrait_func,
        const DB::ActionsDAG::Node * func_node,
        DB::ActionsDAGPtr & actions_dag) const override
    {
        const auto & substrait_type = substrait_func.output_type();
        if (const auto result_type = TypeParser::parseType(substrait_type); isDecimalOrNullableDecimal(result_type))
        {
            const auto a = removeNullable(result_type);
            const auto b = removeNullable(func_node->result_type);
            if (a->equals(*b))
                return func_node;

            // as stated in isTypeMatched， currently we don't change nullability of the result type
            const std::string type_name = func_node->result_type->isNullable() ? wrapNullableType(true, result_type)->getName()
                                                                               : removeNullable(result_type)->getName();
            return ActionsDAGUtil::convertNodeType(actions_dag, func_node, type_name, func_node->result_name, DB::CastType::accurateOrNull);
        }
        return FunctionParser::convertNodeTypeIfNeeded(substrait_func, func_node, actions_dag);
    }

    virtual const DB::ActionsDAG::Node *
    createFunctionNode(DB::ActionsDAGPtr & actions_dag, const String & func_name, const DB::ActionsDAG::NodeRawConstPtrs & args) const
    {
        return toFunctionNode(actions_dag, func_name, args);
    }

public:
    explicit FunctionParserBinaryArithmetic(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) { }
    const ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func, ActionsDAGPtr & actions_dag) const override
    {
        const auto ch_func_name = getCHFunctionName(substrait_func);
        auto parsed_args = parseFunctionArguments(substrait_func, ch_func_name, actions_dag);

        if (parsed_args.size() != 2)
            throw Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly two arguments", getName());

        const auto left_type = DB::removeNullable(parsed_args[0]->result_type);
        const auto right_type = DB::removeNullable(parsed_args[1]->result_type);
        const bool converted = isDecimal(left_type) && isDecimal(right_type);

        if (converted)
        {
            const DecimalType evalType = getDecimalType(left_type, right_type, false);
            parsed_args = convertBinaryArithmeticFunDecimalArgs(actions_dag, parsed_args, evalType, substrait_func);
        }

        const auto * func_node = createFunctionNode(actions_dag, ch_func_name, parsed_args);

        if (converted)
        {
            const auto parsed_outputType = removeNullable(TypeParser::parseType(substrait_func.output_type()));
            assert(isDecimal(parsed_outputType));
            const Int32 parsed_precision = getDecimalPrecision(*parsed_outputType);
            const Int32 parsed_scale = getDecimalScale(*parsed_outputType);

#ifndef NDEBUG
            const auto [precision, scale] = getDecimalType(left_type, right_type, true);
            // assert(parsed_precision == precision);
            // assert(parsed_scale == scale);
#endif
            func_node = checkDecimalOverflow(actions_dag, func_node, parsed_precision, parsed_scale);
        }
        return convertNodeTypeIfNeeded(substrait_func, func_node, actions_dag);
    }
};

class FunctionParserPlus final : public FunctionParserBinaryArithmetic
{
public:
    explicit FunctionParserPlus(SerializedPlanParser * plan_parser_) : FunctionParserBinaryArithmetic(plan_parser_) { }

    static constexpr auto name = "add";
    String getName() const override { return name; }

protected:
    DecimalType internalResultType(const Int32 p1, const Int32 s1, const Int32 p2, const Int32 s2) const override
    {
        return DecimalType::resultAddSubstractDecimalType(p1, s1, p2, s2);
    }
    DecimalType internalEvalType(const Int32 p1, const Int32 s1, const Int32 p2, const Int32 s2) const override
    {
        return DecimalType::evalAddSubstractDecimalType(p1, s1, p2, s2);
    }
};

class FunctionParserMinus final : public FunctionParserBinaryArithmetic
{
public:
    explicit FunctionParserMinus(SerializedPlanParser * plan_parser_) : FunctionParserBinaryArithmetic(plan_parser_) { }

    static constexpr auto name = "subtract";
    String getName() const override { return name; }

protected:
    DecimalType internalResultType(const Int32 p1, const Int32 s1, const Int32 p2, const Int32 s2) const override
    {
        return DecimalType::resultAddSubstractDecimalType(p1, s1, p2, s2);
    }
    DecimalType internalEvalType(const Int32 p1, const Int32 s1, const Int32 p2, const Int32 s2) const override
    {
        return DecimalType::evalAddSubstractDecimalType(p1, s1, p2, s2);
    }
};

class FunctionParserMultiply final : public FunctionParserBinaryArithmetic
{
public:
    explicit FunctionParserMultiply(SerializedPlanParser * plan_parser_) : FunctionParserBinaryArithmetic(plan_parser_) { }
    static constexpr auto name = "multiply";
    String getName() const override { return name; }

protected:
    DecimalType internalResultType(const Int32 p1, const Int32 s1, const Int32 p2, const Int32 s2) const override
    {
        return DecimalType::resultMultiplyDecimalType(p1, s1, p2, s2);
    }
    DecimalType internalEvalType(const Int32 p1, const Int32 s1, const Int32 p2, const Int32 s2) const override
    {
        return DecimalType::evalMultiplyDecimalType(p1, s1, p2, s2);
    }
};

class FunctionParserDivide final : public FunctionParserBinaryArithmetic
{
public:
    explicit FunctionParserDivide(SerializedPlanParser * plan_parser_) : FunctionParserBinaryArithmetic(plan_parser_) { }
    static constexpr auto name = "divide";
    String getName() const override { return name; }

protected:
    DecimalType internalResultType(const Int32 p1, const Int32 s1, const Int32 p2, const Int32 s2) const override
    {
        return DecimalType::resultDivideDecimalType(p1, s1, p2, s2);
    }
    DecimalType internalEvalType(const Int32 p1, const Int32 s1, const Int32 p2, const Int32 s2) const override
    {
        return DecimalType::evalDividetDecimalType(p1, s1, p2, s2);
    }

    const DB::ActionsDAG::Node * createFunctionNode(
        DB::ActionsDAGPtr & actions_dag, const String & func_name, const DB::ActionsDAG::NodeRawConstPtrs & new_args) const override
    {
        assert(func_name == name);
        const auto * left_arg = new_args[0];
        const auto * right_arg = new_args[1];

        if (isDecimal(removeNullable(left_arg->result_type)) || isDecimal(removeNullable(right_arg->result_type)))
            return toFunctionNode(actions_dag, "sparkDivideDecimal", {left_arg, right_arg});
        else
            return toFunctionNode(actions_dag, "sparkDivide", {left_arg, right_arg});
    }
};

static FunctionParserRegister<FunctionParserPlus> register_plus;
static FunctionParserRegister<FunctionParserMinus> register_minus;
static FunctionParserRegister<FunctionParserMultiply> register_mltiply;
static FunctionParserRegister<FunctionParserDivide> register_divide;

}
