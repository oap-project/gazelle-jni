
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
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Parser/AggregateFunctionParser.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}
}

namespace local_engine
{

using namespace DB;

struct ApproxCountDistinctNameStruct
{
    static constexpr auto spark_name = "approx_count_distinct";
    static constexpr auto ch_name = "uniqHLLPP";
};

/// Spark approx_count_distinct(expr, relative_sd) = CH uniqHLLPP(relative_sd)(if(isNull(expr), null, sparkXxHash64(expr)))
/// Spark approx_count_distinct(expr)              = CH uniqHLLPP(0.05)(if(isNull(expr), null, sparkXxHash64(expr)))
template <typename NameStruct>
class AggregateFunctionParserApproxCountDistinct final : public AggregateFunctionParser
{
public:
    static constexpr auto name = NameStruct::spark_name;

    AggregateFunctionParserApproxCountDistinct(ParserContextPtr parser_context_) : AggregateFunctionParser(parser_context_) { }
    ~AggregateFunctionParserApproxCountDistinct() override = default;

    String getName() const override { return NameStruct::spark_name; }

    String getCHFunctionName(const CommonFunctionInfo &) const override { return NameStruct::ch_name; }

    String getCHFunctionName(DataTypes & types) const override
    {
        /// Always invoked during second stage, the first argument is expr, the second argument is relative_sd.
        /// 1. Remove the second argument because types are used to create the aggregate function.
        /// 2. Replace the first argument type with UInt64 or Nullable(UInt64) because uniqHLLPP requres it.
        types.resize(1);
        const auto old_type = types[0];
        types[0] = std::make_shared<DataTypeUInt64>();
        if (old_type->isNullable())
            types[0] = std::make_shared<DataTypeNullable>(types[0]);

        return NameStruct::ch_name;
    }

    Array parseFunctionParameters(
        const CommonFunctionInfo & func_info, ActionsDAG::NodeRawConstPtrs & arg_nodes, ActionsDAG & actions_dag) const override
    {
        if (func_info.phase == substrait::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE
            || func_info.phase == substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT
            || func_info.phase == substrait::AGGREGATION_PHASE_UNSPECIFIED)
        {
            const auto & arguments = func_info.arguments;
            const size_t num_args = arguments.size();
            const size_t num_nodes = arg_nodes.size();
            if (num_args != num_nodes || num_args > 2 || num_args < 1 || num_nodes > 2 || num_nodes < 1)
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Function {} takes 1 or 2 arguments in phase {}",
                    getName(),
                    magic_enum::enum_name(func_info.phase));

            Array params;
            if (num_args == 2)
            {
                const auto & relative_sd_arg = arguments[1].value();
                if (relative_sd_arg.has_literal())
                {
                    auto [_, field] = parseLiteral(relative_sd_arg.literal());
                    params.push_back(std::move(field));
                }
                else
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Second argument of function {} must be literal", getName());
            }
            else
            {
                params.push_back(0.05);
            }

            const auto & expr_arg = arg_nodes[0];
            const auto * is_null_node = toFunctionNode(actions_dag, "isNull", {expr_arg});
            const auto * hash_node = toFunctionNode(actions_dag, "sparkXxHash64", {expr_arg});
            const auto * null_node
                = addColumnToActionsDAG(actions_dag, std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), {});
            const auto * if_node = toFunctionNode(actions_dag, "if", {is_null_node, null_node, hash_node});
            /// Replace the first argument expr with if(isNull(expr), null, sparkXxHash64(expr))
            arg_nodes[0] = if_node;
            arg_nodes.resize(1);

            return params;
        }
        else
        {
            if (arg_nodes.size() != 1)
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Function {} takes 1 argument in phase {}",
                    getName(),
                    magic_enum::enum_name(func_info.phase));

            const auto & result_type = arg_nodes[0]->result_type;
            const auto * aggregate_function_type = checkAndGetDataType<DataTypeAggregateFunction>(result_type.get());
            if (!aggregate_function_type)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "The first argument type of function {} in phase {} must be AggregateFunction, but is {}",
                    getName(),
                    magic_enum::enum_name(func_info.phase),
                    result_type->getName());

            return aggregate_function_type->getParameters();
        }
    }

    Array getDefaultFunctionParameters() const override { return {0.05}; }
};

static const AggregateFunctionParserRegister<AggregateFunctionParserApproxCountDistinct<ApproxCountDistinctNameStruct>> registerer_approx_count_distinct;
}
