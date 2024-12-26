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

#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionHelpers.h>
#include <Parser/FunctionParser.h>

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
class FunctionParserElementAt : public FunctionParser
{
public:
    explicit FunctionParserElementAt(ParserContextPtr parser_context_) : FunctionParser(parser_context_) { }
    ~FunctionParserElementAt() override = default;

    static constexpr auto name = "element_at";
    String getName() const override { return name; }

    const DB::ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override
    {
        /*
            parse element_at(arr, idx) as
            if (idx == 0)
                throwIf(idx == 0)
            else
                arrayElementOrNull(arr, idx)

            parse element_at(map, key) as arrayElementOrNull(map, key)
        */
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() != 2)
            throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly two arguments", getName());

        auto arg_type = removeNullable(parsed_args[0]->result_type);
        if (isMap(arg_type))
        {
            const auto * map_arg = parsed_args[0];
            const DB::DataTypeMap * map_type = checkAndGetDataType<DB::DataTypeMap>(removeNullable(map_arg->result_type).get());
            if (isNothing(map_type->getKeyType()))
            {
                const DB::DataTypePtr value_type = map_type->getValueType();
                const auto * null_const_node = addColumnToActionsDAG(actions_dag, makeNullable(value_type), DB::Field{});
                return null_const_node;
            }
            const auto * key_arg = parsed_args[1];
            const auto * result_node = toFunctionNode(actions_dag, "arrayElementOrNull", {map_arg, key_arg});
            return convertNodeTypeIfNeeded(substrait_func, result_node, actions_dag);
        }
        else if (isArray(arg_type))
        {
            const auto * arr_arg = parsed_args[0];
            const auto * idx_arg = parsed_args[1];

            const auto * zero_node = addColumnToActionsDAG(actions_dag, std::make_shared<DB::DataTypeInt32>(), 0);
            const auto * equal_zero_node = toFunctionNode(actions_dag, "equals", {idx_arg, zero_node});
            const auto * throw_message
                = addColumnToActionsDAG(actions_dag, std::make_shared<DB::DataTypeString>(), "SQL array indices start at 1");
            /// throwIf(idx == 0, 'SQL array indices start at 1')
            const auto * throw_if_node = toFunctionNode(actions_dag, "throwIf", {equal_zero_node, throw_message});

            const auto * array_element_node = toFunctionNode(actions_dag, "arrayElementOrNull", {arr_arg, idx_arg});
            /// if(throwIf(idx == 0, 'SQL array indices start at 1'), arrayElementOrNull(arr, idx), arrayElementOrNull(arr, idx))
            const auto * if_node = toFunctionNode(actions_dag, "if", {throw_if_node, array_element_node, array_element_node});
            return convertNodeTypeIfNeeded(substrait_func, if_node, actions_dag);
        }
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "First argument of function {} must be an array or a map", getName());
    }
};

static FunctionParserRegister<FunctionParserElementAt> register_element_at;
}
