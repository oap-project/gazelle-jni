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
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Parser/FunctionParser.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

namespace local_engine
{

class FunctionParserGetArrayItem : public FunctionParser
{
public:
    explicit FunctionParserGetArrayItem(ParserContextPtr parser_context_) : FunctionParser(parser_context_) { }
    ~FunctionParserGetArrayItem() override = default;

    static constexpr auto name = "get_array_item";
    String getName() const override { return name; }

    const DB::ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override
    {
        /**
            parse get_array_item(arr, idx) as
            if (idx <= 0)
                null
            else
                arrayElementOrNull(arr, idx)
        */
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() != 2)
            throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly two arguments", getName());

        const auto * arr_arg = parsed_args[0];
        const auto * idx_arg = parsed_args[1];
        const DB::DataTypeArray * arr_type = checkAndGetDataType<DB::DataTypeArray>(removeNullable(arr_arg->result_type).get());
        if (!arr_type)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "First argument of function {} must be an array", getName());

        /// arrayElementOrNull(arrary, idx)
        const auto * array_element_node = toFunctionNode(actions_dag, "arrayElementOrNull", {arr_arg, idx_arg});

        /// idx <= 0
        const auto * zero_node = addColumnToActionsDAG(actions_dag, std::make_shared<DB::DataTypeInt32>(), 0);
        const auto * idx_le_zero_node = toFunctionNode(actions_dag, "lessOrEquals", {idx_arg, zero_node});

        const auto * null_node = addColumnToActionsDAG(actions_dag, makeNullable(arr_type->getNestedType()), DB::Field{});
        const auto * if_node = toFunctionNode(actions_dag, "if", {idx_le_zero_node, null_node, array_element_node});
        return convertNodeTypeIfNeeded(substrait_func, if_node, actions_dag);
    }
};

static FunctionParserRegister<FunctionParserGetArrayItem> register_get_array_item;

}
