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
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

namespace local_engine
{
using namespace DB;
class FunctionParserArrayContains : public FunctionParser
{
public:
    explicit FunctionParserArrayContains(ParserContextPtr parser_context_) : FunctionParser(parser_context_) { }
    ~FunctionParserArrayContains() override = default;

    static constexpr auto name = "array_contains";

    String getName() const override { return name; }

    const ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        ActionsDAG & actions_dag) const override
    {
        /**
            parse array_contains(arr, value) as
            if (isNull(arr))
                null
            else if (isNull(value))
                null
            else if (has(assumeNotNull(arr), value))
                true
            else if (has(assumeNotNull(arr), null))
                null
            else
                false

            result nullable:
                arr.nullable || value.nullable || arr.dataType.asInstanceOf[ArrayType].containsNull
        */

        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() != 2)
            throw Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly two arguments", getName());

        auto ch_function_name = getCHFunctionName(substrait_func);

        const auto * arr_arg = parsed_args[0];
        const auto * val_arg = parsed_args[1];

        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(removeNullable(arr_arg->result_type).get());
        if (!array_type)
            throw Exception(DB::ErrorCodes::BAD_ARGUMENTS, "First argument for function {} must be an array", getName());

        // has(assertNotNull(arr), value)
        const auto * arr_not_null_node = toFunctionNode(actions_dag, "assumeNotNull", {arr_arg});
        const auto * has_arr_value_node = toFunctionNode(actions_dag, ch_function_name, {arr_not_null_node, val_arg});

        // has(assertNotNull(arr), null)
        const auto * arr_elem_null_const_node = addColumnToActionsDAG(actions_dag, makeNullable(array_type->getNestedType()), Field{});
        const auto * has_arr_null_node = toFunctionNode(actions_dag, ch_function_name, {arr_not_null_node, arr_elem_null_const_node});

        // should return nullable result
        const auto * arr_is_null_node = toFunctionNode(actions_dag, "isNull", {arr_arg});
        const auto * val_is_null_node = toFunctionNode(actions_dag, "isNull", {val_arg});

        auto result_type = makeNullable(std::make_shared<DataTypeUInt8>());
        const auto * null_const_node = addColumnToActionsDAG(actions_dag, result_type, Field{});
        const auto * true_node = addColumnToActionsDAG(actions_dag, result_type, 1);
        const auto * false_node = addColumnToActionsDAG(actions_dag, result_type, 0);

        const auto * multi_if_node = toFunctionNode(actions_dag, "multiIf", {
            arr_is_null_node,
            null_const_node,
            val_is_null_node,
            null_const_node,
            has_arr_value_node,
            true_node,
            has_arr_null_node,
            null_const_node,
            false_node
        });
        return convertNodeTypeIfNeeded(substrait_func, multi_if_node, actions_dag);
    }

    String getCHFunctionName(const substrait::Expression_ScalarFunction & /*substrait_func*/) const override
    {
        return "has";
    }
};

static FunctionParserRegister<FunctionParserArrayContains> register_array_contains;
}
