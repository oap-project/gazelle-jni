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
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>
#include <Parser/FunctionParser.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

namespace local_engine
{
using namespace DB;
class BaseFunctionParserArrayMaxAndMin : public FunctionParser
{
public:
    explicit BaseFunctionParserArrayMaxAndMin(ParserContextPtr parser_context_) : FunctionParser(parser_context_) { }
    ~BaseFunctionParserArrayMaxAndMin() override = default;

    const ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        ActionsDAG & actions_dag) const override
    {
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} requires exactly one arguments", getName());

        const auto * arr_arg = parsed_args[0];
        const auto * func_const_node = addColumnToActionsDAG(actions_dag, std::make_shared<DataTypeString>(), getCHFunctionName(substrait_func));

        auto is_arr_nullable = arr_arg->result_type->isNullable();
        if (!is_arr_nullable)
        {
            const auto * array_reduce_node = toFunctionNode(actions_dag, "arrayReduce", {func_const_node, arr_arg});
            return convertNodeTypeIfNeeded(substrait_func, array_reduce_node, actions_dag);
        }

        const auto * arr_is_null_node = toFunctionNode(actions_dag, "isNull", {arr_arg});
        const auto * arr_not_null_node = toFunctionNode(actions_dag, "assumeNotNull", {arr_arg});
        const auto * array_reduce_node = toFunctionNode(actions_dag, "arrayReduce", {func_const_node, arr_not_null_node});
        const auto * null_const_node = addColumnToActionsDAG(actions_dag, makeNullable(array_reduce_node->result_type), Field{});
        const auto * if_node = toFunctionNode(actions_dag, "if", {arr_is_null_node, null_const_node, array_reduce_node});
        return convertNodeTypeIfNeeded(substrait_func, if_node, actions_dag);
    }
};

class FunctionParserArrayMax : public BaseFunctionParserArrayMaxAndMin
{
public:
    explicit FunctionParserArrayMax(ParserContextPtr parser_context_) : BaseFunctionParserArrayMaxAndMin(parser_context_) { }
    ~FunctionParserArrayMax() override = default;

    static constexpr auto name = "array_max";
    String getName() const override { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction &) const override { return "max"; }
};
static FunctionParserRegister<FunctionParserArrayMax> register_array_max;


class FunctionParserArrayMin : public BaseFunctionParserArrayMaxAndMin
{
public:
    explicit FunctionParserArrayMin(ParserContextPtr parser_context_) : BaseFunctionParserArrayMaxAndMin(parser_context_) { }
    ~FunctionParserArrayMin() override = default;

    static constexpr auto name = "array_min";
    String getName() const override { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction &) const override { return "min"; }
};
static FunctionParserRegister<FunctionParserArrayMin> register_array_min;

}
