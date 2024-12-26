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
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

namespace local_engine
{

class FunctionParserConcatWS : public FunctionParser
{
public:
    explicit FunctionParserConcatWS(ParserContextPtr parser_context_) : FunctionParser(parser_context_) {}
    ~FunctionParserConcatWS() override = default;

    static constexpr auto name = "concat_ws";

    String getName() const override { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction &) const override { return name; }

    const DB::ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        DB::ActionsDAG & actions_dag) const override
    {
        /*
            parse concat_ws(sep, s1, s2, arr1, arr2, ...)) as
            arrayStringConcat(arrayFlatten(array(s1), array(s2), arr1, arr2, ...), sep)
        */
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.empty())
            throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires at least one argument", getName());

        const auto * first_arg = parsed_args[0];
        DB::ActionsDAG::NodeRawConstPtrs new_args;
        new_args.reserve(parsed_args.size() - 1);

        for (size_t i = 1; i < parsed_args.size(); ++i)
        {
            const auto * arg = parsed_args[i];
            if (isString(removeNullable(arg->result_type)))
            {
                const auto * array_node = toFunctionNode(actions_dag, "array", {arg});
                new_args.push_back(array_node);
            }
            else if (isArray(removeNullable(arg->result_type)))
            {
                const auto * array_not_null_node = toFunctionNode(actions_dag, "assumeNotNull", {arg});
                new_args.push_back(array_not_null_node);
            }
            else
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Function {} requires string or array of string arguments", getName());
        }

        if (new_args.empty())
        {
            const auto * null_const_node = addColumnToActionsDAG(actions_dag, makeNullable(std::make_shared<DB::DataTypeString>()), DB::Field());
            const auto * array_node = toFunctionNode(actions_dag, "array", {null_const_node});
            new_args.push_back(array_node);
        }

        const auto * array_node = toFunctionNode(actions_dag, "array", new_args);
        const auto * array_flatten_node = toFunctionNode(actions_dag, "arrayFlatten", {array_node});

        const auto * result_node = toFunctionNode(actions_dag, "arrayStringConcat", {array_flatten_node, first_arg});
        return result_node;
    }
};

static FunctionParserRegister<FunctionParserConcatWS> register_concat_ws;
}
