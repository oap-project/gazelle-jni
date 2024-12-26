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
#include <DataTypes/DataTypesNumber.h>
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
class FunctionParserSubstring : public FunctionParser
{
public:
    explicit FunctionParserSubstring(ParserContextPtr parser_context_) : FunctionParser(parser_context_) { }
    ~FunctionParserSubstring() override = default;
    static constexpr auto name = "substring";
    String getName() const override { return name; }

    const DB::ActionsDAG::Node * parse(
    const substrait::Expression_ScalarFunction & substrait_func,
    DB::ActionsDAG & actions_dag) const override
    {
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() != 3)
            throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires two or three arguments", getName());

        /*
            parse substring(str, index, length) as
                substring(str, if(index = 0, 1, index), length)
        */
        const auto * str_arg = parsed_args[0];
        const auto * index_arg = parsed_args[1];
        const auto * length_arg = parsed_args[2];

        auto index_type = std::make_shared<DB::DataTypeInt32>();
        const auto * const_zero_node = addColumnToActionsDAG(actions_dag, index_type, 0);
        const auto * const_one_node = addColumnToActionsDAG(actions_dag, index_type, 1);
        const auto * equals_zero_node = toFunctionNode(actions_dag, "equals", {index_arg, const_zero_node});
        const auto * if_node = toFunctionNode(actions_dag, "if", {equals_zero_node, const_one_node, index_arg});
        const auto * less_zero_node = toFunctionNode(actions_dag, "less", {length_arg, const_zero_node});
        const auto * if_len_node = toFunctionNode(actions_dag, "if", {less_zero_node, const_zero_node, length_arg});
        const auto * substring_func_node = toFunctionNode(actions_dag, "substringUTF8", {str_arg, if_node, if_len_node});
        return convertNodeTypeIfNeeded(substrait_func, substring_func_node, actions_dag);
    }

    String getCHFunctionName(const substrait::Expression_ScalarFunction & /*substrait_func*/) const override
    {
        return "substringUTF8";
    }
};

static FunctionParserRegister<FunctionParserSubstring> register_substring;
}
