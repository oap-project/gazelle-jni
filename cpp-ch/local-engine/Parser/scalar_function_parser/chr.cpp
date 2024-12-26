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
#include <DataTypes/DataTypeString.h>
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
class FunctionParserChr : public FunctionParser
{
public:
    explicit FunctionParserChr(ParserContextPtr parser_context_) : FunctionParser(parser_context_) { }
    ~FunctionParserChr() override = default;
    static constexpr auto name = "chr";
    String getName() const override { return name; }

    const DB::ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        DB::ActionsDAG & actions_dag) const override
    {
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() != 1)
            throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires two or three arguments", getName());

        /*
            parse chr(number) as if(number < 0, '', convertCharset(char(0, number), 'unicode', 'utf-8'))
        */
        const auto & num_arg = parsed_args[0];
        const auto * const_zero_node = addColumnToActionsDAG(actions_dag, std::make_shared<DB::DataTypeInt32>(), 0);
        const auto * const_empty_node = addColumnToActionsDAG(actions_dag, std::make_shared<DB::DataTypeString>(), "");
        const auto * const_four_node = addColumnToActionsDAG(actions_dag, std::make_shared<DB::DataTypeInt32>(), 4);
        const auto * const_unicode_node = addColumnToActionsDAG(actions_dag, std::make_shared<DB::DataTypeString>(), "unicode");
        const auto * const_utf8_node = addColumnToActionsDAG(actions_dag, std::make_shared<DB::DataTypeString>(), "utf-8");

        const auto * less_node = toFunctionNode(actions_dag, "less", {num_arg, const_zero_node});

        const auto * char_node = toFunctionNode(actions_dag, "char", {const_zero_node, num_arg});
        const auto * convert_charset_node = toFunctionNode(actions_dag, "convertCharset", {char_node, const_unicode_node, const_utf8_node});

        const auto * if_node = toFunctionNode(actions_dag, "if", {less_node, const_empty_node, convert_charset_node});
        const auto * result_node = convertNodeTypeIfNeeded(substrait_func, if_node, actions_dag);
        return result_node;
    }
};

static FunctionParserRegister<FunctionParserChr> register_chr;
}
