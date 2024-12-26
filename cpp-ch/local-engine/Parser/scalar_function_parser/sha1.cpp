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

class FunctionParserSha1 : public FunctionParser
{
public:
    explicit FunctionParserSha1(ParserContextPtr parser_context_) : FunctionParser(parser_context_) { }
    ~FunctionParserSha1() override = default;

    static constexpr auto name = "sha1";

    String getName() const override { return name; }

    const DB::ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override
    {
        /// Parse sha1(str) as lower(hex(sha1(str)))
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() != 1)
            throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly one arguments", getName());

        const auto * str_arg = parsed_args[0];
        const auto * md5_node = toFunctionNode(actions_dag, "SHA1", {str_arg});
        const auto * hex_node = toFunctionNode(actions_dag, "hex", {md5_node});
        const auto * lower_node = toFunctionNode(actions_dag, "lower", {hex_node});
        return convertNodeTypeIfNeeded(substrait_func, lower_node, actions_dag);
    }
};

static FunctionParserRegister<FunctionParserSha1> register_sha1;
}
