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
}
}

namespace local_engine
{
using namespace DB;
class FunctionParserMyMd5 : public FunctionParser
{
public:
    explicit FunctionParserMyMd5(ParserContextPtr ctx) : FunctionParser(ctx) { }

    static constexpr auto name = "my_md5";

    String getName() const override { return name; }

    const ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        ActionsDAG & actions_dag) const override
    {
        // In Spark: md5(str)
        // In CH: lower(hex(MD5(str)))
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} requires exactly one arguments", getName());

        const auto * str_arg = parsed_args[0];
        const auto * md5_node = toFunctionNode(actions_dag, "MD5", {str_arg});
        const auto * hex_node = toFunctionNode(actions_dag, "hex", {md5_node});
        const auto * lower_node = toFunctionNode(actions_dag, "lower", {hex_node});
        return convertNodeTypeIfNeeded(substrait_func, lower_node, actions_dag);
    }
};

static FunctionParserRegister<FunctionParserMyMd5> register_my_md5;
}
