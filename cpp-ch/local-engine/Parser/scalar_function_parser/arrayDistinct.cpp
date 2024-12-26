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
using namespace DB;
class FunctionParserArrayDistinct : public FunctionParser
{
public:
    explicit FunctionParserArrayDistinct(ParserContextPtr parser_context_) : FunctionParser(parser_context_) { }

    static constexpr auto name = "array_distinct";

    String getName() const override { return name; }

    const ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        ActionsDAG & actions_dag) const override
    {
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly one arguments", getName());

        const auto * array_distinct_node = toFunctionNode(actions_dag, "arrayDistinctSpark", {parsed_args[0]});
        /// We can not call convertNodeTypeIfNeeded which will fail when cast Array(Array(xx)) to Array(Nullable(Array(xx)))
        return array_distinct_node;
    }
};

static FunctionParserRegister<FunctionParserArrayDistinct> register_array_distinct;
}
