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
#include <Common/CHUtil.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>

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
class FunctionParserConcat : public FunctionParser
{
public:
    explicit FunctionParserConcat(ParserContextPtr ctx) : FunctionParser(std::move(ctx)) { }
    ~FunctionParserConcat() override = default;

    static constexpr auto name = "concat";

    String getName() const override { return name; }

    const ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        ActionsDAG & actions_dag) const override
    {
        /*
          parse concat(args) as:
            1. if input single argument is array, then return arrayConcat(args)
            2. otherwise return concat(args)
        */
        String ch_function_name = "concat";
        auto args = parseFunctionArguments(substrait_func, actions_dag);
        if (args.size() == 1 && isArray(removeNullable(args[0]->result_type)))
            ch_function_name = "arrayConcat";

        auto * result_node = toFunctionNode(actions_dag, "concat", args);
        return convertNodeTypeIfNeeded(substrait_func, result_node, actions_dag);
    }
};

static FunctionParserRegister<FunctionParserConcat> register_concat;
}
