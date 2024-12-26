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

class FunctionParserEmpty2Null : public FunctionParser
{
public:
    explicit FunctionParserEmpty2Null(ParserContextPtr parser_context_) : FunctionParser(parser_context_) { }

    static constexpr auto name = "empty2null";

    String getName() const override { return name; }

    const DB::ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override
    {
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() != 1)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Function {} requires exactly one arguments", getName());
        if (parsed_args.at(0)->result_type->getName() != "Nullable(String)")
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Function {}'s argument has to be Nullable(String)", getName());

        const auto * null_node = addColumnToActionsDAG(actions_dag, makeNullableSafe(std::make_shared<DB::DataTypeString>()), DB::Field());

        auto cond = toFunctionNode(actions_dag, "empty", {parsed_args.at(0)});
        auto * if_function = toFunctionNode(actions_dag, "if", {cond, null_node, parsed_args.at(0)});

        return convertNodeTypeIfNeeded(substrait_func, if_function, actions_dag);
    }
};

static FunctionParserRegister<FunctionParserEmpty2Null> register_emtpy2null;
}
