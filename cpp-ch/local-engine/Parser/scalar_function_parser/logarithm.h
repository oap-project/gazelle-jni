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
#pragma once

#include <Core/Field.h>
#include <DataTypes/DataTypeNullable.h>
#include <Parser/FunctionParser.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int NOT_IMPLEMENTED;
}
}

namespace local_engine
{
class FunctionParserLogBase : public FunctionParser
{
public:
    explicit FunctionParserLogBase(ParserContextPtr parser_context_) : FunctionParser(parser_context_) {}
    ~FunctionParserLogBase() override = default;

    virtual String getCHFunctionName() const = 0;
    virtual const DB::ActionsDAG::Node * getParameterLowerBound(DB::ActionsDAG &, const DB::DataTypePtr &) const { return nullptr; }

    const DB::ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        DB::ActionsDAG & actions_dag) const override
    {
        /*
            parse log(x) as
            if (x <= c)
                null
            else
                log(x)
        */
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() != 1)
            throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly one arguments", getName());

        const auto * arg_node = parsed_args[0];
        
        const std::string ch_function_name = getCHFunctionName();
        const auto * log_node = toFunctionNode(actions_dag, ch_function_name, {arg_node});
        auto nullable_result_type = makeNullable(log_node->result_type);

        const auto * null_const_node = addColumnToActionsDAG(actions_dag, nullable_result_type, DB::Field());
        const auto * lower_bound_node = getParameterLowerBound(actions_dag, arg_node->result_type);
        if (!lower_bound_node)
            throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Vritual function {} may not implement for {}", "getParameterLowerBound", getName());
        
        const auto * le_node = toFunctionNode(actions_dag, "lessOrEquals", {arg_node, lower_bound_node});
        const auto * result_node = toFunctionNode(actions_dag, "if", {le_node, null_const_node, log_node});

        return convertNodeTypeIfNeeded(substrait_func, result_node, actions_dag);
    }
};

}
