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

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeNullable.h>
#include <Parser/FunctionParser.h>
#include <Parser/TypeParser.h>
#include <Parser/scalar_function_parser/lambdaFunction.h>
#include <Common/BlockTypeUtils.h>
#include <Common/CHUtil.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB::ErrorCodes
{
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

namespace local_engine
{

template <bool transform_keys = true>
class FunctionParserMapTransformImpl : public FunctionParser
{
public:
    static constexpr auto name = transform_keys ? "transform_keys" : "transform_values";
    String getName() const override { return name; }

    explicit FunctionParserMapTransformImpl(ParserContextPtr parser_context_) : FunctionParser(parser_context_) {}
    ~FunctionParserMapTransformImpl() override = default;

    const DB::ActionsDAG::Node *
    parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override
    {
        /// parse transfrom_keys(expr, func) or transform_values(expr, func) as mapApply(func, expr)
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() != 2)
            throw DB::Exception(DB::ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "{} function must have three arguments", getName());

        auto lambda_args = collectLambdaArguments(parser_context, substrait_func.arguments()[1].value().scalar_function());
        if (lambda_args.size() != 2)
            throw DB::Exception(
                DB::ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "The lambda function in {} must have two arguments", getName());

        const auto * result_node = toFunctionNode(actions_dag, "mapApply", {parsed_args[1], parsed_args[0]});
        return convertNodeTypeIfNeeded(substrait_func, result_node, actions_dag);
    }
};

using FunctionParserTransformKeys = FunctionParserMapTransformImpl<true>;
using FunctionParserTransformValues = FunctionParserMapTransformImpl<false>;

static FunctionParserRegister<FunctionParserTransformKeys> register_transform_keys;
static FunctionParserRegister<FunctionParserTransformValues> register_transform_values;
}