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

#include <DataTypes/DataTypesNumber.h>
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

class FunctionParserSize : public FunctionParser
{
public:
    explicit FunctionParserSize(ParserContextPtr parser_context_) : FunctionParser(parser_context_) { }
    ~FunctionParserSize() override = default;

    static constexpr auto name = "size";

    String getName() const override { return name; }

    const DB::ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override
    {
        /// Parse size(child, true) as ifNull(length(child), -1)
        /// Parse size(child, false) as length(child)
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() != 2)
            throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly two arguments", getName());

        const auto * arg = parsed_args[0];
        const auto * legacy_arg = parsed_args[1];
        if (legacy_arg->type != DB::ActionsDAG::ActionType::COLUMN || !isColumnConst(*legacy_arg->column))
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Second argument of function {} must be a constant boolean", getName());

        const auto * length_node = toFunctionNode(actions_dag, "length", {arg});
        const auto * result_node = length_node;
        bool legacy = legacy_arg->column->getBool(0);
        if (legacy)
        {
            /// To avoid Exceptions like below, we played a trick here
            /// "There is no supertype for types Int32, UInt64 because some of them are signed integers and some are unsigned integers, but there is no signed integer type, that can exactly represent all required unsigned integer values"
            const auto * default_size_node
                = addColumnToActionsDAG(actions_dag, std::make_shared<DB::DataTypeUInt64>(), static_cast<UInt64>(-1));
            const auto * if_null_node = toFunctionNode(actions_dag, "ifNull", {length_node, default_size_node});
            result_node = if_null_node;
        }
        return convertNodeTypeIfNeeded(substrait_func, result_node, actions_dag);
    }
};

static FunctionParserRegister<FunctionParserSize> register_size;
}
