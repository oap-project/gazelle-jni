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

#include <DataTypes/DataTypeNullable.h>
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
class FunctionParserShiftRightUnsigned : public FunctionParser
{
public:
    explicit FunctionParserShiftRightUnsigned(ParserContextPtr parser_context_) : FunctionParser(parser_context_) { }
    ~FunctionParserShiftRightUnsigned() override = default;

    static constexpr auto name = "shiftrightunsigned";

    String getName() const override { return name; }

    const DB::ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override
    {
        /// parse shiftrightunsigned(a, b) as
        /// if (isInteger(a))
        ///   bitShiftRight(a::UInt32, pmod(b, 32))
        /// else if (isLong(a))
        ///   bitShiftRight(a::UInt64, pmod(b, 32))
        /// else
        ///   throw Exception

        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() != 2)
            throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly two arguments", getName());

        const auto * a = parsed_args[0];
        const auto * b = parsed_args[1];

        DB::WhichDataType which(removeNullable(a->result_type));
        const DB::ActionsDAG::Node * base_node = nullptr;
        const DB::ActionsDAG::Node * unsigned_a_node = nullptr;
        if (which.isInt32())
        {
            base_node = addColumnToActionsDAG(actions_dag, std::make_shared<DB::DataTypeUInt32>(), 32);
            const auto * uint32_type_node = addColumnToActionsDAG(actions_dag, std::make_shared<DB::DataTypeString>(), "Nullable(UInt32)");
            unsigned_a_node = toFunctionNode(actions_dag, "CAST", {a, uint32_type_node});
        }
        else if (which.isInt64())
        {
            base_node = addColumnToActionsDAG(actions_dag, std::make_shared<DB::DataTypeUInt32>(), 64);
            const auto * uint64_type_node = addColumnToActionsDAG(actions_dag, std::make_shared<DB::DataTypeString>(), "Nullable(UInt64)");
            unsigned_a_node = toFunctionNode(actions_dag, "CAST", {a, uint64_type_node});
        }
        else
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Function {} requires integer or long as first argument", getName());

        const auto * pmod_node = toFunctionNode(actions_dag, "pmod", {b, base_node});
        const auto * result = toFunctionNode(actions_dag, "bitShiftRight", {unsigned_a_node, pmod_node});
        return convertNodeTypeIfNeeded(substrait_func, result, actions_dag);
    }
};

static FunctionParserRegister<FunctionParserShiftRightUnsigned> register_shift_right_unsigned;
}
