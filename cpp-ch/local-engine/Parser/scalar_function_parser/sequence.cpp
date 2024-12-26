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
#include <DataTypes/DataTypesNumber.h>
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

class FunctionParserSequence : public FunctionParser
{
public:
    explicit FunctionParserSequence(ParserContextPtr parser_context_) : FunctionParser(parser_context_) { }
    ~FunctionParserSequence() override = default;

    static constexpr auto name = "sequence";

    String getName() const override { return name; }

    const DB::ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        DB::ActionsDAG & actions_dag) const override
    {
        /**
            parse sequence(start, end, step) as
            if (isNull(start))
                null
            else if (isNull(end))
                null
            else if (isNull(step))
                null
            else if ((end - start) % step = 0)
                range(start, end + step, step)
            else
                range(start, end, step)

            note: default step is 1 if start <= end, otherwise -1
            step = if(start <= end, 1, -1)
        */

        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() < 2 || parsed_args.size() > 3)
            throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires two or three arguments", getName());

        const auto * start_arg = parsed_args[0];
        const auto * end_arg = parsed_args[1];
        const auto * step_arg = parsed_args.size() == 3 ? parsed_args[2] : nullptr;

        const auto * one_const_node = addColumnToActionsDAG(actions_dag, std::make_shared<DB::DataTypeInt32>(), 1);
        if (!step_arg)
        {
            const auto * minus_one_const_node = addColumnToActionsDAG(actions_dag, std::make_shared<DB::DataTypeInt32>(), -1);
            /// start <= end
            const auto * start_le_end_node = toFunctionNode(actions_dag, "lessOrEquals", {start_arg, end_arg});
            /// if(start <= end, 1, -1)
            step_arg = toFunctionNode(actions_dag, "if", {start_le_end_node, one_const_node, minus_one_const_node});
        }

        const auto * start_is_null_node = toFunctionNode(actions_dag, "isNull", {start_arg});
        const auto * end_is_null_node = toFunctionNode(actions_dag, "isNull", {end_arg});
        const auto * step_is_null_node = toFunctionNode(actions_dag, "isNull", {step_arg});

        const auto * end_minus_start_node = toFunctionNode(actions_dag, "minus", {end_arg, start_arg});
        const auto * modulo_step_node = toFunctionNode(actions_dag, "modulo", {end_minus_start_node, step_arg});
        const auto * zero_const_node = addColumnToActionsDAG(actions_dag, std::make_shared<DB::DataTypeInt32>(), 0);
        /// (end - start) % step = 0
        const auto * modulo_step_eq_zero_node = toFunctionNode(actions_dag, "equals", {modulo_step_node, zero_const_node});

        const auto * start_not_null_node = toFunctionNode(actions_dag, "assumeNotNull", {start_arg});
        const auto * end_not_null_node = toFunctionNode(actions_dag, "assumeNotNull", {end_arg});
        const auto * step_not_null_node = toFunctionNode(actions_dag, "assumeNotNull", {step_arg});
        const auto * end_plus_step_node = toFunctionNode(actions_dag, "plus", {end_not_null_node, step_not_null_node});
        /// range(assumeNotNull(start), assumeNotNull(end) + assumeNotNull(step), assumeNotNull(step))

        // tricky: if step is null, range(, , assumeNotNull(step)) will throw exception: the 3rd argument step can't be less or equal to zero
        // so wrap it to if(isNull(step), 1, assumeNotNull(step)), which has no effect on the result
        const auto * tricky_step_node = toFunctionNode(actions_dag, "if", {step_is_null_node, one_const_node, step_not_null_node});

        const auto * range_1_node = toFunctionNode(actions_dag, "range", {start_not_null_node, end_plus_step_node, tricky_step_node});
        /// range(assumeNotNull(start), assumeNotNull(end), assumeNotNull(step))
        const auto * range_2_node = toFunctionNode(actions_dag, "range", {start_not_null_node, end_not_null_node, tricky_step_node});

        DB::DataTypePtr result_type = makeNullable(range_1_node->result_type);
        const auto * null_const_node = addColumnToActionsDAG(actions_dag, result_type, {});
        const auto * or_condition_node = toFunctionNode(actions_dag, "or", {start_is_null_node, end_is_null_node, step_is_null_node});

        const auto * result_node = toFunctionNode(actions_dag, "multiIf", {
            or_condition_node,
            null_const_node,
            modulo_step_eq_zero_node,
            range_1_node,
            range_2_node
        });
        return convertNodeTypeIfNeeded(substrait_func, result_node, actions_dag);
    }
};

static FunctionParserRegister<FunctionParserSequence> register_sequence;
}
