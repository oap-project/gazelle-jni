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

class FunctionParserSha2 : public FunctionParser
{
public:
    explicit FunctionParserSha2(ParserContextPtr parser_context_) : FunctionParser(parser_context_) { }
    ~FunctionParserSha2() override = default;

    static constexpr auto name = "sha2";

    String getName() const override { return name; }

    const DB::ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override
    {
        /// Parse sha2(str, 0) or sha2(str, 0) as lower(hex(SHA256(str)))
        /// Parse sha2(str, 224) as lower(hex(SHA224(str)))
        /// Parse sha2(str, 384) as lower(hex(SHA384(str)))
        /// Parse sha2(str, 512) as lower(hex(SHA512(str)))
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() != 2)
            throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly two arguments", getName());

        const auto * str_arg = parsed_args[0];
        const auto * bit_length_arg = parsed_args[1];
        if (bit_length_arg->type != DB::ActionsDAG::ActionType::COLUMN || !isColumnConst(*bit_length_arg->column))
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Second argument of function {} must be a constant integer", getName());

        Int32 bit_length = bit_length_arg->column->getInt(0);
        String ch_func_name;
        switch (bit_length)
        {
            case 0:
            case 256:
                ch_func_name = "SHA256";
                break;
            case 224:
                ch_func_name = "SHA224";
                break;
            case 384:
                ch_func_name = "SHA384";
                break;
            case 512:
                ch_func_name = "SHA512";
                break;
            default:
                throw DB::Exception(
                    DB::ErrorCodes::BAD_ARGUMENTS, "Second argument of function {} must be one of 0, 224, 256, 384 or 512", getName());
        }

        const auto * sha2_node = toFunctionNode(actions_dag, ch_func_name, {str_arg});
        const auto * hex_node = toFunctionNode(actions_dag, "hex", {sha2_node});
        const auto * lower_node = toFunctionNode(actions_dag, "lower", {hex_node});
        return convertNodeTypeIfNeeded(substrait_func, lower_node, actions_dag);
    }
};

static FunctionParserRegister<FunctionParserSha2> register_sha2;
}
