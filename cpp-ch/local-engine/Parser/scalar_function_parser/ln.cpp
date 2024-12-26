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
#include <Parser/scalar_function_parser/logarithm.h>

namespace local_engine
{

class FunctionParserLn : public FunctionParserLogBase
{
public:
    explicit FunctionParserLn(ParserContextPtr parser_context_) : FunctionParserLogBase(parser_context_) {}
    ~FunctionParserLn() override = default;

    static constexpr auto name = "ln";

    String getName() const override { return name; }
    String getCHFunctionName() const override { return name; }
    const DB::ActionsDAG::Node * getParameterLowerBound(DB::ActionsDAG & actions_dag, const DB::DataTypePtr & data_type) const override
    {
        return addColumnToActionsDAG(actions_dag, data_type, 0.0);
    }
};

static FunctionParserRegister<FunctionParserLn> register_ln;
}
