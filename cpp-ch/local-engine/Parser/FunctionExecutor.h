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

#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Parser/ExpressionParser.h>
#include <Parser/ParserContext.h>
#include <Parser/SerializedPlanParser.h>
#include <base/types.h>
#include <substrait/algebra.pb.h>
#include <substrait/extensions/extensions.pb.h>

namespace local_engine
{


class FunctionExecutor
{
public:
    struct TestCase
    {
        std::vector<DB::Field> inputs;
        DB::Field expect_output;
    };

    FunctionExecutor(
        const String & name_, const DB::DataTypes & input_types_, const DB::DataTypePtr & output_type_, const DB::ContextPtr & context_)
        : name(name_)
        , input_types(input_types_)
        , output_type(output_type_)
        , context(context_)
        , log(&Poco::Logger::get("FunctionExecutor"))
    {
        buildExpressionParser();
        buildExpression();
        buildHeader();

        parseExpression();
    }

    void execute(DB::Block & block);

    bool executeAndCompare(const std::vector<TestCase> & cases);

    DB::Block getHeader() const;

    String getResultName() const;

private:
    void buildExpressionParser();
    void buildExpression();
    void buildHeader();

    void parseExpression();

    /// substrait scalar function name
    String name;
    DB::DataTypes input_types;
    DB::DataTypePtr output_type;
    DB::ContextPtr context;
    std::unique_ptr<ExpressionParser> expression_parser;

    ::google::protobuf::RepeatedPtrField<substrait::extensions::SimpleExtensionDeclaration> extensions;
    substrait::Expression expression;
    DB::Block header;
    String result_name;
    std::unique_ptr<DB::ExpressionActions> expression_actions;

    Poco::Logger * log;
};

}
