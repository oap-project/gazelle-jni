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

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

namespace local_engine
{
class SparkFunctionReverseParser : public FunctionParser
{
public:
    SparkFunctionReverseParser(ParserContextPtr parser_context_) : FunctionParser(parser_context_) {}
    ~SparkFunctionReverseParser() override = default;

    static constexpr auto name = "reverse";
    String getName() const override { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction & func) const override
    {
        if (func.output_type().has_list())
            return "arrayReverse";
        return "reverseUTF8";
    }
};
static FunctionParserRegister<SparkFunctionReverseParser> register_reverse;
}
