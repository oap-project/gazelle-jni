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

#include <Columns/ColumnConst.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionTokens.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/Regexps.h>
#include <base/map.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}


/** Functions that split strings into an array of strings or vice versa.
  *
  * splitByRegexp(regexp, s[, max_substrings])
  */
namespace
{

using Pos = const char *;

class SparkSplitByRegexpImpl
{
private:
    Regexps::RegexpPtr re;
    OptimizedRegularExpression::MatchVec matches;

    Pos pos;
    Pos end;

    std::optional<size_t> max_splits;
    size_t splits;
    bool max_substrings_includes_remaining_string;

public:
    static constexpr auto name = "splitByRegexpSpark";

    static bool isVariadic() { return true; }
    static size_t getNumberOfArguments() { return 0; }

    static ColumnNumbers getArgumentsThatAreAlwaysConstant() { return {0, 2}; }

    static void checkArguments(const IFunction & func, const ColumnsWithTypeAndName & arguments)
    {
        checkArgumentsWithSeparatorAndOptionalMaxSubstrings(func, arguments);
    }

    static constexpr auto strings_argument_position = 1uz;

    void init(const ColumnsWithTypeAndName & arguments, bool max_substrings_includes_remaining_string_)
    {
        const ColumnConst * col = checkAndGetColumnConstStringOrFixedString(arguments[0].column.get());

        if (!col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}. "
                            "Must be constant string.", arguments[0].column->getName(), name);

        if (!col->getValue<String>().empty())
            re = std::make_shared<OptimizedRegularExpression>(Regexps::createRegexp<false, false, false>(col->getValue<String>()));

        max_substrings_includes_remaining_string = max_substrings_includes_remaining_string_;
        max_splits = extractMaxSplits(arguments, 2);
    }

    /// Called for each next string.
    void set(Pos pos_, Pos end_)
    {
        pos = pos_;
        end = end_;
        splits = 0;
    }

    /// Get the next token, if any, or return false.
    bool get(Pos & token_begin, Pos & token_end)
    {
        if (!re)
        {
            if (pos == end)
                return false;

            token_begin = pos;

            if (max_splits)
            {
                if (max_substrings_includes_remaining_string)
                {
                    if (splits == *max_splits - 1)
                    {
                        token_end = end;
                        pos = end;
                        return true;
                    }
                }
                else
                    if (splits == *max_splits)
                        return false;
            }

            ++pos;
            token_end = pos;
            ++splits;
        }
        else
        {
            if (!pos || pos > end)
                return false;

            token_begin = pos;

            if (max_splits)
            {
                if (max_substrings_includes_remaining_string)
                {
                    if (splits == *max_splits - 1)
                    {
                        token_end = end;
                        pos = nullptr;
                        return true;
                    }
                }
                else
                    if (splits == *max_splits)
                        return false;
            }

            auto res = re->match(pos, end - pos, matches);
            if (!res)
            {
                token_end = end;
                pos = end + 1;
            }
            else if (!matches[0].length)
            {
                /// If match part is empty, increment position to avoid infinite loop.
                token_end = (pos == end ? end : pos + 1);
                ++pos;
                ++splits;
            }
            else
            {
                token_end = pos + matches[0].offset;
                pos = token_end + matches[0].length;
                ++splits;
            }
        }

        return true;
    }
};

using SparkFunctionSplitByRegexp = FunctionTokens<SparkSplitByRegexpImpl>;

/// Fallback splitByRegexp to splitByChar when its 1st argument is a trivial char for better performance
class SparkSplitByRegexpOverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = "splitByRegexpSpark";
    static FunctionOverloadResolverPtr create(ContextPtr context) { return std::make_unique<SparkSplitByRegexpOverloadResolver>(context); }

    explicit SparkSplitByRegexpOverloadResolver(ContextPtr context_)
        : context(context_)
        , split_by_regexp(SparkFunctionSplitByRegexp::create(context)) {}

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return SparkSplitByRegexpImpl::getNumberOfArguments(); }
    bool isVariadic() const override { return SparkSplitByRegexpImpl::isVariadic(); }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        if (patternIsTrivialChar(arguments))
            return FunctionFactory::instance().getImpl("splitByChar", context)->build(arguments);
        return std::make_unique<FunctionToFunctionBaseAdaptor>(
            split_by_regexp, collections::map<DataTypes>(arguments, [](const auto & elem) { return elem.type; }), return_type);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        return split_by_regexp->getReturnTypeImpl(arguments);
    }

private:
    bool patternIsTrivialChar(const ColumnsWithTypeAndName & arguments) const
    {
        if (!arguments[0].column.get())
            return false;
        const ColumnConst * col = checkAndGetColumnConstStringOrFixedString(arguments[0].column.get());
        if (!col)
            return false;

        String pattern = col->getValue<String>();
        if (pattern.size() == 1)
        {
            OptimizedRegularExpression re = Regexps::createRegexp<false, false, false>(pattern);

            std::string required_substring;
            bool is_trivial;
            bool required_substring_is_prefix;
            re.getAnalyzeResult(required_substring, is_trivial, required_substring_is_prefix);
            return is_trivial && required_substring == pattern;
        }
        return false;
    }

    ContextPtr context;
    FunctionPtr split_by_regexp;
};
}

REGISTER_FUNCTION(SparkSplitByRegexp)
{
    factory.registerFunction<SparkSplitByRegexpOverloadResolver>();
}

}
