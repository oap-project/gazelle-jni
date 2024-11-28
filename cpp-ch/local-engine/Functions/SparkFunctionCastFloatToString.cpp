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

#include <base/TypeList.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/castTypeToEither.h>
#include <format>

using namespace DB;

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}
}

namespace local_engine
{

class SparkFunctionCastFloatToString : public IFunction
{
public:
    size_t getNumberOfArguments() const override { return 1; }
    static constexpr auto name = "sparkCastFloatToString";
    SparkFunctionCastFloatToString() = default;
    ~SparkFunctionCastFloatToString() override = default;
    static DB::FunctionPtr create(DB::ContextPtr) { return std::make_shared<SparkFunctionCastFloatToString>(); }
    String getName() const override { return name; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<const DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t) const override
    {
        if (arguments.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {}'s arguments number must be 1", name);
        if (!isFloat(arguments[0].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {}'s 1st argument must be float type", name);
        auto res_col = ColumnString::create();
        using Types = TypeList<DataTypeFloat32, DataTypeFloat64>;
        castTypeToEither(Types{}, arguments[0].type.get(), [&](const auto & arg_type)
        {
            using F = typename std::decay_t<decltype(arg_type)>::FieldType;
            const ColumnVector<F> * src_col = checkAndGetColumn<ColumnVector<F>>(arguments[0].column.get());
            for (size_t i = 0 ; i < src_col->size(); ++i)
            {
                String res_data = std::format("{}", src_col->getElement(i));
                /// If no decimal after the float number, then add a dot and zero chars ('.0') after the casted string.
                if (std::count(res_data.begin(), res_data.end(), '.') == 0)
                    res_data = res_data + ".0";
                res_col->insertData(res_data.data(), res_data.size());
            }
            return true;
        });
        return std::move(res_col);
    }
};

REGISTER_FUNCTION(SparkFunctionCastFloatToString)
{
    factory.registerFunction<SparkFunctionCastFloatToString>();
}

}
