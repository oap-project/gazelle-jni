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
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionGetDateData.h>

namespace local_engine
{
class SparkFunctionConvertToDate : public FunctionGetDateData<false, true, DB::DataTypeDate32::FieldType>
{
public:
    static constexpr auto name = "sparkToDate";
    static DB::FunctionPtr create(DB::ContextPtr) { return std::make_shared<SparkFunctionConvertToDate>(); }
    SparkFunctionConvertToDate() = default;
    ~SparkFunctionConvertToDate() override = default;
    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo &) const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DB::DataTypePtr getReturnTypeImpl(const DB::ColumnsWithTypeAndName &) const override
    {
        auto data_type = std::make_shared<DB::DataTypeDate32>();
        return makeNullable(data_type);
    }
};

REGISTER_FUNCTION(SparkToDate)
{
    factory.registerFunction<SparkFunctionConvertToDate>();
}

}
