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
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>

using namespace DB;

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

namespace local_engine
{
class SparkFunctionArraysOverlap : public IFunction
{
public:
    static constexpr auto name = "sparkArraysOverlap";
    static FunctionPtr create(ContextPtr) { return std::make_shared<SparkFunctionArraysOverlap>(); }
    SparkFunctionArraysOverlap() = default;
    ~SparkFunctionArraysOverlap() override = default;
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    size_t getNumberOfArguments() const override { return 2; }
    String getName() const override { return name; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DB::DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName &) const override
    {
        auto data_type = std::make_shared<DataTypeUInt8>();
        return makeNullable(data_type);
    }
    
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} must have 2 arguments", getName());
        
        auto res = ColumnUInt8::create(input_rows_count, 0);
        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        PaddedPODArray<UInt8> & res_data = res->getData();
        PaddedPODArray<UInt8> & null_map_data = null_map->getData();
        if (input_rows_count == 0)
            return ColumnNullable::create(std::move(res), std::move(null_map));
        
        const ColumnArray * array_col_1 = checkAndGetColumn<ColumnArray>(arguments[0].column.get());  
        const ColumnArray * array_col_2 = checkAndGetColumn<ColumnArray>(arguments[1].column.get());
        if (!array_col_1 || !array_col_2)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} 1st/2nd argument must be array type", getName());

        const ColumnArray::Offsets & array_offsets_1 = array_col_1->getOffsets();
        const ColumnArray::Offsets & array_offsets_2 = array_col_2->getOffsets();

        size_t current_offset_1 = 0, current_offset_2 = 0;
        size_t array_pos_1 = 0, array_pos_2 = 0;
        for (size_t i = 0; i < array_col_1->size(); ++i)
        {
            size_t array_size_1 = array_offsets_1[i] - current_offset_1;
            size_t array_size_2 = array_offsets_2[i] - current_offset_2;
            auto executeCompare = [&](const IColumn & col1, const IColumn & col2, const ColumnUInt8 * null_map1, const ColumnUInt8 * null_map2) -> void
            {   
                for (size_t j = 0; j < array_size_1 && !res_data[i]; ++j)
                {
                    for (size_t k = 0; k < array_size_2; ++k)
                    {
                        if ((null_map1 && null_map1->getElement(j + array_pos_1)) || (null_map2 && null_map2->getElement(k + array_pos_2)))
                        {
                            null_map_data[i] = 1;
                        }
                        else if (col1.compareAt(j + array_pos_1, k + array_pos_2, col2, -1) == 0)
                        {
                            res_data[i] = 1;
                            null_map_data[i] = 0;
                            break;
                        }
                    }
                }
            };
            if (array_col_1->getData().isNullable() || array_col_2->getData().isNullable())
            {
                if (array_col_1->getData().isNullable() && array_col_2->getData().isNullable())
                {
                    const ColumnNullable * array_null_col_1 = assert_cast<const ColumnNullable *>(&array_col_1->getData());
                    const ColumnNullable * array_null_col_2 = assert_cast<const ColumnNullable *>(&array_col_2->getData());
                    executeCompare(array_null_col_1->getNestedColumn(), array_null_col_2->getNestedColumn(),
                        &array_null_col_1->getNullMapColumn(), &array_null_col_2->getNullMapColumn());
                }
                else if (array_col_1->getData().isNullable())
                {
                    const ColumnNullable * array_null_col_1 = assert_cast<const ColumnNullable *>(&array_col_1->getData());
                    executeCompare(array_null_col_1->getNestedColumn(), array_col_2->getData(), &array_null_col_1->getNullMapColumn(), nullptr);
                }
                else if (array_col_2->getData().isNullable())
                {
                    const ColumnNullable * array_null_col_2 = assert_cast<const ColumnNullable *>(&array_col_2->getData());
                    executeCompare(array_col_1->getData(), array_null_col_2->getNestedColumn(), nullptr, &array_null_col_2->getNullMapColumn());
                }
            }
            else if (array_col_1->getData().getDataType() == array_col_2->getData().getDataType())
            {
                executeCompare(array_col_1->getData(), array_col_2->getData(), nullptr, nullptr);
            }

            current_offset_1 = array_offsets_1[i];
            current_offset_2 = array_offsets_2[i];
            array_pos_1 += array_size_1;
            array_pos_2 += array_size_2;
        }
        return ColumnNullable::create(std::move(res), std::move(null_map));
    }
};

REGISTER_FUNCTION(SparkArraysOverlap)
{
    factory.registerFunction<SparkFunctionArraysOverlap>();
}

}
