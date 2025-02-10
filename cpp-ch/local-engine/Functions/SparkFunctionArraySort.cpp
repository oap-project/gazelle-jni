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
#include <Columns/ColumnFunction.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <base/sort.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB::ErrorCodes
{
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TYPE_MISMATCH;
    extern const int ILLEGAL_COLUMN;
}

/// The usage of `arraySort` in CH is different from Spark's `sort_array` function.
/// We need to implement a custom function to sort arrays.
namespace local_engine
{

struct LambdaLess
{
    const DB::IColumn & column;
    DB::DataTypePtr type;
    const DB::ColumnFunction & lambda;
    explicit LambdaLess(const DB::IColumn & column_, DB::DataTypePtr type_, const DB::ColumnFunction & lambda_)
        : column(column_), type(type_), lambda(lambda_) {}

    /// May not efficient
    bool operator()(size_t lhs, size_t rhs) const
    {
        /// The column name seems not matter.
        auto left_value_col = DB::ColumnWithTypeAndName(oneRowColumn(lhs), type, "left");
        auto right_value_col = DB::ColumnWithTypeAndName(oneRowColumn(rhs), type, "right");
        auto cloned_lambda = lambda.cloneResized(1);
        auto * lambda_ = typeid_cast<DB::ColumnFunction *>(cloned_lambda.get());
        lambda_->appendArguments({std::move(left_value_col), std::move(right_value_col)});
        auto compare_res_col = lambda_->reduce();
        DB::Field field;
        compare_res_col.column->get(0, field);
        return field.safeGet<Int32>() < 0;
    }
private:
    ALWAYS_INLINE DB::ColumnPtr oneRowColumn(size_t i) const
    {
        auto res = column.cloneEmpty();
        res->insertFrom(column, i);
        return std::move(res);
    }
};

struct Less
{
    const DB::IColumn & column;

    explicit Less(const DB::IColumn & column_) : column(column_) { }

    bool operator()(size_t lhs, size_t rhs) const
    {
        return column.compareAt(lhs, rhs, column, 1) < 0;
    }
};

class FunctionSparkArraySort : public DB::IFunction
{
public:
    static constexpr auto name = "arraySortSpark";
    static DB::FunctionPtr create(DB::ContextPtr /*context*/) { return std::make_shared<FunctionSparkArraySort>(); }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo &) const override { return true; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    void getLambdaArgumentTypes(DB::DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
            throw DB::Exception(DB::ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Function {} requires as arguments a lambda function and an array", getName());

        if (arguments.size() > 1)
        {
            const auto * lambda_function_type = DB::checkAndGetDataType<DB::DataTypeFunction>(arguments[0].get());
            if (!lambda_function_type || lambda_function_type->getArgumentTypes().size() != 2)
                throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument of function {} must be a lambda function with 2 arguments, found {} instead.",
                        getName(), arguments[0]->getName());
            auto array_nesteed_type = DB::checkAndGetDataType<DB::DataTypeArray>(arguments.back().get())->getNestedType();
            DB::DataTypes lambda_args = {array_nesteed_type, array_nesteed_type};
            arguments[0] = std::make_shared<DB::DataTypeFunction>(lambda_args);
        }
    }

    DB::DataTypePtr getReturnTypeImpl(const DB::ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() > 1)
        {
            const auto * lambda_function_type = checkAndGetDataType<DB::DataTypeFunction>(arguments[0].type.get());
            if (!lambda_function_type)
                throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be a function", getName());
        }

        return arguments.back().type;
    }

    DB::ColumnPtr executeImpl(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr &, size_t input_rows_count) const override
    {
        auto array_col = arguments.back().column;
        auto array_type = arguments.back().type;
        DB::ColumnPtr null_map = nullptr;
        if (const auto * null_col = typeid_cast<const DB::ColumnNullable *>(array_col.get()))
        {
            null_map = null_col->getNullMapColumnPtr();
            array_col = null_col->getNestedColumnPtr();
            array_type = typeid_cast<const DB::DataTypeNullable *>(array_type.get())->getNestedType();
        }

        const auto * array_col_concrete = DB::checkAndGetColumn<DB::ColumnArray>(array_col.get());
        if (!array_col_concrete)
        {
            const auto * aray_col_concrete_const = DB::checkAndGetColumnConst<DB::ColumnArray>(array_col.get());
            if (!aray_col_concrete_const)
            {
                throw DB::Exception(DB::ErrorCodes::ILLEGAL_COLUMN, "Expected array column, found {}", array_col->getName());
            }
            array_col = DB::recursiveRemoveLowCardinality(aray_col_concrete_const->convertToFullColumn());
            array_col_concrete = DB::checkAndGetColumn<DB::ColumnArray>(array_col.get());
        }
        auto array_nested_type = DB::checkAndGetDataType<DB::DataTypeArray>(array_type.get())->getNestedType();

        DB::ColumnPtr sorted_array_col = nullptr;
        if (arguments.size() > 1)
            sorted_array_col = executeWithLambda(*array_col_concrete, array_nested_type, *checkAndGetColumn<DB::ColumnFunction>(arguments[0].column.get()));
        else
            sorted_array_col = executeWithoutLambda(*array_col_concrete);

        if (null_map)
        {
            sorted_array_col = DB::ColumnNullable::create(sorted_array_col, null_map);
        }
        return sorted_array_col;
    }
private:
    static DB::ColumnPtr executeWithLambda(const DB::ColumnArray & array_col, DB::DataTypePtr array_nested_type, const DB::ColumnFunction & lambda)
    {
        const auto & offsets = array_col.getOffsets();
        auto rows = array_col.size();

        size_t nested_size = array_col.getData().size();
        DB::IColumn::Permutation permutation(nested_size);
        for (size_t i = 0; i < nested_size; ++i)
            permutation[i] = i;

        DB::ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < rows; ++i)
        {
            auto next_offset = offsets[i];
            ::sort(&permutation[current_offset],
                &permutation[next_offset],
                LambdaLess(array_col.getData(),
                    array_nested_type,
                    lambda));
            current_offset = next_offset;
        }
        auto res = DB::ColumnArray::create(array_col.getData().permute(permutation, 0), array_col.getOffsetsPtr());
        return res;
    }

    static DB::ColumnPtr executeWithoutLambda(const DB::ColumnArray & array_col)
    {
        const auto & offsets = array_col.getOffsets();
        auto rows = array_col.size();

        size_t nested_size = array_col.getData().size();
        DB::IColumn::Permutation permutation(nested_size);
        for (size_t i = 0; i < nested_size; ++i)
            permutation[i] = i;

        DB::ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < rows; ++i)
        {
            auto next_offset = offsets[i];
            ::sort(&permutation[current_offset],
                    &permutation[next_offset],
                    Less(array_col.getData()));
            current_offset = next_offset;
        }
        auto res = DB::ColumnArray::create(array_col.getData().permute(permutation, 0), array_col.getOffsetsPtr());
        return res;
    }

    String getName() const override
    {
        return name;
    }

};

REGISTER_FUNCTION(ArraySortSpark)
{
    factory.registerFunction<FunctionSparkArraySort>();
}
}
