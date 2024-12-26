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
#include "SparkFunctionRoundHalfUp.h"
#include <Functions/FunctionFactory.h>

namespace local_engine
{
REGISTER_FUNCTION(RoundSpark)
{
    factory.registerFunction<FunctionRoundHalfUp>(
        DB::FunctionDocumentation{
            .description=R"(
Similar to function round,except that in case when given number has equal distance to surrounding numbers, the function rounds away from zero(towards +inf/-inf).
        )",
            .examples{{"roundHalfUp", "SELECT roundHalfUp(3.165,2)", "3.17"}},
            .categories{"Rounding"}
        },
        DB::FunctionFactory::Case::Insensitive);

}
}
