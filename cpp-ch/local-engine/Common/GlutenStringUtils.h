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
#include <algorithm>
#include <sstream>
#include <string>
#include <fmt/core.h>

namespace local_engine
{
namespace GlutenStringUtils
{
bool isNullPartitionValue(const std::string & value);

template <typename T>
struct is_pair : std::false_type
{
};

template <typename K, typename V>
struct is_pair<std::pair<K, V>> : std::true_type
{
};

template <typename Container>
using is_kv_container = is_pair<typename Container::value_type>;

template <typename Container>
std::string mkString(
    const Container & container,
    const std::string & delimiter = ", ",
    const std::string & prefix = "[",
    const std::string & suffix = "]",
    const std::string & pairSeparator = "=")
{
    if (container.empty())
        return "<empty>";

    std::string result = prefix;

    bool first = true;
    for (const auto & element : container)
    {
        if (!first)
            result += delimiter;
        if constexpr (is_kv_container<Container>::value)
            result += fmt::format("{}{}{}", element.first, pairSeparator, element.second);
        else
            result += fmt::format("{}", element);
        first = false;
    }

    result += suffix;
    return result;
}

};
}
