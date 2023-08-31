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
#include "StringUtils.h"
#include <filesystem>
#include <boost/algorithm/string.hpp>
#include <Poco/StringTokenizer.h>

namespace local_engine
{
PartitionValues StringUtils::parsePartitionTablePath(const std::string & file)
{
    PartitionValues result;
    Poco::StringTokenizer path(file, "/");
    for (const auto & item : path)
    {
        auto position = item.find('=');
        if (position != std::string::npos)
        {
            result.emplace_back(PartitionValue(boost::algorithm::to_lower_copy(item.substr(0, position)), item.substr(position + 1)));
        }
    }
    return result;
}
bool StringUtils::isNullPartitionValue(const std::string & value)
{
    return value == "__HIVE_DEFAULT_PARTITION__";
}

std::string StringUtils::getValueFromMapString(const std::string & map, const std::string key)
{
    /*
     * This function parses map string generated by java code
     * in map string
     * keys are seperated by comma,key and value are connected by equation,like this:
     * {k1=v1,k2=v2,k3=v3}
     */
    // remove end } and add , so {k1=v1,k2=v2,k3=v3}->{k1=v1,k2=v2,k3=v3,
    std::string tempMap = map.substr(0, map.size()-1) + ",";
    size_t posKey = tempMap.find(key);
    if(posKey == std::string::npos)
        return "";
    std::string subMap = tempMap.substr(posKey, tempMap.size());
    size_t start = subMap.find("=");
    size_t end = subMap.find(",");
    return subMap.substr(start+1, end-start-1);
}

}
