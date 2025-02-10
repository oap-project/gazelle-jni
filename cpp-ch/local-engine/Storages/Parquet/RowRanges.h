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
#include <cassert>
#include <memory>
#include <string>
namespace local_engine
{

class RowRanges;

struct Range
{
    friend class RowRanges;

public:
    static std::optional<Range> unionRange(const Range & left, const Range & right)
    {
        if (left.from <= right.from)
        {
            if (left.to + 1 >= right.from)
                return Range{left.from, std::max(left.to, right.to)};
        }
        else if (right.to + 1 >= left.from)
        {
            return Range{right.from, std::max(left.to, right.to)};
        }
        return std::nullopt;
    }

    static std::optional<Range> intersection(const Range & left, const Range & right)
    {
        if (left.from <= right.from)
        {
            if (left.to >= right.from)
                return Range{right.from, std::min(left.to, right.to)};
        }
        else if (right.to >= left.from)
        {
            return Range{left.from, std::min(left.to, right.to)};
        }
        return std::nullopt; // Return a default Range object if no intersection range found
    }

    Range(const size_t from_, const size_t to_) : from(from_), to(to_) { assert(from <= to); }

    size_t count() const { return to - from + 1; }

    bool isBefore(const Range & other) const { return to < other.from; }

    bool isAfter(const Range & other) const { return from > other.to; }

    std::string toString() const { return "[" + std::to_string(from) + ", " + std::to_string(to) + "]"; }


    size_t from;
    size_t to;
};

class RowRanges
{
    std::vector<Range> ranges;

public:
    RowRanges() = default;

    explicit RowRanges(const Range & range) { ranges.push_back(range); }
    explicit RowRanges(const std::vector<Range> & ranges_) : ranges(ranges_) { }

    static RowRanges createSingle(const size_t rowCount) { return RowRanges({Range(0L, rowCount - 1L)}); }

    static RowRanges unionRanges(const RowRanges & left, const RowRanges & right)
    {
        RowRanges result;
        auto it1_pair = std::make_pair(left.ranges.begin(), left.ranges.end());
        auto it2_pair = std::make_pair(right.ranges.begin(), right.ranges.end());
        if (it2_pair.first != it2_pair.second)
        {
            Range range2 = *it2_pair.first;
            while (it1_pair.first != it1_pair.second)
            {
                Range range1 = *it1_pair.first;
                if (range1.isAfter(range2))
                {
                    result.add(range2);
                    range2 = range1;
                    std::swap(it1_pair, it2_pair);
                }
                else
                {
                    result.add(range1);
                }
                ++it1_pair.first;
            }
            result.add(range2);
        }
        else
        {
            it2_pair = it1_pair;
        }
        while (it2_pair.first != it2_pair.second)
        {
            result.add(*it2_pair.first);
            ++it2_pair.first;
        }

        return result;
    }

    static RowRanges intersection(const RowRanges & left, const RowRanges & right)
    {
        RowRanges result;

        int rightIndex = 0;
        for (const Range & l : left.ranges)
        {
            for (int i = rightIndex, n = right.ranges.size(); i < n; ++i)
            {
                const Range & r = right.ranges[i];
                if (l.isBefore(r))
                {
                    break;
                }
                else if (l.isAfter(r))
                {
                    rightIndex = i + 1;
                    continue;
                }
                auto tmp = Range::intersection(l, r);
                result.add(tmp.value());
            }
        }
        return result;
    }

    RowRanges slice(const size_t from, const size_t to) const
    {
        RowRanges result;
        for (const Range & range : ranges)
            if (range.from >= from && range.to <= to)
                result.add(range);
        return result;
    }

    void add(const Range & range)
    {
        Range rangeToAdd = range;
        for (int i = ranges.size() - 1; i >= 0; --i)
        {
            Range last = ranges[i];
            assert(!last.isAfter(range));
            const auto u = Range::unionRange(last, rangeToAdd);
            if (!u.has_value())
                break;
            rangeToAdd = u.value();
            ranges.erase(ranges.begin() + i);
        }
        ranges.push_back(rangeToAdd);
    }

    size_t rowCount() const
    {
        size_t cnt = 0;
        for (const Range & range : ranges)
            cnt += range.count();
        return cnt;
    }

    bool isOverlapping(const size_t from, const size_t to) const
    {
        const Range searchRange(from, to);
        const auto it = std::ranges::lower_bound(ranges, searchRange, [](const Range & r1, const Range & r2) { return r1.isBefore(r2); });
        return it != ranges.end() && !it->isAfter(searchRange);
    }

    const std::vector<Range> & getRanges() const { return ranges; }
    std::vector<Range> & getRanges() { return ranges; }

    const Range & getRange(size_t index) const { return ranges[index]; }
    Range & getRange(size_t index) { return ranges[index]; }

    std::string toString() const
    {
        std::string result = "[";
        for (const Range & range : ranges)
            result += "(" + std::to_string(range.from) + ", " + std::to_string(range.to) + "), ";
        if (!ranges.empty())
            result = result.substr(0, result.size() - 2);
        result += "]";
        return result;
    }
};

using RowRangesPtr = std::shared_ptr<RowRanges>;
}