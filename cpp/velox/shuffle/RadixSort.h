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
#include <algorithm>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <vector>

namespace gluten {

// Spark radix sort implementation. This implementation is for shuffle sort only as it removes unused
// params (desc, signed) in shuffle.
// https://github.com/apache/spark/blob/308669fc301916837bacb7c3ec1ecef93190c094/core/src/main/java/org/apache/spark/util/collection/unsafe/sort/RadixSort.java#L25
class RadixSort {
 public:
  // Sorts a given array of longs using least-significant-digit radix sort. This routine assumes
  // you have extra space at the end of the array at least equal to the number of records. The
  // sort is destructive and may relocate the data positioned within the array.
  //
  // @param array array of long elements followed by at least that many empty slots.
  // @param numRecords number of data records in the array.
  // @param startByteIndex the first byte (in range [0, 7]) to sort each long by, counting from the
  //                       least significant byte.
  // @param endByteIndex the last byte (in range [0, 7]) to sort each long by, counting from the
  //                     least significant byte. Must be greater than startByteIndex.
  //
  // @return The starting index of the sorted data within the given array. We return this instead
  //         of always copying the data back to position zero for efficiency.
  static int32_t sort(uint64_t* array, size_t size, int64_t numRecords, int32_t startByteIndex, int32_t endByteIndex) {
    assert(startByteIndex >= 0 && "startByteIndex should >= 0");
    assert(endByteIndex <= 7 && "endByteIndex should <= 7");
    assert(endByteIndex > startByteIndex);
    assert(numRecords * 2 <= size);

    int64_t inIndex = 0;
    int64_t outIndex = numRecords;

    if (numRecords > 0) {
      auto counts = getCounts(array, numRecords, startByteIndex, endByteIndex);

      for (auto i = startByteIndex; i <= endByteIndex; i++) {
        if (!counts[i].empty()) {
          sortAtByte(array, numRecords, counts[i], i, inIndex, outIndex);
          std::swap(inIndex, outIndex);
        }
      }
    }

    return static_cast<int32_t>(inIndex);
  }

 private:
  // Performs a partial sort by copying data into destination offsets for each byte value at the
  // specified byte offset.
  //
  // @param array array to partially sort.
  // @param numRecords number of data records in the array.
  // @param counts counts for each byte value. This routine destructively modifies this array.
  // @param byteIdx the byte in a long to sort at, counting from the least significant byte.
  // @param inIndex the starting index in the array where input data is located.
  // @param outIndex the starting index where sorted output data should be written.
  static void sortAtByte(
      uint64_t* array,
      int64_t numRecords,
      std::vector<int64_t>& counts,
      int32_t byteIdx,
      int64_t inIndex,
      int64_t outIndex) {
    assert(counts.size() == 256);

    auto offsets = transformCountsToOffsets(counts, outIndex);

    for (auto offset = inIndex; offset < inIndex + numRecords; ++offset) {
      auto bucket = (array[offset] >> (byteIdx * 8)) & 0xff;
      array[offsets[bucket]++] = array[offset];
    }
  }

  // Computes a value histogram for each byte in the given array.
  //
  // @param array array to count records in.
  // @param numRecords number of data records in the array.
  // @param startByteIndex the first byte to compute counts for (the prior are skipped).
  // @param endByteIndex the last byte to compute counts for.
  //
  // @return a vector of eight 256-element count arrays, one for each byte starting from the least
  //         significant byte. If the byte does not need sorting the vector entry will be empty.
  static std::vector<std::vector<int64_t>>
  getCounts(uint64_t* array, int64_t numRecords, int32_t startByteIndex, int32_t endByteIndex) {
    std::vector<std::vector<int64_t>> counts;
    counts.resize(8);

    // Optimization: do a fast pre-pass to determine which byte indices we can skip for sorting.
    // If all the byte values at a particular index are the same we don't need to count it.
    int64_t bitwiseMax = 0;
    int64_t bitwiseMin = -1L;
    for (auto offset = 0; offset < numRecords; ++offset) {
      auto value = array[offset];
      bitwiseMax |= value;
      bitwiseMin &= value;
    }
    auto bitsChanged = bitwiseMin ^ bitwiseMax;

    // Compute counts for each byte index.
    for (auto i = startByteIndex; i <= endByteIndex; i++) {
      if (((bitsChanged >> (i * 8)) & 0xff) != 0) {
        counts[i].resize(256);
        for (auto offset = 0; offset < numRecords; ++offset) {
          counts[i][(array[offset] >> (i * 8)) & 0xff]++;
        }
      }
    }

    return counts;
  }

  // Transforms counts into the proper output offsets for the sort type.
  //
  // @param counts counts for each byte value. This routine destructively modifies this vector.
  // @param numRecords number of data records in the original data array.
  // @param outputOffset output offset in bytes from the base array object.
  //
  // @return the input counts vector.
  static std::vector<int64_t>& transformCountsToOffsets(std::vector<int64_t>& counts, int64_t outputOffset) {
    assert(counts.size() == 256);

    int64_t pos = outputOffset;
    for (auto i = 0; i < 256; i++) {
      auto tmp = counts[i & 0xff];
      counts[i & 0xff] = pos;
      pos += tmp;
    }

    return counts;
  }
};

} // namespace gluten
