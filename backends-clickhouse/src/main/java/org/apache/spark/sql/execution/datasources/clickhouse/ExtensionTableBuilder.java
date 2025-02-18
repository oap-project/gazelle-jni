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
package org.apache.spark.sql.execution.datasources.clickhouse;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.sql.execution.datasources.mergetree.MetaSerializer;
import org.apache.spark.sql.execution.datasources.mergetree.PartSerializer;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import scala.collection.Seq;
import scala.collection.Seq$;

public class ExtensionTableBuilder {
  private ExtensionTableBuilder() {}

  public static ExtensionTableNode makeExtensionTable(
      Long start, Long end, Long step, Integer numSlices, Integer sliceIndex)
      throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode json = mapper.createObjectNode();
    json.put("start", start);
    json.put("end", end);
    json.put("step", step);
    json.put("numSlices", numSlices);
    json.put("sliceIndex", sliceIndex);

    String result;
    result = mapper.writeValueAsString(json);
    return new ExtensionTableNode(
        Collections.emptyList(), result, (Seq<String>) Seq$.MODULE$.<String>empty());
  }

  public static ExtensionTableNode makeExtensionTable(
      String database,
      String tableName,
      String snapshotId,
      String relativeTablePath,
      String absoluteTablePath,
      String orderByKey,
      String lowCardKey,
      String minmaxIndexKey,
      String bfIndexKey,
      String setIndexKey,
      String primaryKey,
      PartSerializer partSerializer,
      StructType tableSchema,
      Map<String, String> clickhouseTableConfigs,
      List<String> preferredLocations) {

    String result =
        MetaSerializer.apply(
            database,
            tableName,
            snapshotId,
            relativeTablePath,
            absoluteTablePath,
            orderByKey,
            lowCardKey,
            minmaxIndexKey,
            bfIndexKey,
            setIndexKey,
            primaryKey,
            partSerializer,
            tableSchema,
            clickhouseTableConfigs);
    return new ExtensionTableNode(
        preferredLocations, result, partSerializer.pathList(absoluteTablePath));
  }
}
