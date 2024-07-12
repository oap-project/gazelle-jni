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
package org.apache.gluten.validate;

import org.apache.gluten.extension.ValidationResult;

import java.util.Vector;

public class NativePlanValidationInfo {
  private final Vector<String> fallbackInfo = new Vector<>();
  private final int isSupported;

  public NativePlanValidationInfo(int isSupported, String fallbackInfo) {
    this.isSupported = isSupported;
    String[] splitInfo = fallbackInfo.split("@");
    for (int i = 0; i < splitInfo.length; i++) {
      this.fallbackInfo.add(splitInfo[i]);
    }
  }

  public ValidationResult asResult() {
    if (isSupported == 1) {
      return ValidationResult.succeeded();
    }
    return ValidationResult.failed(
        String.format(
            "Native validation failed: %n%s",
            fallbackInfo.stream().reduce((l, r) -> l + "\n" + r)));
  }
}
