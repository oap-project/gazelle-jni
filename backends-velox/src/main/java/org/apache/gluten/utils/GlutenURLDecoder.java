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
package org.apache.gluten.utils;

import java.io.*;
import java.net.URLEncoder;

public class GlutenURLDecoder {
  // This Util is copied from
  // https://github.com/openjdk-mirror/jdk7u-jdk/blob/master/src/share/classes/java/net/URLDecoder.java
  // Just remove the special handle for '+'
  /**
   * Decodes a <code>application/x-www-form-urlencoded</code> string using a specific encoding
   * scheme. The supplied encoding is used to determine what characters are represented by any
   * consecutive sequences of the form "<code>%<i>xy</i></code>".
   *
   * <p><em><strong>Note:</strong> The <a href=
   * "http://www.w3.org/TR/html40/appendix/notes.html#non-ascii-chars"> World Wide Web Consortium
   * Recommendation</a> states that UTF-8 should be used. Not doing so may introduce
   * incompatibilities.</em>
   *
   * @param s the <code>String</code> to decode
   * @param enc The name of a supported <a href="../lang/package-summary.html#charenc">character
   *     encoding</a>.
   * @return the newly decoded <code>String</code>
   * @exception UnsupportedEncodingException If character encoding needs to be consulted, but named
   *     character encoding is not supported
   * @see URLEncoder#encode(java.lang.String, java.lang.String)
   * @since 1.4
   */
  public static String decode(String s, String enc) throws UnsupportedEncodingException {

    boolean needToChange = false;
    int numChars = s.length();
    StringBuffer sb = new StringBuffer(numChars > 500 ? numChars / 2 : numChars);
    int i = 0;

    if (enc.length() == 0) {
      throw new UnsupportedEncodingException("URLDecoder: empty string enc parameter");
    }

    char c;
    byte[] bytes = null;
    while (i < numChars) {
      c = s.charAt(i);
      switch (c) {
        case '%':
          /*
           * Starting with this instance of %, process all
           * consecutive substrings of the form %xy. Each
           * substring %xy will yield a byte. Convert all
           * consecutive  bytes obtained this way to whatever
           * character(s) they represent in the provided
           * encoding.
           */

          try {

            // (numChars-i)/3 is an upper bound for the number
            // of remaining bytes
            if (bytes == null) {
              bytes = new byte[(numChars - i) / 3];
            }
            int pos = 0;

            while (((i + 2) < numChars) && (c == '%')) {
              int v = Integer.parseInt(s.substring(i + 1, i + 3), 16);
              if (v < 0) {
                throw new IllegalArgumentException(
                    "URLDecoder: Illegal "
                        + "hex characters in escape (%) pattern - negative value");
              }
              bytes[pos++] = (byte) v;
              i += 3;
              if (i < numChars) {
                c = s.charAt(i);
              }
            }

            // A trailing, incomplete byte encoding such as
            // "%x" will cause an exception to be thrown

            if ((i < numChars) && (c == '%')) {
              throw new IllegalArgumentException(
                  "URLDecoder: Incomplete trailing escape (%) pattern");
            }

            sb.append(new String(bytes, 0, pos, enc));
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "URLDecoder: Illegal hex characters in escape (%) pattern - " + e.getMessage());
          }
          needToChange = true;
          break;
        default:
          sb.append(c);
          i++;
          break;
      }
    }

    return (needToChange ? sb.toString() : s);
  }
}
