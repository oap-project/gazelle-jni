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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

/**
 * Code is copied from <a
 * href="https://stackoverflow.com/questions/3923129/get-a-list-of-resources-from-classpath-directory">here</a>
 * and then modified for Gluten's use.
 */
public class ResourceUtil {

  private static final Logger LOG = LoggerFactory.getLogger(ResourceUtil.class);

  /**
   * Get a collection of resource paths by the input RegEx pattern.
   *
   * @param pattern The pattern to match.
   * @return The relative resource paths in the order they are found.
   */
  public static List<String> getResources(final Pattern pattern) {
    final List<String> buffer = new ArrayList<>();
    final String classPath = System.getProperty("java.class.path");
    final String[] classPathElements = classPath.split(File.pathSeparator);
    for (final String element : classPathElements) {
      getResources(element, pattern, buffer);
    }
    return Collections.unmodifiableList(buffer);
  }

  private static void getResources(
      final String element, final Pattern pattern, final List<String> buffer) {
    final File file = new File(element);
    if (!file.exists()) {
      LOG.info("Skip non-existing classpath: {}", element);
      return;
    }
    if (file.isDirectory()) {
      getResourcesFromDirectory(file, file, pattern, buffer);
    } else {
      getResourcesFromJarFile(file, pattern, buffer);
    }
  }

  private static void getResourcesFromJarFile(
      final File file, final Pattern pattern, final List<String> buffer) {
    ZipFile zf;
    try {
      zf = new ZipFile(file);
    } catch (final ZipException e) {
      throw new RuntimeException(e);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    final Enumeration e = zf.entries();
    while (e.hasMoreElements()) {
      final ZipEntry ze = (ZipEntry) e.nextElement();
      final String fileName = ze.getName();
      final boolean accept = pattern.matcher(fileName).matches();
      if (accept) {
        buffer.add(fileName);
      }
    }
    try {
      zf.close();
    } catch (final IOException e1) {
      throw new RuntimeException(e1);
    }
  }

  private static void getResourcesFromDirectory(
      final File root, final File directory, final Pattern pattern, final List<String> buffer) {
    final File[] fileList = directory.listFiles();
    for (final File file : fileList) {
      if (file.isDirectory()) {
        getResourcesFromDirectory(root, file, pattern, buffer);
      } else {
        final String relative = root.toURI().relativize(file.toURI()).getPath();
        final boolean accept = pattern.matcher(relative).matches();
        if (accept) {
          buffer.add(relative);
        }
      }
    }
  }
}
