/*-
 * -\-\-
 * Helios Services
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.helios.servicescommon;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Can read a resolv.conf file and tell you the domain.
 */
public class ResolverConfReader {
  private static final Logger log = LoggerFactory.getLogger(ResolverConfReader.class);

  /**
   * Looks in file to find the domain setting.
   *
   * @param file path to resolver config file
   *
   * @return the domain specified there, or empty string if any failure
   */
  public static String getDomainFromResolverConf(final String file) {
    try (final InputStream in = new FileInputStream(file)) {
      final InputStreamReader isr = new InputStreamReader(in);
      try (final BufferedReader br = new BufferedReader(isr)) {
        String line;
        while ((line = br.readLine()) != null) {
          if (line.startsWith("domain")) {
            final StringTokenizer st = new StringTokenizer(line);
            st.nextToken(); /* skip "domain" */
            if (!st.hasMoreTokens()) {
              continue;
            }
            return st.nextToken();
          }
        }
      }
    } catch (FileNotFoundException e) {
      log.warn("Resolver config file not found", e);
    } catch (IOException e) {
      log.warn("Error reading config file", e);
    }
    return "";
  }

}
