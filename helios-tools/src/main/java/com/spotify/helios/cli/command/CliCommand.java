/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.helios.cli.command;

import com.spotify.helios.cli.Target;

import net.sourceforge.argparse4j.inf.Namespace;

import java.io.BufferedReader;
import java.io.PrintStream;
import java.util.List;

public interface CliCommand {
  int run(final Namespace options, final List<Target> targets, final PrintStream out,
          final PrintStream err, final String username, final boolean json,
          final BufferedReader stdin)
      throws Exception;
}
