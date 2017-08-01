/*-
 * -\-\-
 * Helios Tools
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

package com.spotify.helios.cli.command;

import static com.google.common.base.CharMatcher.WHITESPACE;
import static com.google.common.base.Joiner.on;
import static com.spotify.helios.cli.Output.table;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.spotify.helios.cli.Table;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import java.io.BufferedReader;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

public class JobListCommand extends ControlCommand {

  private final Argument quietArg;
  private final Argument patternArg;
  private final Argument fullArg;
  private final Argument deployedArg;

  public JobListCommand(final Subparser parser) {
    super(parser);

    parser.help("list jobs");

    patternArg = parser.addArgument("pattern")
        .nargs("?")
        .help("Job reference to filter on");

    fullArg = parser.addArgument("-f")
        .action(storeTrue())
        .help("Print full job id's.");

    quietArg = parser.addArgument("-q")
        .action(storeTrue())
        .help("only print job id's");

    deployedArg = parser.addArgument("-y")
        .action(storeTrue())
        .help("only show deployed jobs");
  }

  @Override
  int run(final Namespace options, final HeliosClient client, final PrintStream out,
          final boolean json, final BufferedReader stdin)
      throws ExecutionException, InterruptedException {
    final boolean full = options.getBoolean(fullArg.getDest());
    final boolean quiet = options.getBoolean(quietArg.getDest());
    final String pattern = options.getString(patternArg.getDest());
    final boolean deployed = options.getBoolean(deployedArg.getDest());

    final Map<JobId, Job> jobs;
    if (pattern == null) {
      jobs = client.jobs().get();
    } else {
      jobs = client.jobs(pattern).get();
    }

    if (!Strings.isNullOrEmpty(pattern) && jobs.isEmpty()) {
      if (json) {
        out.println(Json.asPrettyStringUnchecked(jobs));
      } else if (!quiet) {
        out.printf("job pattern %s matched no jobs%n", pattern);
      }
      return 1;
    }

    final Map<JobId, JobStatus> jobStatuses = getJobStatuses(client, jobs, deployed);

    final Set<JobId> sortedJobIds = Sets.newTreeSet(jobStatuses.keySet());

    if (json) {
      if (quiet) {
        out.println(Json.asPrettyStringUnchecked(sortedJobIds));
      } else {
        final Map<JobId, Job> filteredJobs = Maps.newHashMap();
        for (final Entry<JobId, Job> entry : jobs.entrySet()) {
          if (jobStatuses.containsKey(entry.getKey())) {
            filteredJobs.put(entry.getKey(), entry.getValue());
          }
        }
        out.println(Json.asPrettyStringUnchecked(filteredJobs));
      }
    } else {
      if (quiet) {
        for (final JobId jobId : sortedJobIds) {
          out.println(jobId);
        }
      } else {
        final Table table = table(out);
        table.row("JOB ID", "NAME", "VERSION", "HOSTS", "COMMAND", "ENVIRONMENT");

        for (final JobId jobId : sortedJobIds) {
          final Job job = jobs.get(jobId);
          final String command = on(' ').join(escape(job.getCommand()));
          final String env = Joiner.on(" ").withKeyValueSeparator("=").join(job.getEnv());
          final JobStatus status = jobStatuses.get(jobId);
          table.row(full ? jobId : jobId.toShortString(), jobId.getName(), jobId.getVersion(),
              status != null ? status.getDeployments().keySet().size() : 0,
              command, env);
        }
        table.print();
      }
    }

    return 0;
  }

  private Map<JobId, JobStatus> getJobStatuses(
      final HeliosClient client,
      final Map<JobId, Job> jobs,
      final boolean deployed)
      throws InterruptedException, ExecutionException {

    final Map<JobId, JobStatus> jobStatuses = client.jobStatuses(jobs.keySet()).get();

    // maybe filter on deployed jobs
    final Map<JobId, JobStatus> filteredJobStatuses = Maps.newHashMap();
    if (!deployed) {
      filteredJobStatuses.putAll(jobStatuses);
    } else {
      for (final Entry<JobId, JobStatus> e : jobStatuses.entrySet()) {
        if (!e.getValue().getDeployments().isEmpty()) {
          filteredJobStatuses.put(e.getKey(), e.getValue());
        }
      }
    }
    return filteredJobStatuses;
  }

  private static List<String> escape(final List<String> command) {
    final List<String> escaped = Lists.newArrayList();
    for (final String s : command) {
      escaped.add(escape(s));
    }
    return escaped;
  }

  private static String escape(final String arg) {
    return WHITESPACE.matchesAnyOf(arg) ? '"' + arg + '"' : arg;
  }
}

