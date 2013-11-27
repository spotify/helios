package com.spotify.helios.cli.command;

import com.spotify.helios.cli.Table;
import com.spotify.helios.common.Client;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.TaskStatus.State;
import com.spotify.helios.common.protocol.TaskStatusEvent;
import com.spotify.helios.common.protocol.TaskStatusEvents;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.getLast;
import static com.spotify.helios.cli.Output.table;

public class JobHistoryCommand extends ControlCommand {

  private final Argument jobIdArg;

  public JobHistoryCommand(Subparser parser) {
    super(parser);

    parser.help("see the run history of a job");

    jobIdArg = parser.addArgument("jobid")
         .help("Job id");
  }

  @Override
  int run(Namespace options, Client client, PrintStream out, boolean json)
      throws ExecutionException, InterruptedException {

    String jobIdString = options.getString(jobIdArg.getDest());

    final Map<JobId, Job> jobs = client.jobs(jobIdString).get();

    if (jobs.size() == 0) {
      out.printf("Unknown job: %s%n", jobIdString);
      return 1;
    } else if (jobs.size() > 1) {
      out.printf("Ambiguous job id: %s%n", jobIdString);
      return 1;
    }

    final JobId jobId = getLast(jobs.keySet());

    TaskStatusEvents result = client.jobHistory(jobId).get();

    if (json) {
      out.println(Json.asPrettyStringUnchecked(result));
      return 0;
    }

    final Table table = table(out);
    table.row("AGENT", "TIMESTAMP", "STATE", "THROTTLED", "CONTAINERID");
    List<TaskStatusEvent> events = result.getEvents();
    DateTimeFormatter format = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss.SSS");

    for (TaskStatusEvent event : events) {
      String agent = checkNotNull(event.getAgent());
      long timestamp = checkNotNull(event.getTimestamp());
      TaskStatus status = checkNotNull(event.getStatus());
      State state = checkNotNull(status.getState());
      String containerId = status.getContainerId();
      containerId = containerId == null ? "<none>" : containerId;

      table.row(agent, format.print(timestamp), state, status.getThrottled(), containerId);
    }
    table.print();
    return 0;
  }
}
