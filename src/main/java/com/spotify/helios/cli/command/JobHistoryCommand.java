package com.spotify.helios.cli.command;

import com.google.common.base.Preconditions;

import com.spotify.helios.cli.Table;
import com.spotify.helios.common.Client;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobIdParseException;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.TaskStatus.State;
import com.spotify.helios.common.protocol.JobStatusEvent;
import com.spotify.helios.common.protocol.JobStatusEvents;

import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.spotify.helios.cli.Output.table;

public class JobHistoryCommand extends ControlCommand {

  private static final class EventComparator implements Comparator<JobStatusEvent> {
    @Override
    public int compare(JobStatusEvent arg0, JobStatusEvent arg1) {
      if (arg1.getTimestamp() > arg0.getTimestamp()) {
        return -1;
      } else if (arg1.getTimestamp() == arg0.getTimestamp()) {
        return 0;
      } else {
        return 1;
      }
    }
  }

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

    JobId jobId;
    String jobIdString = options.getString(jobIdArg.getDest());
    try {
      jobId = JobId.parse(jobIdString);
    } catch (JobIdParseException e) {
      if (!json) {
        out.println("Invalid job id: " + jobIdString);
      }
      System.err.println("Invalid job id: " + jobIdString);
      return 1;
    }

    JobStatusEvents result = client.jobHistory(jobId).get();
    if (json) {
      out.println(Json.asPrettyStringUnchecked(result));
      return 0;
    }

    final Table table = table(out);
    table.row("AGENT", "TIMESTAMP", "STATE", "CONTAINERID");
    List<JobStatusEvent> events = result.getEvents();
    DateTimeFormatter format = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss.SSS");

    Collections.sort(events, new EventComparator());

    for (JobStatusEvent event : events) {
      String agent = checkNotNull(event.getAgent());
      long timestamp = checkNotNull(event.getTimestamp());
      TaskStatus status = checkNotNull(event.getStatus());
      State state = checkNotNull(status.getState());
      String containerId = status.getContainerId();
      containerId = containerId == null ? "<none>" : containerId;

      table.row(agent, format.print(timestamp), state, containerId);
    }
    table.print();
    return 0;
  }
}
