/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;

import com.spotify.helios.common.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.AgentStatus;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.spotify.helios.cli.Utils.allAsMap;

public class HostStatusCommand extends ControlCommand {

  private final Argument hostsArg;

  public HostStatusCommand(final Subparser parser) {
    super(parser);

    parser.help("show host status");

    hostsArg = parser.addArgument("hosts")
        .nargs("+")
        .help("");
  }

  @Override
  int run(final Namespace options, final HeliosClient client, final PrintStream out, final boolean json)
      throws ExecutionException, InterruptedException {

    List<String> hosts = options.getList(hostsArg.getDest());

    if (hosts.isEmpty()) {
      hosts = client.listAgents().get();
    }

    final Map<String, ListenableFuture<AgentStatus>> futures = Maps.newHashMap();
    for (final String host : hosts) {
      futures.put(host, client.agentStatus(host));
    }

    final Map<String, AgentStatus> statuses = allAsMap(futures);

    out.println(Json.asPrettyStringUnchecked(statuses));

    return 0;
  }
}
