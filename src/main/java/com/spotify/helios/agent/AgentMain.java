/**
 * Copyright (C) 2012 Spotify AB
 */

package com.spotify.helios.agent;

import com.spotify.helios.common.LoggingConfig;
import com.spotify.helios.common.ServiceMain;
import net.sourceforge.argparse4j.inf.ArgumentParserException;

/**
 * Instantiates and runs helios agent.
 */
public class AgentMain extends ServiceMain {

  private final AgentConfig agentConfig;
  private AgentService service;

  public AgentMain(final String[] args) throws ArgumentParserException {
    this(new AgentParser(args));
  }

  public AgentMain(final AgentParser parser) {
    this(parser.getAgentConfig(), parser.getLoggingConfig());
  }

  public AgentMain(final AgentConfig agentConfig, final LoggingConfig loggingConfig) {
    super(loggingConfig);
    this.agentConfig = agentConfig;
  }

  @Override
  protected void startUp() throws Exception {
    service = new AgentService(agentConfig);
    service.start();
  }

  @Override
  protected void shutDown() throws Exception {
    service.stop();
  }

  public static void main(final String[] args) {
    try {
      final AgentMain main = new AgentMain(args);
      main.startAsync();
      main.awaitTerminated();
    } catch (Throwable e) {
      System.exit(1);
    }
    // Ensure we exit even if there's lingering non-daemon threads
    System.exit(0);
  }

}
