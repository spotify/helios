/**
 * Copyright (C) 2012 Spotify AB
 */

package com.spotify.helios.agent;

import com.spotify.helios.common.LoggingConfig;
import com.spotify.helios.servicescommon.ServiceMain;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.json.ObjectMapperFactory;
import com.yammer.dropwizard.validation.Validator;

import net.sourceforge.argparse4j.inf.ArgumentParserException;

/**
 * Instantiates and runs helios agent.
 */
public class AgentMain extends ServiceMain {

  private final AgentConfig agentConfig;
  private AgentService service;

  public AgentMain(final String... args) throws ArgumentParserException {
    this(new AgentParser(args));
  }

  public AgentMain(final AgentParser parser) {
    this(parser.getAgentConfig(), parser.getLoggingConfig());
  }

  public AgentMain(final AgentConfig agentConfig, final LoggingConfig loggingConfig) {
    super(loggingConfig, agentConfig.getSentryDsn());
    this.agentConfig = agentConfig;
  }

  @Override
  protected void startUp() throws Exception {
    final Environment environment = new Environment("helios-agent", agentConfig,
        new ObjectMapperFactory(), new Validator());
    service = new AgentService(agentConfig, environment);
    service.startAsync().awaitRunning();
  }

  @Override
  protected void shutDown() throws Exception {
    if (service != null) {
      service.stopAsync().awaitTerminated();
    }
  }

  public static void main(final String... args) {
    try {
      final AgentMain main = new AgentMain(args);
      main.startAsync();
      main.awaitTerminated();
    } catch (Throwable e) {
      e.printStackTrace();
      System.exit(1);
    }
    // Ensure we exit even if there's lingering non-daemon threads
    System.exit(0);
  }

}
