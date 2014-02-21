package com.spotify.helios.servicescommon;

import com.spotify.helios.agent.AgentParser;
import com.spotify.helios.common.LoggingConfig;
import com.spotify.helios.master.MasterParser;
import com.spotify.logging.LoggingConfigurator;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SentryTest {

  private static final Logger log = LoggerFactory.getLogger(SentryTest.class);
  private static final int SENTRY_PORT = 9876;
  private static final int UDP_SERVER_TIMEOUT = 5000;
  private static final String TEST_DSN = String.format("udp://1:1@localhost:%d/1", SENTRY_PORT);

  @Test
  public void testMasterParserAndConfig() throws Exception {
    final String dsn = new MasterParser("--sentry-dsn", TEST_DSN).getMasterConfig().getSentryDsn();
    assertEquals("wrong sentry DSN", TEST_DSN, dsn);
  }

  @Test
  public void testAgentParserAndConfig() throws Exception {
    final String dsn = new AgentParser("--sentry-dsn", TEST_DSN).getAgentConfig().getSentryDsn();
    assertEquals("wrong sentry DSN", TEST_DSN, dsn);
  }

  @Test
  public void testSentryAppender() throws Exception {
    // start our UDP server which will receive sentry messages
    final UdpServer udpServer = new UdpServer(SENTRY_PORT);
    // turn on logging which enables the sentry appender
    final LoggingConfig config = new LoggingConfig(0, true, null, false);
    ServiceMain.setupLogging(config, TEST_DSN);
    // log a message at error level so sentry appender sends it to UDP server
    log.error("Ignore test message printed by Helios SentryTest");
    // be nice and turn logging back off
    LoggingConfigurator.configureNoLogging();
    // make sure we got the message as expected, note that getMessage is a blocking method
    final String message = udpServer.getMessage();
    assertTrue("Expected message beginning with 'Sentry', instead got " + message,
               message.startsWith("Sentry"));
  }

  /**
   * Simple Udp server which will listen on the specified port when constructed. It stores the
   * first message it receives, and then stops listening for more messages. The message can be
   * retrieved using the getMessage method. Both run and getMessage are synchronized so that
   * getMessage will block until the run method has exited, meaning we've either received a message
   * or the receive operation has timed out.
   */
  private static class UdpServer implements Runnable {

    private final DatagramSocket serverSocket;
    private String message;

    private UdpServer(int port) throws SocketException, InterruptedException {
      serverSocket = new DatagramSocket(port);
      serverSocket.setSoTimeout(UDP_SERVER_TIMEOUT);
      final Thread thread = new Thread(this);
      thread.setDaemon(true);
      thread.start();
    }

    @Override
    public synchronized void run() {
      final byte[] data = new byte[1024];
      final DatagramPacket packet = new DatagramPacket(data, data.length);
      try {
        serverSocket.receive(packet);
        message = new String(packet.getData()).trim();
      } catch (IOException e) {
        message = "Exception while receiving sentry call. " + e.getMessage();
      }
    }

    public synchronized String getMessage() throws InterruptedException {
      return message;
    }
  }

}
