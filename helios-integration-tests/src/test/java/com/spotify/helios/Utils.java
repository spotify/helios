package com.spotify.helios;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.databind.JsonNode;
import com.spotify.helios.cli.CliMain;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.testing.Prober;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.fasterxml.jackson.databind.node.JsonNodeType.STRING;
import static java.util.Arrays.asList;

public class Utils {

  public static final String DEFAULT_IMAGE_INFO_PATH = "../helios-services/target/test-classes/";

  public static <T> T cli(final Class<T> klass, final String masterEndpoint, final String... args)
      throws Exception {
    return cli(klass, masterEndpoint, asList(args));
  }

  private static <T> T cli(final Class<T> klass, final String masterEndpoint,
                           final List<String> args) throws Exception {
    final ImmutableList<String> argList = new ImmutableList.Builder<String>()
        .add("-z")
        .add(masterEndpoint)
        .add("--json")
        .addAll(args)
        .build();

    return Json.read(main(argList).toString(), klass);
  }

  public static ByteArrayOutputStream main(final List<String> args) throws Exception {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final ByteArrayOutputStream err = new ByteArrayOutputStream();
    final CliMain main = new CliMain(new PrintStream(out), new PrintStream(err),
                                     args.toArray(new String[args.size()]));
    main.run();
    return out;
  }

  public static String masterImage() throws IOException {
    final String path = System.getProperty("masterImage",
                                           DEFAULT_IMAGE_INFO_PATH + "master-image.json");
    return imageInfo(path);
  }

  public static String agentImage() throws IOException {
    final String path = System.getProperty("agentImage",
                                           DEFAULT_IMAGE_INFO_PATH + "agent-image.json");
    return imageInfo(path);
  }

  public static String soloImage() throws IOException {
    final String path = System.getProperty("soloImage",
                                           DEFAULT_IMAGE_INFO_PATH + "solo-image.json");
    return imageInfo(path);
  }

  private static String imageInfo(final String path) throws IOException {
    final String json = new String(Files.readAllBytes(Paths.get(path)));
    final JsonNode node = Json.readTree(json);
    final JsonNode imageNode = node.get("image");
    return (imageNode == null || imageNode.getNodeType() != STRING) ? null : imageNode.asText();
  }

  public static class AgentStatusProber implements Prober {

    private final HeliosClient client;
    private final String hostName;

    public AgentStatusProber(final String masterEndpoint, final String user,
                             final String hostName) {
      this.hostName = hostName;
      client = HeliosClient.newBuilder()
          .setEndpoints(masterEndpoint)
          .setUser(user)
          .build();
    }

    @Override
    public boolean probe(String host, int port) {
      try {
        final HostStatus hostStatus = client.hostStatus(hostName).get(10, TimeUnit.SECONDS);
        return hostStatus != null && hostStatus.getStatus() == HostStatus.Status.UP;
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        throw Throwables.propagate(e);
      }
    }
  }

}
