package com.spotify.helios.agent;

import com.spotify.docker.DockerClient;
import com.spotify.docker.DockerException;
import com.spotify.docker.DockerTimeoutException;
import com.spotify.helios.servicescommon.RiemannFacade;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public abstract class MonitoredDockerClient {

  private MonitoredDockerClient() {
  }

  public static DockerClient wrap(RiemannFacade riemann, final DockerClient client) {
    return (DockerClient) Proxy.newProxyInstance(
        MonitoredDockerClient.class.getClassLoader(),
        new Class[]{DockerClient.class},
        new MonitoringInvocationHandler(riemann, client));
  }

  private static class MonitoringInvocationHandler implements InvocationHandler {

    private final RiemannFacade riemann;
    private final DockerClient client;

    public MonitoringInvocationHandler(final RiemannFacade riemann, final DockerClient client) {
      this.riemann = riemann;

      this.client = client;
    }

    @Override
    public Object invoke(final Object proxy, final Method method, final Object[] args)
        throws Throwable {
      try {
        return method.invoke(client, args);
      } catch (InvocationTargetException e) {
        if (e.getCause() instanceof DockerException) {
          final String tag;
          if (e.getCause() instanceof DockerTimeoutException) {
            tag = "timeout";
          } else {
            tag = "error";
          }
          riemann.event()
              .service("helios-agent/docker")
              .tags("docker", tag, method.getName())
              .send();
        }
        throw e.getCause();
      }
    }
}
}
