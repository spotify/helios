package com.spotify.helios.agent;

import org.eclipse.jetty.continuation.Continuation;
import org.eclipse.jetty.continuation.ContinuationSupport;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.io.IOException;

import javax.servlet.Servlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class BadDockerServer {
  private final Server server;

  public static class ForwardingServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
      // cause this to essentially sleep for eternity - Snow White mode if you will.
      Continuation continuation = ContinuationSupport.getContinuation(req);
      continuation.suspend();
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
      // cause this to essentially sleep for eternity - Snow White mode if you will.
      Continuation continuation = ContinuationSupport.getContinuation(req);
      continuation.suspend();
    }
  }

  public BadDockerServer(int port) {
    Servlet s = new ForwardingServlet();
    server = new Server(port);
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.addServlet(new ServletHolder(s), "/*");

    HandlerList handlers = new HandlerList();
    handlers.setHandlers(new Handler[]{context});
    server.setHandler(handlers);

  }

  public static void main(String[] args) throws Exception {
    new BadDockerServer(8080).start();
  }

  public void start() throws Exception {
    server.start();
  }

  public void stop() throws Exception {
    server.stop();
  }
}
