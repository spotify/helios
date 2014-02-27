package com.spotify.helios.master.http;

import com.spotify.helios.common.PomVersion;
import com.spotify.helios.common.VersionCompatibility.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static com.spotify.helios.common.Version.POM_VERSION;
import static com.spotify.helios.common.VersionCompatibility.HELIOS_SERVER_VERSION_HEADER;
import static com.spotify.helios.common.VersionCompatibility.HELIOS_VERSION_HEADER;
import static com.spotify.helios.common.VersionCompatibility.HELIOS_VERSION_STATUS_HEADER;
import static com.spotify.helios.common.VersionCompatibility.getStatus;
import static com.spotify.helios.common.VersionCompatibility.Status.INVALID;
import static com.spotify.helios.common.VersionCompatibility.Status.MISSING;

public class VersionResponseFilter implements Filter {
  private static final Logger log = LoggerFactory.getLogger(VersionResponseFilter.class);

  private static final PomVersion SERVER_VERSION = PomVersion.parse(POM_VERSION);

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    // If ever there was a valid use of goto, this would be it... if Java had it.
    if (!(request instanceof HttpServletRequest)) {
      log.debug("request is not HTTP");
      chain.doFilter(request, response);
      return;
    }

    final HttpServletRequest httpReq = (HttpServletRequest) request;
    final HttpServletResponse httpResponse = (HttpServletResponse) response;
    httpResponse.addHeader(HELIOS_SERVER_VERSION_HEADER, SERVER_VERSION.toString());

    final String header = httpReq.getHeader(HELIOS_VERSION_HEADER);
    if (header == null) {
      log.debug("No header " + HELIOS_VERSION_HEADER);
      httpResponse.addHeader(HELIOS_VERSION_STATUS_HEADER, MISSING.toString());
      chain.doFilter(request, response);
      return;
    }

    final PomVersion clientVersion;
    try {
      clientVersion = PomVersion.parse(header);
    } catch (RuntimeException e) {
      log.debug("failure to parse version header " + header);
      httpResponse.addHeader(HELIOS_VERSION_STATUS_HEADER, INVALID.toString());
      httpResponse.sendError(400, "Helios client version format is bogus - expect n.n.n");
      return;
    }

    final Status status = getStatus(SERVER_VERSION, clientVersion);
    httpResponse.addHeader(HELIOS_VERSION_STATUS_HEADER, status.toString());
    if (status == Status.INCOMPATIBLE) {
      log.debug("version " + clientVersion + " is incompatible");
      httpResponse.sendError(426, "Your client version is incompatible with the server version "
          + POM_VERSION);
    } else {
      log.debug("version " + clientVersion + " state " + status);
      chain.doFilter(request, response);
    }
  }

  @Override
  public void init(FilterConfig arg0) throws ServletException {}

  @Override
  public void destroy() {}
}
