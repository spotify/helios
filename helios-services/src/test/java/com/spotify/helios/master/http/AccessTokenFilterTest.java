package com.spotify.helios.master.http;

import com.spotify.helios.authentication.HttpAuthenticator;
import com.spotify.helios.common.VersionCompatibility;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 * Would feel nicer to have full stubs for the request/response object than mockito mocks, as in
 * some cases the test shouldn't really care if ServletResponse.sendError(int) or
 * ServletResponse.setStatus(int) is called.
 */
public class AccessTokenFilterTest {

  public static final String TOKEN_HEADER = "AccessToken";
  private final HttpAuthenticator authenticator = mock(HttpAuthenticator.class);

  private final HttpServletRequest request = mock(HttpServletRequest.class);
  private final HttpServletResponse response = mock(HttpServletResponse.class);
  private final FilterChain filterChain = mock(FilterChain.class);

  private AccessTokenFilter authenticateAllFilter = new AccessTokenFilter(authenticator, "all");
  private AccessTokenFilter authenticateSomeFilter = new AccessTokenFilter(authenticator, "1.0.0");

  @Before
  public void setUp() {
    when(authenticator.getHttpAuthHeaderKey()).thenReturn(TOKEN_HEADER);
  }

  private void verifyNoTokenResponse() throws Exception {
    verify(response).sendError(401);
    verify(authenticator, never()).verifyToken(anyString(), anyString());
    verify(filterChain, never()).doFilter(request, response);
  }

  private void verifyInvalidTokenResponse() throws Exception {
    verify(response).sendError(401);
    verify(filterChain, never()).doFilter(request, response);
  }

  private void verifyValidTokenResponse() throws IOException, ServletException {
    verifyZeroInteractions(response);
    verify(filterChain).doFilter(request, response);
  }

  @Test
  public void clientVersionBelowVersionCheck_NotAuthenticated() throws Exception {
    when(request.getHeader(VersionCompatibility.HELIOS_VERSION_HEADER)).thenReturn("0.9.0");

    authenticateSomeFilter.doFilter(request, response, filterChain);

    verifyZeroInteractions(response);
    verify(authenticator, never()).verifyToken(anyString(), anyString());
    verify(filterChain).doFilter(request, response);
  }

  @Test
  public void clientVersionAtVersionCheck_NoToken() throws Exception {
    when(request.getHeader(VersionCompatibility.HELIOS_VERSION_HEADER)).thenReturn("1.0.0");

    authenticateSomeFilter.doFilter(request, response, filterChain);

    verifyNoTokenResponse();
  }

  @Test
  public void clientVersionAtVersionCheck_ValidToken() throws Exception {
    when(request.getHeader(VersionCompatibility.HELIOS_VERSION_HEADER)).thenReturn("2.0.0");
    when(request.getHeader(TOKEN_HEADER)).thenReturn("foobar");

    when(authenticator.verifyToken(null, "foobar")).thenReturn(true);

    authenticateSomeFilter.doFilter(request, response, filterChain);

    verifyValidTokenResponse();
  }

  @Test
  public void clientVersionAtVersionCheck_InvalidToken() throws Exception {
    when(request.getHeader(VersionCompatibility.HELIOS_VERSION_HEADER)).thenReturn("1.0.1");
    when(request.getHeader(TOKEN_HEADER)).thenReturn("batman");

    when(authenticator.verifyToken(null, "batman")).thenReturn(false);

    authenticateSomeFilter.doFilter(request, response, filterChain);

    verifyInvalidTokenResponse();
  }

  @Test
  public void testRequireForAllVersions_NoToken() throws Exception {
    when(request.getHeader(VersionCompatibility.HELIOS_VERSION_HEADER)).thenReturn("1.2.3");

    authenticateAllFilter.doFilter(request, response, filterChain);

    verifyNoTokenResponse();
  }

  @Test
  public void testRequireForAllVersions_InvalidToken() throws Exception {
    when(request.getHeader(VersionCompatibility.HELIOS_VERSION_HEADER)).thenReturn("1.2.3");
    when(request.getHeader(TOKEN_HEADER)).thenReturn("abc123");

    when(authenticator.verifyToken(null, "abc123")).thenReturn(false);

    authenticateAllFilter.doFilter(request, response, filterChain);

    verifyInvalidTokenResponse();
  }

  @Test
  public void testRequireForAllVersions_ValidToken() throws Exception {
    when(request.getHeader(TOKEN_HEADER)).thenReturn("my-token");
    when(request.getHeader(VersionCompatibility.HELIOS_VERSION_HEADER)).thenReturn("1.2.3");

    when(authenticator.verifyToken(null, "my-token")).thenReturn(true);

    authenticateAllFilter.doFilter(request, response, filterChain);

    verifyValidTokenResponse();
  }
}