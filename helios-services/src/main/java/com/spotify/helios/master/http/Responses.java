package com.spotify.helios.master.http;

import javax.ws.rs.WebApplicationException;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.status;

public class Responses {

  public static WebApplicationException badRequest(final Object entity) {
    return new WebApplicationException(status(BAD_REQUEST).entity(entity).build());
  }

  public static WebApplicationException badRequest() {
    return new WebApplicationException(BAD_REQUEST);
  }

  public static WebApplicationException notFound(final Object entity) {
    return new WebApplicationException(status(NOT_FOUND).entity(entity).build());
  }

  public static WebApplicationException notFound() {
    return new WebApplicationException(NOT_FOUND);
  }
}
