package com.spotify.helios.agent.docker;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;

public class LogsResponseReader implements MessageBodyReader<LogStream> {

  @Override
  public boolean isReadable(final Class<?> type, final Type genericType,
                            final Annotation[] annotations,
                            final MediaType mediaType) {
    return type == LogStream.class;
  }

  @Override
  public LogStream readFrom(final Class<LogStream> type, final Type genericType,
                            final Annotation[] annotations,
                            final MediaType mediaType,
                            final MultivaluedMap<String, String> httpHeaders,
                            final InputStream entityStream)
      throws IOException, WebApplicationException {
    return new LogStream(entityStream);
  }
}
