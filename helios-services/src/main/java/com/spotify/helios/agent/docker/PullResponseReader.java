package com.spotify.helios.agent.docker;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;

public class PullResponseReader implements MessageBodyReader<ImagePull> {

  @Override
  public boolean isReadable(final Class<?> type, final Type genericType,
                            final Annotation[] annotations,
                            final MediaType mediaType) {
    return type == ImagePull.class;
  }

  @Override
  public ImagePull readFrom(final Class<ImagePull> type, final Type genericType,
                            final Annotation[] annotations,
                            final MediaType mediaType,
                            final MultivaluedMap<String, String> httpHeaders,
                            final InputStream entityStream)
      throws IOException, WebApplicationException {
    return new ImagePull(entityStream);
  }
}
