package com.spotify.helios.agent.docker;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

@Provider
@Produces(MediaType.APPLICATION_JSON)
public class ObjectMapperProvider implements ContextResolver<ObjectMapper> {

  private static final Function<? super Object, ? extends Object> VOID_VALUE =
      new Function<Object, Object>() {
        @Override
        public Object apply(final Object input) {
          return null;
        }
      };

  private static final SimpleModule MODULE = new SimpleModule();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    MODULE.addSerializer(Set.class, new SetSerializer());
    MODULE.addDeserializer(Set.class, new SetDeserializer());
    OBJECT_MAPPER.registerModule(MODULE);
    OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    OBJECT_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
  }


  @Override
  public ObjectMapper getContext(Class<?> type) {
    return OBJECT_MAPPER;
  }

  private static class SetSerializer extends JsonSerializer<Set> {

    @Override
    public void serialize(final Set value, final JsonGenerator jgen,
                          final SerializerProvider provider)
        throws IOException {
      final Map map = (value == null) ? null : Maps.asMap(value, VOID_VALUE);
      OBJECT_MAPPER.writeValue(jgen, map);
    }
  }

  private static class SetDeserializer extends JsonDeserializer<Set> {

    @Override
    public Set<?> deserialize(final JsonParser jp, final DeserializationContext ctxt)
        throws IOException {
      final Map map = OBJECT_MAPPER.readValue(jp, Map.class);
      return (map == null) ? null : map.keySet();
    }
  }
}
