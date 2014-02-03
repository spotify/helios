package com.spotify.helios.master.resources;

import com.spotify.helios.common.HeliosException;
import com.spotify.helios.master.MasterModel;
import com.yammer.metrics.annotation.ExceptionMetered;
import com.yammer.metrics.annotation.Timed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/masters")
public class MastersResource {

  private static final Logger log = LoggerFactory.getLogger(MastersResource.class);

  private final MasterModel model;

  public MastersResource(final MasterModel model) {
    this.model = model;
  }

  @GET
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public List<String> list() throws HeliosException {
    return model.getRunningMasters();
  }
}
