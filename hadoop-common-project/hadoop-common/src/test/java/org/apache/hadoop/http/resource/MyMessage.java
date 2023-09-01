package org.apache.hadoop.http.resource;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/")
public class MyMessage {

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public String getMessage() {

    return "My message\n";
  }
}
