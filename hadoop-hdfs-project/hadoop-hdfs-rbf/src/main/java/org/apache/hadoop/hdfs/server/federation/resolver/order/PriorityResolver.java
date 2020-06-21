package org.apache.hadoop.hdfs.server.federation.resolver.order;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hdfs.server.federation.resolver.PathLocation;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class PriorityResolver extends HashFirstResolver {
  private static final Logger LOG =
      LoggerFactory.getLogger(PriorityResolver.class);

  public PriorityResolver() {
  }

  @Override
  public String getFirstNamespace(String path, PathLocation loc) {
    if (CollectionUtils.isEmpty(loc.getDestinations())) {
      LOG.error("Cannot get destination for {}", loc);
      return null;
    }
    List<RemoteLocation> remoteLocations =
        new LinkedList<>(loc.getDestinations());
    remoteLocations.sort(Collections.reverseOrder());
    List<RemoteLocation> highPriRLoc = new LinkedList<>();
    RemoteLocation preRLoc = null;
    for (RemoteLocation rLoc : remoteLocations) {
      if (preRLoc == null || rLoc.getPriority() == preRLoc.getPriority()) {
        preRLoc = rLoc;
        highPriRLoc.add(rLoc);
      } else {
        break;
      }
    }
    PathLocation highPriPathLocation =
        new PathLocation(loc.getSourcePath(), highPriRLoc);
    return super.getFirstNamespace(path, highPriPathLocation);
  }
}