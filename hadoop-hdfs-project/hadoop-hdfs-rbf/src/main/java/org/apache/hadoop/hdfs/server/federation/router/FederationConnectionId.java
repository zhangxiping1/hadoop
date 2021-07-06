/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.federation.router;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.Client.ConnectionId;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Extends the {@link ConnectionId} to support multiple connections from the
 * {@link Router} to the {@link org.apache.hadoop.hdfs.server.namenode.NameNode}.
 */
public class FederationConnectionId extends ConnectionId {

  private int connectionIndex;
  private static final int PRIME = 16777619;
  FederationConnectionId(InetSocketAddress address, Class<?> protocol,
                         UserGroupInformation ticket, int rpcTimeout,
                         RetryPolicy connectionRetryPolicy, Configuration conf, int index) {
    super(address, protocol, ticket, rpcTimeout, connectionRetryPolicy, conf);
    this.connectionIndex = index;
  }

  @Override
  public int hashCode() {
    int hashCode = super.hashCode();
    return PRIME * hashCode + connectionIndex;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    if (obj instanceof FederationConnectionId) {
      FederationConnectionId other = (FederationConnectionId)obj;
      if (this.connectionIndex == other.connectionIndex) {
        return true;
      }
    }
    return false;
  }
}