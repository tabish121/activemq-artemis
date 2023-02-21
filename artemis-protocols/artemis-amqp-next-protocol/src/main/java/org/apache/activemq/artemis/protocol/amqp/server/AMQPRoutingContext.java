/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.protocol.amqp.server;

import org.apache.activemq.artemis.core.server.routing.RoutingContext;
import org.apache.activemq.artemis.protocol.amqp.AMQPRemotingConnection;
import org.apache.qpid.protonj2.engine.Connection;

public class AMQPRoutingContext extends RoutingContext {

   private final Connection protonConnection;

   public Connection getProtonConnection() {
      return protonConnection;
   }

   @Override
   public AMQPRemotingConnection getConnection() {
      return getConnection();
   }

   public AMQPRoutingContext(AMQPRemotingConnection remotingConnection, Connection protonConnection) {
      super(remotingConnection,
            remotingConnection.getClientID(),
            remotingConnection.getSASLResult() != null ? remotingConnection.getSASLResult().getUsername() : null);

      this.protonConnection = protonConnection;
   }
}
