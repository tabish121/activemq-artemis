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

import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.routing.RoutingHandler;
import org.apache.activemq.artemis.protocol.amqp.AMQPRemotingConnection;
import org.apache.activemq.artemis.protocol.amqp.support.AMQPConnectionConstants;
import org.apache.activemq.artemis.utils.ConfigurationHelper;
import org.apache.qpid.protonj2.engine.Connection;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.transport.ConnectionError;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class AMQPRoutingHandler extends RoutingHandler<AMQPRoutingContext> {

   public AMQPRoutingHandler(ActiveMQServer server) {
      super(server);
   }

   public boolean route(AMQPRemotingConnection remoteingConnection, Connection connection) throws Exception {
      return route(new AMQPRoutingContext(remoteingConnection, connection));
   }

   @Override
   protected void refuse(AMQPRoutingContext context) {
      final String errorDescription;

      switch (context.getResult().getStatus()) {
         case REFUSED_USE_ANOTHER:
            errorDescription = String.format("Connection router %s rejected this connection", context.getRouter());
            break;
         case REFUSED_UNAVAILABLE:
            errorDescription = String.format("Connection router %s is not ready", context.getRouter());
            break;
         default:
            errorDescription = String.format("Connection router %s did not provide a reason", context.getRouter());
            break;
      }

      final ErrorCondition error = new ErrorCondition(ConnectionError.CONNECTION_FORCED, errorDescription);
      final Connection protonConnection = context.getProtonConnection();

      protonConnection.setCondition(error);
      protonConnection.setProperties(Collections.singletonMap(AMQPConnectionConstants.CONNECTION_OPEN_FAILED, true));
      protonConnection.close();
   }

   @Override
   protected void redirect(AMQPRoutingContext context) {
      final String host = ConfigurationHelper.getStringProperty(
         TransportConstants.HOST_PROP_NAME, TransportConstants.DEFAULT_HOST, context.getTarget().getConnector().getParams());
      final int port = ConfigurationHelper.getIntProperty(
         TransportConstants.PORT_PROP_NAME, TransportConstants.DEFAULT_PORT, context.getTarget().getConnector().getParams());

      final String errorDescription = String.format(
         "Connection router %s redirected this connection to %s:%d", context.getRouter(), host, port);

      final Map<Symbol, Object>  info = new HashMap<>();
      info.put(AMQPConnectionConstants.REDIRECT_NETWORK_HOST, host);
      info.put(AMQPConnectionConstants.REDIRECT_PORT, port);

      final ErrorCondition error = new ErrorCondition(ConnectionError.REDIRECT, errorDescription, info);
      final Connection protonConnection = context.getProtonConnection();

      protonConnection.setCondition(error);
      protonConnection.setProperties(Collections.singletonMap(AMQPConnectionConstants.CONNECTION_OPEN_FAILED, true));
      protonConnection.close();
   }
}
