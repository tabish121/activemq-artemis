/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.protocol.amqp.server;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

import org.apache.activemq.artemis.protocol.amqp.AMQPRemotingConnection;
import org.apache.activemq.artemis.protocol.amqp.support.AMQPConnectionConstants;
import org.apache.activemq.artemis.protocol.amqp.support.AMQPConnectionProperties;
import org.apache.qpid.protonj2.engine.Connection;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.transport.AmqpError;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;

/**
 * Represents an incoming AMQP connection to the broker.
 * <p>
 * Incoming client connections must always open first on the broker so we can validate here
 * the connection offered and desired capabilities and then answer back with the correct
 * response to desired capabilities etc.  We should also validate here the client requests
 * for specific connection semantics such as sole connection per container id etc.
 * <p>
 * If another broker feature handles incoming connections in a different means that through
 * the standards and practices used here a new context should likely be created that enforces
 * that rule-set as opposed to adding unreasonably large amount of configuration etc here.
 */
public class AMQPIncomingConnectionContext extends AMQPServerConnectionContext {

   protected final AMQPConnectionProperties remoteProperties = new AMQPConnectionProperties();

   public AMQPIncomingConnectionContext(AMQPRemotingConnection remotingConnection, Executor executor, Connection connection) {
      super(remotingConnection, executor, connection);
   }

   @Override
   public boolean isIncoming() {
      return true;
   }

   @Override
   protected void handleRemoteOpen(Connection protonConnection) {
      remoteProperties.discoverProperties(
         protonConnection.getRemoteOfferedCapabilities(), protonConnection.getRemoteDesiredCapabilities());

      if (wasConnectionIsRedirected(protonConnection) || wasSoleConnectionDirectiveViolated(protonConnection)) {
         return; // Connection was closed and processing the open should cease.
      }

      // Remote met all the criteria for success configure the connection locally and then trigger open response.
      protonConnection.setContainerId(server.getConfiguration().getName());
      protonConnection.setOfferedCapabilities(AMQPConnectionProperties.getDefaultOfferedCapabilities());
      protonConnection.setDesiredCapabilities(AMQPConnectionProperties.getDefaultDesiredCapabilities());
      protonConnection.open();
   }

   @Override
   protected void handleLocalOpen(Connection protonConnection) {
      // TODO: Configure connection idle timeout processing.
   }

   /**
    * Performs check to see if the connection itself is redirected or refused which
    * if true this method will populate the error condition and any redirection data
    * into the connection Open performative and close the connection. If the connection
    * was redirected then processing of the open should stop.
    *
    * @param protonConnection
    *    The proton {@link Connection} that is being opened.
    *
    * @return true if the connection was redirected or refused.
    */
   protected boolean wasConnectionIsRedirected(Connection protonConnection) {
      try {
         // TODO Maybe just make this a method in the remoting connection ?
         if (remotingConnection.getTransportConnection().getRouter() != null) {
             remotingConnection.getProtocolManager().getRoutingHandler().route(remotingConnection, protonConnection);
         }
      } catch (Exception e) {
         protonConnection.setCondition(new ErrorCondition(AmqpError.INTERNAL_ERROR, e.getMessage()));
         protonConnection.close();
      }

      return protonConnection.isLocallyClosed();
   }

   /**
    * Validates that a connection requested to be the sole connection with the given container
    * ID and there was already a connection with that ID present in which case the connection is
    * closed and an error is provided indicating same.
    *
    * @param protonConnection
    *    The proton {@link Connection} that is being opened.
    *
    * @return true if the connection violated the sole connection per container id invariant.
    */
   protected boolean wasSoleConnectionDirectiveViolated(Connection protonConnection) {
      final boolean idOK = remotingConnection.registerClientConnection(
         protonConnection.getRemoteContainerId(), remoteProperties.isSoleConnectionPerContainerIdDesired());

      if (!idOK) {
         // https://issues.apache.org/jira/browse/ARTEMIS-728
         final Map<Symbol, Object> connectionProperties = new HashMap<>();
         connectionProperties.put(AMQPConnectionConstants.CONNECTION_OPEN_FAILED, "true");
         final Map<Symbol, Object> info = new HashMap<>();
         info.put(AmqpError.INVALID_FIELD, AMQPConnectionConstants.CONTAINER_ID);
         protonConnection.setCondition(
            new ErrorCondition(AmqpError.INVALID_FIELD, "sole connection per container ID capability violated", info));
         protonConnection.setProperties(connectionProperties);
         protonConnection.close();
      }

      return protonConnection.isLocallyClosed();
   }
}
