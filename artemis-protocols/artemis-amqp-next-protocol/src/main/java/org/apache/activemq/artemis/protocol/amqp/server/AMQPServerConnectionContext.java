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

import java.lang.invoke.MethodHandles;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.protocol.amqp.AMQPConnectionConfiguration;
import org.apache.activemq.artemis.protocol.amqp.AMQPRemotingConnection;
import org.apache.qpid.protonj2.engine.Connection;
import org.apache.qpid.protonj2.engine.Engine;
import org.apache.qpid.protonj2.engine.Receiver;
import org.apache.qpid.protonj2.engine.Sender;
import org.apache.qpid.protonj2.engine.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class maintains state and provides functionality for the server side of
 * an AMQP connection initiated from a remote client.
 */
public abstract class AMQPServerConnectionContext {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   protected final ActiveMQServer server;
   protected final Executor executor;
   protected final AMQPRemotingConnection remotingConnection;
   protected final AMQPConnectionConfiguration configuration;
   protected final ScheduledExecutorService scheduler;

   public AMQPServerConnectionContext(AMQPRemotingConnection remotingConnection, Executor executor, Connection connection) {
      this.server = remotingConnection.getServer();
      this.scheduler = remotingConnection.getServer().getScheduledPool();
      this.executor = executor;
      this.configuration = remotingConnection.getConfiguration();
      this.remotingConnection = remotingConnection;

      // This context binds to the connection life-cycle events and create server side resources
      // appropriate to the direction and intended functionality of the connection.
      connection.openHandler(this::handleRemoteOpen);
      connection.closeHandler(this::handleRemoteClose);
      connection.localOpenHandler(this::handleLocalOpen);
      connection.localCloseHandler(this::handleRemoteClose);
      connection.sessionOpenHandler(this::handleSessionOpen);
      connection.receiverOpenHandler(this::handleReceiverOpen);
      connection.senderOpenHandler(this::handleSenderOpen);
      connection.engineShutdownHandler(this::handleEngineShutdown);
   }

   public AMQPRemotingConnection getRemotingConnection() {
      return remotingConnection;
   }

   public AMQPConnectionConfiguration getConfiguration() {
      return configuration;
   }

   /**
    * Returns a boolean indicating if this connection context (Endpoint) is for an
    * incoming connection.
    *
    * @return true if the connection was initiated remotely.
    */
   public abstract boolean isIncoming();

   /**
    * Returns a boolean indicating if this connection context (Endpoint) is for an
    * outgoing connection.
    *
    * @return true if the connection was initiated locally.
    */
   public boolean isOutgoing() {
      return !isIncoming();
   }

   /*
    * Below are the event points for AMQP connections that must be handled by the server
    * some of which are abstract as the direction of connection creation matters for the
    * handling.
    */

   /**
    * Called when the connection is remotely opened.
    *
    * @param protonConnection
    *       The proton {@link Connection} instance this context is linked to.
    */
   protected abstract void handleRemoteOpen(Connection protonConnection);

   /**
    * Called when the connection is locally opened.
    *
    * @param protonConnection
    *       The proton {@link Connection} instance this context is linked to.
    */
   protected abstract void handleLocalOpen(Connection protonConnection);

   /**
    * Called when the connection is remotely closed.
    *
    * @param protonConnection
    *       The proton {@link Connection} instance this context is linked to.
    */
   protected void handleRemoteClose(Connection protonConnection) {
      try {
         protonConnection.close();
      } catch (Exception ex) {
         logger.trace("Error while closing connection after remote closed: ", ex.getMessage(), ex);
      } finally {
         // Allow output to be dispatched prior to the shutdown process
         // being triggered which should close the IO connection if not
         // already closed.
         executor.execute(() -> protonConnection.getEngine().shutdown());
      }
   }

   /**
    * Called when the connection is locally closed.
    *
    * @param protonConnection
    *       The proton {@link Connection} instance this context is linked to.
    */
   protected void handleLocalClose(Connection protonConnection) {
      // TODO
   }

   /**
    * Called when the remote has opened a new Session
    *
    * @param protonSession
    *       The proton {@link Session} instance on this side of the connection.
    */
   protected void handleSessionOpen(Session protonSession) {
      // TODO
   }

   /**
    * Called when the remote has opened a new link and this end of the link is
    * the receiver side meaning the remote intends to send deliveries to this
    * server.
    *
    * @param protonReceiver
    *       The proton {@link Receiver} instance on this side of the link.
    */
   protected void handleReceiverOpen(Receiver protonReceiver) {
      // TODO
   }

   /**
    * Called when the remote has opened a new link and this end of the link is
    * the sending side meaning the remote intends to receive deliveries from
    * this server.
    *
    * @param protonSender
    *       The proton {@link Sender} instance on this side of the link.
    */
   protected void handleSenderOpen(Sender protonSender) {
      // TODO
   }

   /**
    * Called when the proton engine has been shut-down, this connection context
    * should cleanup and local resources as the connection is no longer live.
    *
    * @param protonEngine
    *       The proton {@link Engine} that was shut down.
    */
   protected void handleEngineShutdown(Engine protonEngine) {
      // TODO
   }
}
