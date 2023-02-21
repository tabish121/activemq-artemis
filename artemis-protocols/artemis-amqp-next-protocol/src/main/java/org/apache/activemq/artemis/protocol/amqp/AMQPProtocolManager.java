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

package org.apache.activemq.artemis.protocol.amqp;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.BaseInterceptor;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyServerConnection;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.management.Notification;
import org.apache.activemq.artemis.core.server.management.NotificationListener;
import org.apache.activemq.artemis.protocol.amqp.message.AMQPMessage;
import org.apache.activemq.artemis.protocol.amqp.server.AMQPRoutingHandler;
import org.apache.activemq.artemis.spi.core.protocol.AbstractProtocolManager;
import org.apache.activemq.artemis.spi.core.protocol.ConnectionEntry;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManagerFactory;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.remoting.Acceptor;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelPipeline;

/**
 * The AMQP protocol manager implementation.
 */
public final class AMQPProtocolManager extends AbstractProtocolManager<AMQPMessage, AMQPMessageInterceptor, AMQPRemotingConnection, AMQPRoutingHandler> implements NotificationListener {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static final List<String> WEBSOCKET_REGISTRY_NAMES = Collections.singletonList("amqp");

   private final ActiveMQServer server;
   private final AMQPRoutingHandler routingHandler;
   private final AMQPProtocolManagerFactory factory;
   private final AMQPConnectionConfiguration configuration = new AMQPConnectionConfiguration();

   private final List<AMQPMessageInterceptor> incomingInterceptors = new ArrayList<>();
   private final List<AMQPMessageInterceptor> outgoingInterceptors = new ArrayList<>();
   private final Map<SimpleString, RoutingType> prefixes = new HashMap<>();

   @SuppressWarnings("rawtypes")
   public AMQPProtocolManager(AMQPProtocolManagerFactory factory, ActiveMQServer server, List<BaseInterceptor> incomingInterceptors, List<BaseInterceptor> outgoingInterceptors) {
      this.factory = factory;
      this.server = server;
      this.routingHandler = new AMQPRoutingHandler(server);
      this.updateInterceptors(incomingInterceptors, outgoingInterceptors);
   }

   public ActiveMQServer getServer() {
      return server;
   }

   @Override
   public ProtocolManagerFactory<AMQPMessageInterceptor> getFactory() {
      return factory;
   }

   @Override
   @SuppressWarnings("rawtypes")
   public void updateInterceptors(List<BaseInterceptor> incomingInterceptors, List<BaseInterceptor> outgoingInterceptors) {
      this.incomingInterceptors.clear();
      this.incomingInterceptors.addAll(getFactory().filterInterceptors(incomingInterceptors));
      this.outgoingInterceptors.clear();
      this.outgoingInterceptors.addAll(getFactory().filterInterceptors(outgoingInterceptors));
   }

   @Override
   public ConnectionEntry createConnectionEntry(Acceptor acceptorUsed, Connection connection) {
      final long connectionTTL = ensureConnectionTTLConfigured();
      final Executor connectionExecutor = server.getExecutorFactory().getExecutor();

      AMQPRemotingConnection remotingConnection = AMQPRemotingConnectionFactory.clinetInbound(this, connection, connectionExecutor);

      // ConnectionEntry only understands -1 as disabling TTL otherwise we would see disconnects
      // for no reason so we double check here to ensure that a valid TTL is applied.
      return new ConnectionEntry(remotingConnection,
                                 connectionExecutor,
                                 System.currentTimeMillis(),
                                 connectionTTL <= 0 ? -1 : connectionTTL);
   }

   @Override
   public void handleBuffer(RemotingConnection connection, ActiveMQBuffer buffer) {
      connection.bufferReceived(connection.getID(), buffer);
   }

   @Override
   public boolean isProtocol(byte[] array) {
      return array.length >= 4 && array[0] == (byte) 'A' &&
                                  array[1] == (byte) 'M' &&
                                  array[2] == (byte) 'Q' &&
                                  array[3] == (byte) 'P';
   }

   @Override
   public boolean acceptsNoHandshake() {
      return false;
   }

   @Override
   public List<String> websocketSubprotocolIdentifiers() {
      return WEBSOCKET_REGISTRY_NAMES;
   }

   @Override
   public AMQPRoutingHandler getRoutingHandler() {
      return routingHandler;
   }

   @Override
   public void setAnycastPrefix(String anycastPrefix) {
      for (String prefix : anycastPrefix.split(",")) {
         prefixes.put(SimpleString.toSimpleString(prefix), RoutingType.ANYCAST);
      }
   }

   @Override
   public void setMulticastPrefix(String multicastPrefix) {
      for (String prefix : multicastPrefix.split(",")) {
         prefixes.put(SimpleString.toSimpleString(prefix), RoutingType.MULTICAST);
      }
   }

   @Override
   public void handshake(NettyServerConnection connection, ActiveMQBuffer buffer) {
      // No response needed so far in this protocol head.
   }

   @Override
   public void addChannelHandlers(ChannelPipeline pipeline) {
      // No response needed so far in this protocol head.
   }

   @Override
   public void onNotification(Notification notification) {
      // No response needed so far in this protocol head.
   }

   /**
    * @return a snapshot of the current broker AMQP connection configuration options.
    */
   public AMQPConnectionConfiguration snapshotConfiguration() {
      ensureConnectionTTLConfigured();

      // Update on each create to ensure latest view of server configuration.
      configuration.setAmqpUseCoreSubscriptionNaming(server.getConfiguration().isAmqpUseCoreSubscriptionNaming());

      return configuration.copy();
   }

   /*
    * We always want to use a value for connection Idle timeout and local
    * TTL value assigned into the IO connection so here we try and find one
    * based on levels of configuration.
    */
   private final long ensureConnectionTTLConfigured() {
      long ttl = ActiveMQClient.DEFAULT_CONNECTION_TTL;

      if (server.getConfiguration().getConnectionTTLOverride() >= 0) {
         ttl = server.getConfiguration().getConnectionTTLOverride();
      }

      // This was either configured on the acceptor which overrides the override
      // or we update the per instance configuration with the value we have already.
      // The connection configuration object defaults its idle-timeout value to minus
      // one so we know if a valid value was applied based on that.
      if (configuration.getConnectionIdleTimeout() >= 0) {
         ttl = configuration.getConnectionIdleTimeout();
      } else {
         configuration.setConnectionIdleTimeout(ttl);
      }

      return ttl > 0 ? ttl : -1;  // Broker friendly TTL value
   }

   /*
    * These bean access methods are simply to carry the acceptor configuration into the
    * AMQP configuration object and are therefore not documented here, to see what each
    * setting implies for the connection see the API documentation in the configuration
    * class.
    */

   // TODO : Can we make this just point to the configuration object

   public boolean isDirectDeliver() {
      return configuration.isDirectDeliver();
   }

   public void setDirectDeliver(boolean directDeliver) {
      configuration.setDirectDeliver(directDeliver);
   }

   public Long getAmqpIdleTimeout() {
      return configuration.getConnectionIdleTimeout() == -1 ? null : configuration.getConnectionIdleTimeout();
   }

   public void setAmqpIdleTimeout(Long ttl) {
      logger.debug("Setting up {} as the connectionTtl", ttl);
      if (ttl != null) {
         configuration.setConnectionIdleTimeout(ttl);
      } else {
         configuration.setConnectionIdleTimeout(-1);
      }
   }

   public int getAmqpCredits() {
      return configuration.getSenderCredits();
   }

   public void setAmqpCredits(int amqpCredits) {
      configuration.setSenderCredits(amqpCredits);
   }

   public int getAmqpLowCredits() {
      return configuration.getSenderCreditsLow();
   }

   public void setAmqpLowCredits(int amqpLowCredits) {
      configuration.setSenderCreditsLow(amqpLowCredits);
   }

   public int getMaxFrameSize() {
      return configuration.getMaxFrameSize();
   }

   public void setMaxFrameSize(int maxFrameSize) {
      configuration.setMaxFrameSize(maxFrameSize);
   }

   public String[] getSaslMechanisms() {
      return configuration.getSaslMechanisms();
   }

   public void setSaslMechanisms(String[] saslMechanisms) {
      configuration.setSaslMechanisms(saslMechanisms);
   }

   public String getSaslLoginConfigScope() {
      return configuration.getSaslLoginConfigScope();
   }

   public void setSaslLoginConfigScope(String saslLoginConfigScope) {
      configuration.setSaslLoginConfigScope(saslLoginConfigScope);
   }

   public int getInitialRemoteMaxFrameSize() {
      return configuration.getInitialRemoteMaxFrameSize();
   }

   public void setInitialRemoteMaxFrameSize(int initialRemoteMaxFrameSize) {
      configuration.setInitialRemoteMaxFrameSize(initialRemoteMaxFrameSize);
   }

   public boolean isUseModifiedForTransientDeliveryErrors() {
      return configuration.isUseModifiedForTransientDeliveryErrors();
   }

   public void setAmqpUseModifiedForTransientDeliveryErrors(boolean useModifiedForTransientDeliveryErrors) {
      configuration.setUseModifiedForTransientDeliveryErrors(useModifiedForTransientDeliveryErrors);
   }

   public boolean isAmqpTreatRejectAsUnmodifiedDeliveryFailed() {
      return configuration.isTreatRejectAsUnmodifiedDeliveryFailed();
   }

   public void setAmqpTreatRejectAsUnmodifiedDeliveryFailed(boolean treatRejectAsUnmodifiedDeliveryFailed) {
      configuration.setTreatRejectAsUnmodifiedDeliveryFailed(treatRejectAsUnmodifiedDeliveryFailed);
   }
}
