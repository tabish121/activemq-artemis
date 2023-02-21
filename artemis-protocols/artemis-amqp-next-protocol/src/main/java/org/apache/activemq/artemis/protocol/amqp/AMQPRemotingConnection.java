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
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import javax.security.auth.Subject;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQRemoteDisconnectException;
import org.apache.activemq.artemis.core.buffers.impl.ChannelBufferWrapper;
import org.apache.activemq.artemis.core.client.ActiveMQClientLogger;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.protocol.amqp.sasl.server.SASLResult;
import org.apache.activemq.artemis.spi.core.protocol.AbstractRemotingConnection;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.utils.ByteUtil;
import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferComponent;
import org.apache.qpid.protonj2.buffer.ProtonBufferComponentAccessor;
import org.apache.qpid.protonj2.buffer.netty.Netty4ProtonBufferAllocator;
import org.apache.qpid.protonj2.buffer.netty.Netty4ToProtonBufferAdapter;
import org.apache.qpid.protonj2.engine.Engine;
import org.apache.qpid.protonj2.engine.exceptions.EngineStateException;
import org.apache.qpid.protonj2.types.transport.ConnectionError;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ReferenceCountUtil;

/**
 * Provides the server with a link back to the AMQP protocol handling bits
 */
public class AMQPRemotingConnection extends AbstractRemotingConnection {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static final AtomicIntegerFieldUpdater<AMQPRemotingConnection> CONNECTION_REGISTRATION_UPDATER =
      AtomicIntegerFieldUpdater.newUpdater(AMQPRemotingConnection.class, "connectionRegistered");

   private final AMQPProtocolManager manager;

   private final AMQPConnectionConfiguration configuration;

   private final Engine protonEngine;

   private final Executor connectionExecutor;

   private SASLResult saslResult;

   @SuppressWarnings("unused")
   private volatile int connectionRegistered;

   private final Netty4ProtonBufferAllocator allocator = (Netty4ProtonBufferAllocator) Netty4ProtonBufferAllocator.UNPOOLED;

   /**
    * Contractual constructor that accepts the transport {@link Connection} and assigned
    * {@link Executor} for use in the protocol head.
    *
    * @param manager
    *       The AMQP protocol manager that is linked to this remote connection.
    * @param transportConnection
    *       The IO connection that underlies this AMQP connection
    * @param executor
    *       The connection {@link Executor} assigned to this AMQP connection.
    */
   public AMQPRemotingConnection(AMQPProtocolManager manager, Connection transportConnection, Executor executor, Engine engine) {
      super(transportConnection, executor);

      this.manager = manager;
      this.protonEngine = engine;
      this.protonEngine.outputConsumer(this::handleEngineOutput);
      this.protonEngine.shutdownHandler(this::handleEngineShutdown);
      this.protonEngine.errorHandler(this::handleEngineFailure);
      this.connectionExecutor = executor;
      this.configuration = manager.snapshotConfiguration();

      transportConnection.setProtocolConnection(this);
   }

   public AMQPConnectionConfiguration getConfiguration() {
      return configuration;
   }

   public AMQPProtocolManager getProtocolManager() {
      return manager;
   }

   public Executor getConnectionExecutor() {
      return connectionExecutor;
   }

   public ActiveMQServer getServer() {
      return manager.getServer();
   }

   public Engine getProtonEngine() {
      return protonEngine;
   }

   @Override
   public boolean isSupportsFlowControl() {
      return true;
   }

   @Override
   public void fail(ActiveMQException error, String scaleDownTargetNodeID) {
      if (destroyed) {
         return;
      }

      destroyed = true;

      // Filter it like the other protocols
      if (!(error instanceof ActiveMQRemoteDisconnectException)) {
         ActiveMQClientLogger.LOGGER.connectionFailureDetected(transportConnection.getRemoteAddress(), error.getMessage(), error.getType());
      }

      // Then call the listeners before the underlying AMQP connection is closed.
      callFailureListeners(error, scaleDownTargetNodeID);
      callClosingListeners();

      getTransportConnection().close();
   }

   @Override
   public void destroy() {
      synchronized (this) {
         if (destroyed) {
            return;
         }

         destroyed = true;
      }

      try {
         protonEngine.shutdown();
      } catch (Exception ex) {}

      callClosingListeners();

      getTransportConnection().close();
   }

   @Override
   public void disconnect(boolean criticalError) {
      try {
         protonEngine.connection().setCondition(
            new ErrorCondition(ConnectionError.CONNECTION_FORCED, "Server initiated connection close"));
         protonEngine.connection().close();
      } catch (Exception ex) {
         // Error on close can be ignore, IO connection might already be down.
      } finally {
         protonEngine.shutdown();
      }
   }

   @Override
   public void disconnect(String scaleDownNodeID, boolean criticalError) {
      disconnect(criticalError);
   }

   @Override
   public void bufferReceived(Object connectionID, ActiveMQBuffer buffer) {
      final ByteBuf incoming = buffer.byteBuf();

      if (logger.isTraceEnabled()) {
         ByteUtil.debugFrame(logger, "Buffer Received ", incoming);
      }

      try (ProtonBuffer wrappedByteBuf = allocator.wrap(incoming)) {
         do {
            protonEngine.ingest(wrappedByteBuf);
         } while (wrappedByteBuf.isReadable() && protonEngine.isWritable());
         // TODO - How do we handle case of not all data read ? Old code has some loggers
      } catch (EngineStateException e) {
         logger.warn("Caught problem during incoming data processing: {}", e.getMessage(), e);
         protonEngine.shutdown();
         destroy();
      }

      super.bufferReceived(connectionID, buffer);
   }

   @Override
   public String getProtocolName() {
      return AMQPProtocolManagerFactory.AMQP_PROTOCOL_NAME;
   }

   @Override
   public String getClientID() {
      return protonEngine.connection().getRemoteContainerId();
   }

   public SASLResult getSASLResult() {
      return saslResult;
   }

   void handleSaslOutcome(SASLResult result) {
      saslResult = result;
      if (!result.isOK()) {
         // TODO: Handle logging and state for failed SASL
         //       This could be checked in the engine shutdown handler is the SASL
         //       handle shuts down the or fails the engine.
         destroy();
      }
   }

   @Override
   public Subject getSubject() {
      SASLResult saslResult = getSASLResult();
      if (saslResult != null && saslResult.getSubject() != null) {
         return saslResult.getSubject();
      } else {
         return super.getSubject();
      }
   }

   /**
    * Allows a connection to attempt to register a new connection with the server and control
    * if the registered ID should be exclusive to a single connection.
    *
    * @param registeredId
    *       The ID under which the connection is registered
    * @param soleConnectionPerContainerIdDesired
    *       Should the connection be the sole owner of this ID
    *
    * @return true if registration succeeded or false if a connection already owns the ID,
    */
   public boolean registerClientConnection(String registeredId, boolean soleConnectionPerContainerId) {
      if (getServer().addClientConnection(registeredId, soleConnectionPerContainerId)) {
         CONNECTION_REGISTRATION_UPDATER.getAndSet(this, 1);
         return true;
      } else {
         return false;
      }
   }

   private void handleEngineShutdown(Engine engine) {
      try {
         if (CONNECTION_REGISTRATION_UPDATER.compareAndSet(this, 1, 0)) {
            getServer().removeClientConnection(protonEngine.connection().getRemoteContainerId());
         }

         getTransportConnection().close();
      } finally {
         logger.trace("Proton engine shutdown and the connection was closed");
      }
   }

   private void handleEngineFailure(Engine engine) {
      // TODO handle failure gracefully
      engine.shutdown();
      destroy();
   }

   private void handleEngineOutput(final ProtonBuffer buffer) {
      int writeCount = buffer.componentCount();

      try (ProtonBuffer ioBuffer = buffer; ProtonBufferComponentAccessor accessor = buffer.componentAccessor()) {
         for (ProtonBufferComponent output = accessor.firstReadable(); output != null; output = accessor.nextReadable()) {
            final ByteBuf nettyBuf;

            if (output instanceof Netty4ToProtonBufferAdapter) {
               // Take ownership of the netty buffer which leaves reference counts intact.
               nettyBuf = ((Netty4ToProtonBufferAdapter) output).unwrapAndRelease();
            } else if (output.unwrap() instanceof ByteBuf) {
               // Get the netty buffer and add a reference so the proton buffer close doesn't release it.
               nettyBuf = (ByteBuf) ReferenceCountUtil.retain(output.unwrap());
            } else {
               nettyBuf = PooledByteBufAllocator.DEFAULT.directBuffer(output.getReadableBytes());

               if (output.hasReadbleArray()) {
                  nettyBuf.writeBytes(output.getReadableArray(), output.getReadableArrayOffset(), output.getReadableBytes());
               } else {
                  nettyBuf.writeBytes(output.getReadableBuffer());
               }
            }

            getTransportConnection().write(new ChannelBufferWrapper(nettyBuf, true), --writeCount == 0);
         }
      }
   }
}
