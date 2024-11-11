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

package org.apache.activemq.artemis.protocol.amqp.connect.bridge;

import static org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport.DETACH_FORCED;
import static org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport.NOT_FOUND;
import static org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport.RESOURCE_DELETED;

import java.io.Closeable;
import java.util.function.Consumer;

import org.apache.activemq.artemis.protocol.amqp.proton.AMQPConnectionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPSessionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.ProtonServerReceiverContext;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.transport.Detach;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Receiver;

/**
 * Base implementation for AMQP Bridge receiver implementations
 */
public abstract class AMQPBridgeReceiver implements Closeable {

   protected final AMQPBridgeManager bridge;
   protected final AMQPBridgeReceiverConfiguration configuration;
   protected final AMQPBridgeReceiverInfo receiverInfo;
   protected final AMQPBridgePolicy policy;
   protected final AMQPConnectionContext connection;
   protected final AMQPSessionContext session;

   protected ProtonServerReceiverContext receiverContext;
   protected Receiver protonReceiver;
   protected boolean started;
   protected boolean closed;
   protected Consumer<AMQPBridgeReceiver> remoteOpenHandler;
   protected Consumer<AMQPBridgeReceiver> remoteCloseHandler;

   public AMQPBridgeReceiver(AMQPBridgeManager bridge,
                             AMQPBridgeReceiverConfiguration configuration,
                             AMQPSessionContext session,
                             AMQPBridgeReceiverInfo receiverInfo,
                             AMQPBridgePolicy policy) {
      this.bridge = bridge;
      this.receiverInfo = receiverInfo;
      this.policy = policy;
      this.connection = session.getAMQPConnectionContext();
      this.session = session;
      this.configuration = configuration;
   }

   /**
    * Starts the receiver instance which includes creating the remote resources and
    * performing any internal initialization needed to fully establish the receiver
    * instance. This call should not block and any errors encountered on creation of
    * the backing receiver resources should utilize the error handling mechanisms of
    * this AMQP bridge receiver.
    */
   public final synchronized void start() {
      if (!started && !closed) {
         started = true;
         asyncCreateReceiver();
      }
   }

   /**
    * Close the AMQP bridge receiver instance and cleans up its resources. This method
    * should not block and the actual resource shutdown work should occur asynchronously.
    */
   @Override
   public final synchronized void close() {
      if (!closed) {
         closed = true;
         if (started) {
            started = false;
            asyncCloseReceiver();
         }
      }
   }

   /**
    * @return the policy that this sender was configured to use.
    */
   public final AMQPBridgePolicy getPolicy() {
      return policy;
   }

   /**
    * @return the {@link AMQPBridgeManager} that this receiver operates under.
    */
   public final AMQPBridgeManager getBridge() {
      return bridge;
   }

   /**
    * @return an information object that defines the characteristics of the {@link AMQPBridgeReceiver}
    */
   public final AMQPBridgeReceiverInfo getReceiverInfo() {
      return receiverInfo;
   }

   /**
    * Provides and event point for notification of the receiver having been opened successfully
    * by the remote. This handler will not be called if the remote rejects the link attach and
    * a {@link Detach} is expected to follow.
    *
    * @param handler
    *    The handler that will be invoked when the remote opens this receiver.
    *
    * @return this receiver instance.
    */
   public final AMQPBridgeReceiver setRemoteOpenHandler(Consumer<AMQPBridgeReceiver> handler) {
      if (started) {
         throw new IllegalStateException("Cannot set a remote open handler after the senderContext is started");
      }

      this.remoteCloseHandler = handler;
      return this;
   }

   /**
    * Provides and event point for notification of the receiver having been closed by
    * the remote.
    *
    * @param handler
    *    The handler that will be invoked when the remote closes this receiver.
    *
    * @return this receiver instance.
    */
   public final AMQPBridgeReceiver setRemoteClosedHandler(Consumer<AMQPBridgeReceiver> handler) {
      if (started) {
         throw new IllegalStateException("Cannot set a remote close handler after the senderContext is started");
      }

      this.remoteCloseHandler = handler;
      return this;
   }

   /**
    * Handles the create of the actual AMQP receiver link on the connection thread.
    */
   protected abstract void asyncCreateReceiver();

   /**
    * Handles the close of the actual AMQP receiver link on the connection thread.
    */
   protected abstract void asyncCloseReceiver();

   protected final Symbol[] getRemoteTerminusCapabilities() {
      if (policy.getRemoteTerminusCapabilities() != null && !policy.getRemoteTerminusCapabilities().isEmpty()) {
         return policy.getRemoteTerminusCapabilities().toArray(new Symbol[0]);
      } else {
         return null;
      }
   }

   protected final boolean remoteLinkClosedInterceptor(Link link) {
      if (link == protonReceiver && link.getRemoteCondition() != null && link.getRemoteCondition().getCondition() != null) {
         final Symbol errorCondition = link.getRemoteCondition().getCondition();

         // Cases where remote link close is not considered terminal, additional checks
         // should be added as needed for cases where the remote has closed the link either
         // during the attach or at some point later.

         if (RESOURCE_DELETED.equals(errorCondition)) {
            // Remote side manually deleted this queue.
            return true;
         } else if (NOT_FOUND.equals(errorCondition)) {
            // Remote did not have a queue that matched.
            return true;
         } else if (DETACH_FORCED.equals(errorCondition)) {
            // Remote operator forced the link to detach.
            return true;
         }
      }

      return false;
   }
}
