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
package org.apache.activemq.artemis.protocol.amqp.connect.federation;

import static org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport.DETACH_FORCED;
import static org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport.NOT_FOUND;
import static org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport.RESOURCE_DELETED;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.config.TransformerConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.transformer.Transformer;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPException;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationConsumerInfo;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationReceiveFromResourcePolicy;
import org.apache.activemq.artemis.protocol.amqp.federation.internal.FederationConsumerInternal;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPConnectionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPSessionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.ProtonAbstractReceiver;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Modified;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.messaging.Released;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for AMQP Federation consumers that implements some of the common functionality.
 */
public abstract class AMQPFederationConsumer implements FederationConsumerInternal {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   protected static final Symbol[] OUTCOMES = new Symbol[]{Accepted.DESCRIPTOR_SYMBOL, Rejected.DESCRIPTOR_SYMBOL,
                                                           Released.DESCRIPTOR_SYMBOL, Modified.DESCRIPTOR_SYMBOL};

   protected static final Modified DEFAULT_OUTCOME;
   static {
      DEFAULT_OUTCOME = new Modified();
      DEFAULT_OUTCOME.setDeliveryFailed(true);
   }

   protected enum ConsumerState {
      NEW,
      STARTED,
      STOPPING,
      STOPPED,
      CLOSED
   }

   // Sequence ID value used to keep links that would otherwise have the same name from overlapping
   // this generally occurs when a remote link detach is delayed and new demand is added before it
   // arrives resulting in an unintended link stealing scenario in the proton engine but can also
   // occur when consumers on the same queue have differing filters.
   protected static final AtomicLong LINK_SEQUENCE_ID = new AtomicLong();

   protected final AMQPFederation federation;
   protected final AMQPFederationConsumerConfiguration configuration;
   protected final FederationConsumerInfo consumerInfo;
   protected final AMQPConnectionContext connection;
   protected final AMQPSessionContext session;
   protected final Predicate<Link> remoteCloseInterceptor = this::remoteLinkClosedInterceptor;
   protected final BiConsumer<FederationConsumerInfo, Message> messageObserver;
   protected final AtomicLong messageCount = new AtomicLong();
   protected final Transformer transformer;

   protected ProtonAbstractReceiver receiver;
   protected Receiver protonReceiver;
   protected volatile ConsumerState state = ConsumerState.NEW;
   protected Consumer<FederationConsumerInternal> remoteCloseHandler;

   public AMQPFederationConsumer(AMQPFederation federation, AMQPFederationConsumerConfiguration configuration,
                                 AMQPSessionContext session, FederationConsumerInfo consumerInfo,
                                 FederationReceiveFromResourcePolicy policy,
                                 BiConsumer<FederationConsumerInfo, Message> messageObserver) {
      this.federation = federation;
      this.consumerInfo = consumerInfo;
      this.connection = session.getAMQPConnectionContext();
      this.session = session;
      this.configuration = configuration;
      this.messageObserver = messageObserver;

      final TransformerConfiguration transformerConfiguration = policy.getTransformerConfiguration();
      if (transformerConfiguration != null) {
         this.transformer = federation.getServer().getServiceRegistry().getFederationTransformer(policy.getPolicyName(), transformerConfiguration);
      } else {
         this.transformer = (m) -> m;
      }
   }

   /**
    * @return the number of messages this consumer has received from the remote during its lifetime.
    */
   public final long getMessagesReceived() {
      return messageCount.get();
   }

   @Override
   public final AMQPFederation getFederation() {
      return federation;
   }

   @Override
   public final FederationConsumerInfo getConsumerInfo() {
      return consumerInfo;
   }

   @Override
   public final synchronized void start() {
      if (state == ConsumerState.CLOSED) {
         throw new IllegalStateException("Cannot start a consumer that was already closed.");
      }

      if (state == ConsumerState.STOPPING) {
         throw new IllegalStateException("Cannot start a consumer that is currently stopping.");
      }

      if (state == ConsumerState.NEW) {
         state = ConsumerState.STARTED;
         asyncCreateReceiver();
      } else {
         state = ConsumerState.STARTED;
         asyncStartReceiver();
      }
   }

   @Override
   public final boolean isStarted() {
      return state == ConsumerState.STARTED;
   }

   @Override
   public final synchronized void stop(Consumer<Boolean> onStopped) {
      if (state != ConsumerState.STARTED) {
         throw new IllegalStateException("Cannot trigger a stop on a not started consumer");
      }

      state = ConsumerState.STOPPING;

      asyncStopReceiver((stopped) -> {
         // Take the lock to prevent overlap of start calls that are not triggered
         // from the provided stopped callback. A call to start in the callback will
         // be able to complete without issue.
         synchronized (this) {
            if (stopped) {
               state = ConsumerState.STOPPED;
            }

            try {
               onStopped.accept(stopped);
            } catch (Exception ex) {
               logger.trace("Caught error running provided on stopped callback: ", ex);
            }
         }
      });
   }

   @Override
   public boolean isStopping() {
      return state == ConsumerState.STOPPING;
   }

   @Override
   public boolean isStopped() {
      return state == ConsumerState.STOPPED;
   }

   @Override
   public final synchronized void close() {
      if (state != ConsumerState.CLOSED) {
         state = ConsumerState.CLOSED;

         if (state != ConsumerState.NEW) {
            asyncCloseReceiver();
         }
      }
   }

   @Override
   public final boolean isClosed() {
      return state == ConsumerState.CLOSED;
   }

   @Override
   public final AMQPFederationConsumer setRemoteClosedHandler(Consumer<FederationConsumerInternal> handler) {
      if (state != ConsumerState.NEW) {
         throw new IllegalStateException("Cannot set a remote close handler after the consumer is started");
      }

      this.remoteCloseHandler = handler;
      return this;
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

   /**
    * Called from a subclass upon handling an incoming federated message from the remote.
    *
    * @param message
    *    The original message that arrived from the remote.
    */
   protected final void recordFederatedMessageReceived(Message message) {
      messageCount.incrementAndGet();

      if (messageObserver != null) {
         messageObserver.accept(consumerInfo, message);
      }
   }

   /**
    * Called before the message is dispatched to the broker for processing.
    *
    * @param message
    *    The message after any processing which is about to be dispatched.
    *
    * @throws ActiveMQException if any broker plugin throws an exception during its processing.
    */
   protected final void signalBeforeFederationConsumerMessageHandled(Message message) throws ActiveMQException {
      try {
         federation.getServer().callBrokerAMQPFederationPlugins((plugin) -> {
            if (plugin instanceof ActiveMQServerAMQPFederationPlugin) {
               ((ActiveMQServerAMQPFederationPlugin) plugin).beforeFederationConsumerMessageHandled(this, message);
            }
         });
      } catch (ActiveMQException t) {
         ActiveMQServerLogger.LOGGER.federationPluginExecutionError("beforeFederationConsumerMessageHandled", t);
      }
   }

   /**
    * Called after the message is dispatched to the broker for processing.
    *
    * @param message
    *    The message after any processing which has been dispatched to the broker.
    *
    * @throws ActiveMQException if any broker plugin throws an exception during its processing.
    */
   protected final void signalAfterFederationConsumerMessageHandled(Message message) throws ActiveMQException {
      try {
         federation.getServer().callBrokerAMQPFederationPlugins((plugin) -> {
            if (plugin instanceof ActiveMQServerAMQPFederationPlugin) {
               ((ActiveMQServerAMQPFederationPlugin) plugin).afterFederationConsumerMessageHandled(this, message);
            }
         });
      } catch (ActiveMQException t) {
         ActiveMQServerLogger.LOGGER.federationPluginExecutionError("afterFederationConsumerMessageHandled", t);
      }
   }

   /**
    * Called during the start of the consumer to trigger an asynchronous link attach
    * of the underlying AMQP receiver that backs this federation consumer. The new
    * receiver should be created in a started state.
    */
   protected abstract void asyncCreateReceiver();

   /**
    * Called during the re-start of the consumer to trigger an asynchronous flow of
    * credit to the underlying AMQP receiver that backs this federation consumer.
    */
   protected void asyncStartReceiver() {
      connection.runLater(() -> {
         if (state == ConsumerState.CLOSED) {
            return;
         }

         try {
            receiver.start();
         } catch (Exception ex) {
            logger.debug("Caught error trying to start an existing receiver:", ex);
         }

         connection.flush();
      });
   }

   /**
    * Called during the stop of the consumer to trigger an asynchronous stop of
    * the underlying AMQP receiver that backs this federation consumer, the stop
    * needs to wait for credit to be drained and in-flight messages to be settled.
    * The supplied {@link Consumer} will be passed a boolean <code>true</code> if
    * the stop completed successfully or <code>false</code> if the stop request
    * timed out.
    *
    * @param onStopped
    *    A {@link Consumer} that will be called to signal the stop completed or timed out.
    */
   protected void asyncStopReceiver(Consumer<Boolean> onStopped) {
      connection.runLater(() -> {
         if (state == ConsumerState.CLOSED) {
            return;
         }

         try {
            receiver.stop(configuration.getReceiverQuiesceTimeout(), (recvr, stopped) -> onStopped.accept(stopped));
         } catch (Exception ex) {
            logger.debug("Caught error trying to stop an existing receiver:", ex);
         }

         connection.flush();
      });
   }

   /**
    * Called during the close of the consumer to trigger an asynchronous link detach
    * of the underlying AMQP receiver that backs this federation consumer.
    */
   protected final void asyncCloseReceiver() {
      connection.runLater(() -> {
         federation.removeLinkClosedInterceptor(consumerInfo.getId());

         if (receiver != null) {
            try {
               receiver.close(false);
            } catch (ActiveMQAMQPException e) {
            } finally {
               receiver = null;
            }
         }

         // Need to track the proton receiver and close it here as the default
         // context implementation doesn't do that and could result in no detach
         // being sent in some cases and possible resources leaks.
         if (protonReceiver != null) {
            try {
               protonReceiver.close();
            } finally {
               protonReceiver = null;
            }
         }

         connection.flush();
      });
   }
}
