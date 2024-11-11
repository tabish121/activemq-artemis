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

import static org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport.AMQP_LINK_INITIALIZER_KEY;
import static org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport.RECEIVER_PRIORITY;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.TransformerConfiguration;
import org.apache.activemq.artemis.core.server.AddressQueryResult;
import org.apache.activemq.artemis.core.server.transformer.Transformer;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPInternalErrorException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPNotFoundException;
import org.apache.activemq.artemis.protocol.amqp.logger.ActiveMQAMQPProtocolMessageBundle;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPConnectionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPSessionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.AmqpJmsSelectorFilter;
import org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport;
import org.apache.activemq.artemis.protocol.amqp.proton.ProtonServerReceiverContext;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.messaging.Modified;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.messaging.Released;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Receiver implementation for Bridged Addresses that receives from a remote
 * AMQP peer and forwards those messages onto the internal broker Address for
 * consumption by an attached consumers.
 */
public class AMQPBridgeFromAddressReceiver extends AMQPBridgeReceiver {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static final Symbol[] OUTCOMES = new Symbol[] {Accepted.DESCRIPTOR_SYMBOL, Rejected.DESCRIPTOR_SYMBOL,
                                                          Released.DESCRIPTOR_SYMBOL, Modified.DESCRIPTOR_SYMBOL};

   private static final Modified MODIFIED_FAILED;
   static {
      Modified modifiedFailed = new Modified();
      modifiedFailed.setDeliveryFailed(true);

      MODIFIED_FAILED = modifiedFailed;
   }

   // Sequence ID value used to keep links that would otherwise have the same name from overlapping
   // this generally occurs when a remote link detach is delayed and new demand is added before it
   // arrives resulting in an unintended link stealing scenario in the proton engine.
   private static final AtomicLong LINK_SEQUENCE_ID = new AtomicLong();

   private final Transformer transformer;

   public AMQPBridgeFromAddressReceiver(AMQPBridgeManager bridge, AMQPBridgeReceiverConfiguration configuration,
                                        AMQPSessionContext session, AMQPBridgeReceiverInfo receiverInfo,
                                        AMQPBridgeAddressPolicy policy) {
      super(bridge, configuration, session, receiverInfo, policy);

      final TransformerConfiguration transformerConfiguration = policy.getTransformerConfiguration();
      if (transformerConfiguration != null) {
         this.transformer = bridge.getServer().getServiceRegistry().getBridgeTransformer(policy.getPolicyName(), transformerConfiguration);
      } else {
         this.transformer = (m) -> m;
      }
   }

   @Override
   protected void asyncCloseReceiver() {
      connection.runLater(() -> {
         bridge.removeLinkClosedInterceptor(receiverInfo.getId());

         if (receiverContext != null) {
            try {
               receiverContext.close(false);
            } catch (ActiveMQAMQPException e) {
            } finally {
               receiverContext = null;
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

   @Override
   protected void asyncCreateReceiver() {
      connection.runLater(() -> {
         if (closed) {
            return;
         }

         try {
            final Receiver protonReceiver = session.getSession().receiver(generateLinkName());
            final Target target = new Target();
            final Source source = new Source();
            final String address = receiverInfo.getRemoteAddress();
            final String filterString = receiverInfo.getFilterString();

            source.setOutcomes(Arrays.copyOf(OUTCOMES, OUTCOMES.length));
            source.setDefaultOutcome(MODIFIED_FAILED);
            source.setDurable(TerminusDurability.NONE);
            source.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);
            source.setAddress(address);
            source.setCapabilities(getRemoteTerminusCapabilities());

            if (filterString != null && !filterString.isBlank()) {
               final AmqpJmsSelectorFilter jmsFilter = new AmqpJmsSelectorFilter(filterString);
               final Map<Symbol, Object> filtersMap = new HashMap<>();
               filtersMap.put(AmqpSupport.JMS_SELECTOR_KEY, jmsFilter);

               source.setFilter(filtersMap);
            }

            target.setAddress(receiverInfo.getLocalAddress());

            final Map<Symbol, Object> receiverProperties;
            if (receiverInfo.getPriority() != null) {
               receiverProperties = new HashMap<>();
               receiverProperties.put(RECEIVER_PRIORITY, receiverInfo.getPriority().intValue());
            } else {
               receiverProperties = null;
            }

            protonReceiver.setSenderSettleMode(configuration.isUsingPresettledSenders() ? SenderSettleMode.SETTLED : SenderSettleMode.UNSETTLED);
            protonReceiver.setReceiverSettleMode(ReceiverSettleMode.FIRST);
            protonReceiver.setProperties(receiverProperties);
            protonReceiver.setTarget(target);
            protonReceiver.setSource(source);
            protonReceiver.open();

            final ScheduledFuture<?> openTimeoutTask;
            final AtomicBoolean openTimedOut = new AtomicBoolean(false);

            if (configuration.getLinkAttachTimeout() > 0) {
               openTimeoutTask = bridge.getServer().getScheduledPool().schedule(() -> {
                  openTimedOut.set(true);
                  bridge.signalResourceCreateError(ActiveMQAMQPProtocolMessageBundle.BUNDLE.brokerConnectionTimeout());
               }, configuration.getLinkAttachTimeout(), TimeUnit.SECONDS);
            } else {
               openTimeoutTask = null;
            }

            this.protonReceiver = protonReceiver;

            protonReceiver.attachments().set(AMQP_LINK_INITIALIZER_KEY, Runnable.class, () -> {
               try {
                  if (openTimeoutTask != null) {
                     openTimeoutTask.cancel(false);
                  }

                  if (openTimedOut.get()) {
                     return;
                  }

                  final boolean linkOpened = protonReceiver.getRemoteSource() != null;

                  // Intercept remote close and check for valid reasons for remote closure such as
                  // the remote peer not having a matching node for this subscription or from an
                  // operator manually closing the link etc.
                  bridge.addLinkClosedInterceptor(receiverInfo.getId(), this::remoteLinkClosedInterceptor);

                  receiverContext = new AMQPBridgeAddressDeliveryReceiver(session, receiverInfo, protonReceiver);

                  if (linkOpened) {
                     logger.debug("AMQP Bridge {} address receiver {} completed open", bridge.getName(), receiverInfo);
                  } else {
                     logger.debug("AMQP Bridge {} address receiver {} rejected by remote", bridge.getName(), receiverInfo);
                  }

                  session.addReceiver(protonReceiver, (session, protonRcvr) -> {
                     return this.receiverContext;
                  });

                  if (linkOpened && remoteOpenHandler != null) {
                     remoteOpenHandler.accept(this);
                  }
               } catch (Exception e) {
                  bridge.signalError(e);
               }
            });
         } catch (Exception e) {
            bridge.signalError(e);
         }

         connection.flush();
      });
   }

   private String generateLinkName() {
      return "amqp-bridge-" + bridge.getName() +
             "-address-receiver-" + receiverInfo.getRemoteAddress() +
             "-" + bridge.getServer().getNodeID() +
             "-" + LINK_SEQUENCE_ID.incrementAndGet();
   }

   /**
    * Wrapper around the standard receiver context that provides bridge specific entry
    * points and customizes inbound delivery handling for this Address receiver.
    */
   private class AMQPBridgeAddressDeliveryReceiver extends ProtonServerReceiverContext {

      private final SimpleString cachedAddress;

      /**
       * Creates the AMQP bridge receiver instance.
       *
       * @param session
       *    The server session context bound to the receiver instance.
       * @param receiver
       *    The proton receiver that will be wrapped in this server context instance.
       */
      AMQPBridgeAddressDeliveryReceiver(AMQPSessionContext session, AMQPBridgeReceiverInfo receiverInfo, Receiver receiver) {
         super(session.getSessionSPI(), session.getAMQPConnectionContext(), session, receiver);

         this.cachedAddress = SimpleString.of(receiverInfo.getLocalAddress());
      }

      @Override
      public void close(boolean remoteLinkClose) throws ActiveMQAMQPException {
         super.close(remoteLinkClose);

         if (remoteLinkClose && remoteCloseHandler != null) {
            try {
               remoteCloseHandler.accept(AMQPBridgeFromAddressReceiver.this);
            } catch (Exception e) {
               logger.debug("User remote closed handler threw error: ", e);
            } finally {
               remoteCloseHandler = null;
            }
         }
      }

      @Override
      protected Runnable createCreditRunnable(AMQPConnectionContext connection) {
         // We defer to the configuration instance as opposed to the base class version that reads
         // from the connection this allows us to defer to configured policy properties that specify
         // credit over those in the bridge configuration or on the connector URI.
         return createCreditRunnable(configuration.getReceiverCredits(), configuration.getReceiverCreditsLow(), receiver, connection, this);
      }

      @Override
      protected int getConfiguredMinLargeMessageSize(AMQPConnectionContext connection) {
         // Looks at policy properties first before looking at receiver configuration and finally
         // going to the base connection context to read the URI configuration.
         return configuration.getLargeMessageThreshold();
      }

      @Override
      public void initialize() throws Exception {
         initialized = true;

         final Target target = (Target) receiver.getRemoteTarget();

         // Match the settlement mode of the remote instead of relying on the default of MIXED.
         receiver.setSenderSettleMode(receiver.getRemoteSenderSettleMode());

         // We don't currently support SECOND so enforce that the answer is always FIRST
         receiver.setReceiverSettleMode(ReceiverSettleMode.FIRST);

         // the target will have an address and it will naturally have a Target otherwise
         // the remote is misbehaving and we close it.
         if (target == null || target.getAddress() == null || target.getAddress().isEmpty()) {
            throw new ActiveMQAMQPInternalErrorException("Remote should have sent a valid Target but we got: " + target);
         }

         if (!target.getAddress().equals(receiverInfo.getLocalAddress())) {
            throw new ActiveMQAMQPInternalErrorException("Remote should have sent a matching Target address but we got: " + target.getAddress());
         }

         address = SimpleString.of(receiverInfo.getLocalAddress());
         defRoutingType = receiverInfo.getRoutingType();

         try {
            final AddressQueryResult result = sessionSPI.addressQuery(address, defRoutingType, false);

            // We initiated this link so the settings should refer to an address that definitely exists
            // however there is a chance the address was removed in the interim.
            if (!result.isExists()) {
               throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.addressDoesntExist(address.toString());
            }
         } catch (ActiveMQAMQPNotFoundException e) {
            throw e;
         } catch (Exception e) {
            logger.debug(e.getMessage(), e);
            throw new ActiveMQAMQPInternalErrorException(e.getMessage(), e);
         }

         flow();
      }

      @Override
      protected void actualDelivery(Message message, Delivery delivery, DeliveryAnnotations deliveryAnnotations, Receiver receiver, Transaction tx) {
         try {
            if (logger.isTraceEnabled()) {
               logger.trace("AMQP Bridge {} address receiver {} dispatching incoming message: {}", bridge.getName(), receiverInfo, message);
            }

            final Message theMessage = transformer.transform(message);

            if (theMessage != message && logger.isTraceEnabled()) {
               logger.trace("The transformer {} replaced the original message {} with a new instance {}",
                            transformer, message, theMessage);
            }

            sessionSPI.serverSend(this, tx, receiver, delivery, cachedAddress, routingContext, theMessage);
         } catch (Exception e) {
            logger.warn("Inbound delivery for {} encountered an error: {}", receiverInfo, e.getMessage(), e);
            deliveryFailed(delivery, receiver, e);
         }
      }
   }
}
