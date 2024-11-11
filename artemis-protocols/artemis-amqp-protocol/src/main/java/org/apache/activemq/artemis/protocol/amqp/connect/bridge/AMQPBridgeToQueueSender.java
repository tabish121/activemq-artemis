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
import java.lang.invoke.MethodHandles;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.AddressQueryResult;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.QueueQueryResult;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPLargeMessage;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPSessionCallback;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPInternalErrorException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPNotFoundException;
import org.apache.activemq.artemis.protocol.amqp.logger.ActiveMQAMQPProtocolMessageBundle;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPConnectionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPLargeMessageWriter;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPMessageWriter;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPSessionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.MessageWriter;
import org.apache.activemq.artemis.protocol.amqp.proton.ProtonServerSenderContext;
import org.apache.activemq.artemis.protocol.amqp.proton.SenderController;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sender type that handles sending messages sent to a local queue to a remote AMQP peer.
 */
public class AMQPBridgeToQueueSender extends AMQPBridgeSender {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public AMQPBridgeToQueueSender(AMQPBridgeManager bridge,
                                  AMQPBridgeSenderConfiguration configuration,
                                  AMQPSessionContext session,
                                  AMQPBridgeSenderInfo senderInfo,
                                  AMQPBridgeQueuePolicy policy) {
      super(bridge, configuration, session, senderInfo, policy);
   }

   @Override
   public AMQPBridgeQueuePolicy getPolicy() {
      return (AMQPBridgeQueuePolicy) policy;
   }

   @Override
   protected void asyncCloseSender() {
      connection.runLater(() -> {
         if (senderContext != null) {
            try {
               senderContext.close(null);
            } catch (ActiveMQAMQPException e) {
            } finally {
               senderContext = null;
            }
         }

         // Need to track the proton senderContext and close it here as the default
         // context implementation doesn't do that and could result in no detach
         // being sent in some cases and possible resources leaks.
         if (protonSender != null) {
            try {
               protonSender.close();
            } finally {
               protonSender = null;
            }
         }

         connection.flush();
      });
   }

   @Override
   protected void asyncCreateSender() {
      connection.runLater(() -> {
         if (closed) {
            return;
         }

         try {
            final Sender protonSender = session.getSession().sender(generateLinkName());
            final Target target = new Target();
            final Source source = new Source();
            final String address = senderInfo.getRemoteAddress();

            source.setAddress(senderInfo.getLocalFqqn());

            target.setAddress(address);
            target.setDurable(TerminusDurability.NONE);
            target.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);
            target.setCapabilities(getRemoteTerminusCapabilities());

            protonSender.setSenderSettleMode(configuration.isUsingPresettledSenders() ? SenderSettleMode.SETTLED : SenderSettleMode.UNSETTLED);
            protonSender.setReceiverSettleMode(ReceiverSettleMode.FIRST);
            protonSender.setTarget(target);
            protonSender.setSource(source);
            protonSender.open();

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

            this.protonSender = protonSender;

            protonSender.attachments().set(AMQP_LINK_INITIALIZER_KEY, Runnable.class, () -> {
               try {
                  if (openTimeoutTask != null) {
                     openTimeoutTask.cancel(false);
                  }

                  if (openTimedOut.get()) {
                     return;
                  }

                  final boolean linkOpened = protonSender.getRemoteTarget() != null;

                  if (linkOpened) {
                     logger.debug("AMQP Bridge {} queue senderContext {} completed open", bridge.getName(), senderInfo);
                  } else {
                     logger.debug("AMQP Bridge {} queue senderContext {} rejected by remote", bridge.getName(), senderInfo);
                  }

                  // Intercept remote close and check for valid reasons for remote closure such as
                  // the remote peer not having a matching node for this subscription or from an
                  // operator manually closing the link etc.
                  bridge.addLinkClosedInterceptor(senderInfo.getId(), this::remoteLinkClosedInterceptor);

                  senderContext = new AMQPBridgeQueueSenderContext(
                     connection, protonSender, session, session.getSessionSPI(), new AMQPBridgeQueueDeliverySender(senderInfo));

                  session.addSender(protonSender, senderContext);

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
             "-queue-sender-" + senderInfo.getRemoteAddress() +
             "-" + bridge.getServer().getNodeID();
   }

   private class AMQPBridgeQueueSenderContext extends ProtonServerSenderContext {

      AMQPBridgeQueueSenderContext(AMQPConnectionContext connection, Sender sender,
                                   AMQPSessionContext protonSession, AMQPSessionCallback server,
                                   SenderController senderController) {
         super(connection, sender, protonSession, server, senderController);
      }

      @Override
      public void close(boolean remoteLinkClose) throws ActiveMQAMQPException {
         super.close(remoteLinkClose);

         if (remoteLinkClose && remoteCloseHandler != null) {
            try {
               remoteCloseHandler.accept(AMQPBridgeToQueueSender.this);
            } catch (Exception e) {
               logger.debug("User remote closed handler threw error: ", e);
            } finally {
               remoteCloseHandler = null;
            }
         }
      }
   }

   private class AMQPBridgeQueueDeliverySender implements SenderController {

      private final AMQPBridgeSenderInfo senderInfo;

      // A cached AMQP standard message writers married to the server senderContext instance on initialization
      private AMQPMessageWriter standardMessageWriter;
      private AMQPLargeMessageWriter largeMessageWriter;

      AMQPBridgeQueueDeliverySender(AMQPBridgeSenderInfo senderInfo) {
         this.senderInfo = senderInfo;
      }

      @Override
      public void close(boolean remoteClose) {
         bridge.removeLinkClosedInterceptor(senderInfo.getId());
      }

      @Override
      public org.apache.activemq.artemis.core.server.Consumer init(ProtonServerSenderContext senderContext) throws Exception {
         this.standardMessageWriter = new AMQPMessageWriter(senderContext);
         this.largeMessageWriter = new AMQPLargeMessageWriter(senderContext);

         final AMQPSessionCallback sessionSPI = session.getSessionSPI();
         final SimpleString address = SimpleString.of(senderInfo.getLocalAddress());
         final SimpleString queue = SimpleString.of(senderInfo.getLocalQueue());
         final RoutingType defRoutingType = senderInfo.getRoutingType();
         final AMQPBridgeQueuePolicy policy = getPolicy();

         try {
            final AddressQueryResult addressResult = sessionSPI.addressQuery(address, defRoutingType, false);

            // We initiated this link so the settings should refer to an address that definitely exists
            // however there is a chance the address was removed in the interim.
            if (!addressResult.isExists()) {
               throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.addressDoesntExist(address.toString());
            }

            final QueueQueryResult queueResult = sessionSPI.queueQuery(queue, defRoutingType, false);

            // We initiated this link so the settings should refer to an queue that definitely exists
            // however there is a chance the address was removed in the interim.
            if (!queueResult.isExists()) {
               throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.addressDoesntExist(queue.toString());
            }
         } catch (ActiveMQAMQPNotFoundException e) {
            throw e;
         } catch (Exception e) {
            logger.debug(e.getMessage(), e);
            throw new ActiveMQAMQPInternalErrorException(e.getMessage(), e);
         }

         final int priority = policy.getPriority() != null ?
            policy.getPriority() : ActiveMQDefaultConfiguration.getDefaultConsumerPriority() + policy.getPriorityAdjustment();

         return sessionSPI.createSender(senderContext, queue, policy.getFilter(), false, priority);
      }

      @Override
      public MessageWriter selectOutgoingMessageWriter(ProtonServerSenderContext sender, MessageReference reference) {
         final MessageWriter selected;

         if (reference.getMessage() instanceof AMQPLargeMessage) {
            selected = largeMessageWriter;
         } else {
            selected = standardMessageWriter;
         }

         return selected;
      }
   }
}
