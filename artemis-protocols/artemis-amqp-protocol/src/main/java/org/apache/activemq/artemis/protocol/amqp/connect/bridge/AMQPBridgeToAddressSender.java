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
import static org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport.CORE_MESSAGE_TUNNELING_SUPPORT;
import static org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport.DETACH_FORCED;
import static org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport.NOT_FOUND;
import static org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport.RESOURCE_DELETED;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.AddressQueryResult;
import org.apache.activemq.artemis.core.server.MessageReference;
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
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sender type that handles sending messages sent to a local address to a remote AMQP peer.
 */
public class AMQPBridgeToAddressSender implements AMQPBridgeSender {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private final AMQPBridgeManager bridge;
   private final AMQPBridgeSenderConfiguration configuration;
   private final AMQPBridgeSenderInfo senderInfo;
   private final AMQPBridgeAddressPolicy policy;
   private final AMQPConnectionContext connection;
   private final AMQPSessionContext session;

   private ProtonServerSenderContext senderContext;
   private Sender protonSender;
   private boolean started;
   private volatile boolean closed;
   private Consumer<AMQPBridgeSender> remoteOpenHandler;
   private Consumer<AMQPBridgeSender> remoteCloseHandler;

   public AMQPBridgeToAddressSender(AMQPBridgeManager bridge,
                                    AMQPBridgeSenderConfiguration configuration,
                                    AMQPSessionContext session,
                                    AMQPBridgeSenderInfo senderInfo,
                                    AMQPBridgeAddressPolicy policy) {
      this.bridge = bridge;
      this.senderInfo = senderInfo;
      this.policy = policy;
      this.connection = session.getAMQPConnectionContext();
      this.session = session;
      this.configuration = configuration;
   }

   @Override
   public void start() {
      if (!started && !closed) {
         started = true;
         asyncCreateSender();
      }
   }

   @Override
   public void close() {
      if (!closed) {
         closed = true;
         if (started) {
            started = false;
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
      }
   }

   @Override
   public AMQPBridgeManager getBridge() {
      return bridge;
   }

   @Override
   public AMQPBridgeSenderInfo getSenderInfo() {
      return senderInfo;
   }

   @Override
   public AMQPBridgeSender setRemoteOpenHandler(Consumer<AMQPBridgeSender> handler) {
      if (started) {
         throw new IllegalStateException("Cannot set a remote open handler after the senderContext is started");
      }

      this.remoteCloseHandler = handler;
      return this;
   }

   @Override
   public AMQPBridgeSender setRemoteClosedHandler(Consumer<AMQPBridgeSender> handler) {
      if (started) {
         throw new IllegalStateException("Cannot set a remote close handler after the senderContext is started");
      }

      this.remoteCloseHandler = handler;
      return this;
   }

   protected boolean remoteLinkClosedInterceptor(Link link) {
      if (link == protonSender && link.getRemoteCondition() != null && link.getRemoteCondition().getCondition() != null) {
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

   private String generateLinkName() {
      return "amqp-bridge-" + bridge.getName() +
             "-address-sender-" + senderInfo.getRemoteAddress() +
             "-" + bridge.getServer().getNodeID();
   }

   private Symbol[] getRemoteTerminusCapabilities() {
      if (policy.getRemoteTerminusCapabilities() != null && !policy.getRemoteTerminusCapabilities().isEmpty()) {
         return policy.getRemoteTerminusCapabilities().toArray(new Symbol[0]);
      } else {
         return null;
      }
   }

   private void asyncCreateSender() {
      connection.runLater(() -> {
         if (closed) {
            return;
         }

         try {
            final Sender protonSender = session.getSession().sender(generateLinkName());
            final Target target = new Target();
            final Source source = new Source();
            final String address = senderInfo.getRemoteAddress();

            source.setAddress(senderInfo.getLocalAddress());

            target.setAddress(address);
            target.setDurable(TerminusDurability.NONE);
            target.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);
            target.setCapabilities(getRemoteTerminusCapabilities());

            protonSender.setSenderSettleMode(configuration.isUsingPresettledSenders() ? SenderSettleMode.SETTLED : SenderSettleMode.UNSETTLED);
            protonSender.setReceiverSettleMode(ReceiverSettleMode.FIRST);
            // TODO: Support tunneling or not ?
            // If enabled offer core tunneling which we prefer to AMQP conversions of core as
            // the large ones will be converted to standard AMQP messages in memory. When not
            // offered the remote must not use core tunneling and AMQP conversion will be the
            // fallback. A non-Artemis remote peer should ignore this as it won't know what
            // core tunneling.
            if (configuration.isCoreMessageTunnelingEnabled()) {
               protonSender.setDesiredCapabilities(new Symbol[] {CORE_MESSAGE_TUNNELING_SUPPORT});
            }
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
                     logger.debug("AMQP Bridge {} address senderContext {} completed open", bridge.getName(), senderInfo);
                  } else {
                     logger.debug("AMQP Bridge {} address senderContext {} rejected by remote", bridge.getName(), senderInfo);
                  }

                  // Intercept remote close and check for valid reasons for remote closure such as
                  // the remote peer not having a matching node for this subscription or from an
                  // operator manually closing the link etc.
                  bridge.addLinkClosedInterceptor(senderInfo.getId(), this::remoteLinkClosedInterceptor);

                  senderContext = new AMQPBridgeAddressSenderContext(
                     connection, protonSender, session, session.getSessionSPI(), new AMQPBridgeAddressDeliverySender(senderInfo));

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

   private class AMQPBridgeAddressSenderContext extends ProtonServerSenderContext {

      AMQPBridgeAddressSenderContext(AMQPConnectionContext connection, Sender sender,
                                     AMQPSessionContext protonSession, AMQPSessionCallback server,
                                     SenderController senderController) {
         super(connection, sender, protonSession, server, senderController);
      }

      @Override
      public void close(boolean remoteLinkClose) throws ActiveMQAMQPException {
         super.close(remoteLinkClose);

         if (remoteLinkClose && remoteCloseHandler != null) {
            try {
               remoteCloseHandler.accept(AMQPBridgeToAddressSender.this);
            } catch (Exception e) {
               logger.debug("User remote closed handler threw error: ", e);
            } finally {
               remoteCloseHandler = null;
            }
         }
      }
   }

   private class AMQPBridgeAddressDeliverySender implements SenderController {

      private final AMQPBridgeSenderInfo senderInfo;

      // A cached AMQP standard message writers married to the server senderContext instance on initialization
      private AMQPMessageWriter standardMessageWriter;
      private AMQPLargeMessageWriter largeMessageWriter;

      AMQPBridgeAddressDeliverySender(AMQPBridgeSenderInfo senderInfo) {
         this.senderInfo = senderInfo;
      }

      @Override
      public void close() {
         bridge.removeLinkClosedInterceptor(senderInfo.getId());

         try {
            session.getSessionSPI().removeTemporaryQueue(SimpleString.of(senderInfo.getLocalQueue()));
         } catch (Exception e) {
            // Ignore on close, its temporary anyway and will be removed later
         }
      }

      @Override
      public org.apache.activemq.artemis.core.server.Consumer init(ProtonServerSenderContext senderContext) throws Exception {
         this.standardMessageWriter = new AMQPMessageWriter(senderContext);
         this.largeMessageWriter = new AMQPLargeMessageWriter(senderContext);

         final AMQPSessionCallback sessionSPI = session.getSessionSPI();
         final SimpleString address = SimpleString.of(senderInfo.getLocalAddress());
         final SimpleString queue = SimpleString.of(senderInfo.getLocalQueue());
         final RoutingType defRoutingType = senderInfo.getRoutingType();

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

         try {
            sessionSPI.createTemporaryQueue(address, queue, RoutingType.MULTICAST, SimpleString.of(policy.getFilter()), 1);
         } catch (Exception e) {
            throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorCreatingTemporaryQueue(e.getMessage());
         }

         return sessionSPI.createSender(senderContext, queue, null, false, policy.getPriority());
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
