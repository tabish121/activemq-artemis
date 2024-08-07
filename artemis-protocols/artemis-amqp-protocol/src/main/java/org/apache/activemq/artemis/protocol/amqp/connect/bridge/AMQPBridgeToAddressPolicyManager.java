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

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerAddressPlugin;
import org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeSenderInfo.Role;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPSessionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AMQP Bridge policy manager that tracks local addresses that match the policy configurations
 * and creates senders to the remote peer for that address until such time as the address is
 * removed locally.
 */
public class AMQPBridgeToAddressPolicyManager implements ActiveMQServerAddressPlugin {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private final ActiveMQServer server;
   private final AMQPBridgeManager bridge;
   private final AMQPBridgeAddressPolicy policy;
   private final Map<String, AMQPBridgeToAddressEntry> addressTracking = new HashMap<>();

   private volatile AMQPBridgeSenderConfiguration configuration;
   private volatile AMQPSessionContext session;
   private volatile boolean started;

   public AMQPBridgeToAddressPolicyManager(AMQPBridgeManager bridge, AMQPBridgeAddressPolicy addressPolicy) {
      Objects.requireNonNull(bridge, "The AMQP Bridge instance cannot be null");
      Objects.requireNonNull(addressPolicy, "The Address match policy cannot be null");

      this.bridge = bridge;
      this.policy = addressPolicy;
      this.server = bridge.getServer();
   }

   /**
    * Start the address policy manager which will initiate a scan of all broker address
    * bindings and create and matching remote senders. Start on a policy manager
    * should only be called after its parent {@link AMQPBridgeManager} is started and
    * the remote connection has been established.
    *
    * @param session
    *    The {@link AMQPSessionContext} that this policy uses for its senders.
    * @param bridgeConfiguration
    *    The configuration of the bridge for this connection's lifetime.
    */
   public synchronized void start(AMQPSessionContext session, AMQPBridgeConfiguration bridgeConfiguration) {
      if (!started) {
         started = true;
         configuration = new AMQPBridgeSenderConfiguration(bridgeConfiguration, policy.getProperties());
         this.session = session;
         server.registerBrokerPlugin(this);
         scanAllAddresses();
      }
   }

   /**
    * Stops the address policy manager which will close any open remote senders that are
    * active for local queue existence. Stop should generally be called whenever the parent
    * {@link AMQPBridgeManager} loses its connection to the remote.
    */
   public synchronized void stop() {
      if (started) {
         started = false;
         addressTracking.forEach((k, v) -> {
            v.close();
         });
         addressTracking.clear();
         server.unRegisterBrokerPlugin(this);
      }
   }

   @Override
   public synchronized void afterAddAddress(AddressInfo addressInfo, boolean reload) {
      logger.info("Scanning added address: {}", addressInfo);
      if (started && policy.test(addressInfo)) {
         try {
            if (!addressTracking.containsKey(addressInfo.getName().toString())) {
               final AMQPBridgeToAddressEntry entry = new AMQPBridgeToAddressEntry(addressInfo);

               addressTracking.put(entry.getLocalAddress(), entry);

               tryCreateBridgeReceiverForAddress(entry);
            }
         } catch (Exception e) {
            logger.warn("Error looking up bindings for address {}.", addressInfo, e);
         }
      }
   }

   @Override
   public synchronized void afterRemoveAddress(SimpleString address, AddressInfo addressInfo) throws ActiveMQException {
      if (started) {
         final AMQPBridgeToAddressEntry entry = addressTracking.remove(address.toString());

         if (entry != null) {
            logger.trace("Closing remote sender for bridged Address {}", entry.getLocalAddress());
            entry.close();
         }
      }
   }

   private void scanAllAddresses() {
      server.getPostOffice()
            .getAddresses()
            .stream()
            .map(address -> server.getAddressInfo(address))
            .forEach(addressInfo -> afterAddAddress(addressInfo, false));
   }

   private void tryCreateBridgeReceiverForAddress(AMQPBridgeToAddressEntry addressEntry) {
      final AddressInfo addressInfo = addressEntry.getAddressInfo();

      if (!addressEntry.hasSender()) {
         logger.trace("AMQP Brigde from Address Policy manager creating remote receiver for address: {}", addressInfo);

         final AMQPBridgeSenderInfo senderInfo = createSenderInfo(addressInfo);
         final AMQPBridgeSender addressSender = createBridgeSender(senderInfo);

         // Handle remote open and cancel any additional link recovery attempts. Ensure that
         // thread safety is accounted for here as the notification come from the connection
         // thread.
         addressSender.setRemoteOpenHandler(openedReceiver -> {
            synchronized (this) {
               final AMQPBridgeLinkRecoveryHandler<AMQPBridgeToAddressEntry> recoveryHandler = addressEntry.getRecoveryHandler();

               // We've connected so any existing recovery handler can now be closed and cleared
               // as we will create a new one if the link is forced closed by the remote and we
               // determine the outcome of that is not terminal to the connection.
               if (recoveryHandler != null) {
                  try {
                     recoveryHandler.close();
                  } finally {
                     addressEntry.clearRecoveryHandler();
                  }
               }
            }
         });

         // Handle remote close with remove of receiver which means that future demand will
         // attempt to create a new receiver for that demand. Ensure that thread safety is
         // accounted for here as the notification can be asynchronous.
         addressSender.setRemoteClosedHandler((closedReceiver) -> {
            synchronized (this) {
               try {
                  final AMQPBridgeToAddressEntry tracked = addressTracking.get(addressEntry.getLocalAddress());

                  if (tracked != null) {
                     tracked.clearSender();
                  }
               } finally {
                  closedReceiver.close();
               }

               if (configuration.isLinkRecoveryEnabled()) {
                  // If the close came from a previous attempt that is itself a recovery we use the
                  // existing entry's recovery handler, otherwise we need to create a new handler
                  // to deal with link recovery.
                  AMQPBridgeLinkRecoveryHandler<AMQPBridgeToAddressEntry> recoveryHandler = addressEntry.getRecoveryHandler();
                  if (recoveryHandler == null) {
                     addressEntry.setRecoveryHandler(
                        recoveryHandler = new AMQPBridgeLinkRecoveryHandler<>(addressEntry, this::linkRecoveryHandler, configuration));
                  }

                  final boolean scheduled = recoveryHandler.tryScheduleNextRecovery(server.getScheduledPool());

                  if (!scheduled) {
                     try {
                        recoveryHandler.close();
                     } finally {
                        addressEntry.clearRecoveryHandler();
                     }
                  }
               }
            }
         });

         addressEntry.setSender(addressSender);

         addressSender.start();
      }
   }

   protected final void linkRecoveryHandler(AMQPBridgeToAddressEntry entry) {
      synchronized (this) {
         if (started) {
            // This will check for existing demand and or an existing sender
            // in order to prevent duplicate links so we don't need to check here.
            tryCreateBridgeReceiverForAddress(entry);
         }
      }
   }

   private AMQPBridgeSender createBridgeSender(AMQPBridgeSenderInfo senderInfo) {
      Objects.requireNonNull(senderInfo, "AMQP Bridge Address sender information object was null");

      if (logger.isTraceEnabled()) {
         logger.trace("AMQP Bridge {} creating address sender: {} for policy: {}", bridge.getName(), senderInfo, policy.getPolicyName());
      }

      // Don't initiate anything yet as the caller might need to register error handlers etc
      // before the attach is sent otherwise they could miss the failure case.
      return new AMQPBridgeToAddressSender(bridge, configuration, session, senderInfo, policy);
   }

   private String generateTempQueueName(String remoteAddress) {
      return "amqp-bridge-" + bridge.getName() +
             "-address-sender-to-" + remoteAddress +
             "-" + UUID.randomUUID().toString();
   }

   private AMQPBridgeSenderInfo createSenderInfo(AddressInfo address) {
      final String addressName = address.getName().toString();
      final StringBuilder remoteAddressBuilder = new StringBuilder();

      if (policy.getRemoteAddressPrefix() != null) {
         remoteAddressBuilder.append(policy.getRemoteAddressPrefix());
      }

      if (policy.getRemoteAddress() != null && !policy.getRemoteAddress().isBlank()) {
         remoteAddressBuilder.append(policy.getRemoteAddress());
      } else {
         remoteAddressBuilder.append(addressName);
      }

      if (policy.getRemoteAddressSuffix() != null) {
         remoteAddressBuilder.append(policy.getRemoteAddressSuffix());
      }

      final String remoteAddress = remoteAddressBuilder.toString();

      return new AMQPBridgeSenderInfo(Role.ADDRESS_SENDER,
                                      addressName,
                                      generateTempQueueName(remoteAddress),
                                      address.getRoutingType(),
                                      remoteAddress);
   }
}
