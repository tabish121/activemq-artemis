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
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerAddressPlugin;
import org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeSenderInfo.Role;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AMQP Bridge policy manager that tracks local addresses that match the policy configurations
 * and creates senders to the remote peer for that address until such time as the address is
 * removed locally.
 */
public class AMQPBridgeToAddressPolicyManager extends AMQPBridgeToPolicyManager implements ActiveMQServerAddressPlugin {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private final AMQPBridgeAddressPolicy policy;
   private final Map<String, AMQPBridgeToAddressEntry> addressTracking = new HashMap<>();

   public AMQPBridgeToAddressPolicyManager(AMQPBridgeManager bridge, AMQPBridgeMetrics metrics, AMQPBridgeAddressPolicy policy) {
      super(bridge, metrics, policy.getPolicyName(), AMQPBridgeType.BRIDGE_TO_ADDRESS);

      Objects.requireNonNull(policy, "The Address match policy cannot be null");

      this.policy = policy;
   }

   /**
    * @return the policy that defines the bridged address this policy manager monitors.
    */
   @Override
   public AMQPBridgeAddressPolicy getPolicy() {
      return policy;
   }

   @Override
   protected void scanManagedResources() {
      server.getPostOffice()
            .getAddresses()
            .stream()
            .map(address -> server.getAddressInfo(address))
            .forEach(addressInfo -> afterAddAddress(addressInfo, false));
   }

   @Override
   protected void safeCleanupManagerResources(boolean force) {
      try {
         addressTracking.forEach((k, v) -> {
            tryCloseBridgeSender(v.releaseRecoveryHandler().clearSender());
         });
      } finally {
         addressTracking.clear();
      }
   }

   @Override
   public synchronized void afterAddAddress(AddressInfo addressInfo, boolean reload) {
      if (isActive() && policy.test(addressInfo)) {
         try {
            if (!addressTracking.containsKey(addressInfo.getName().toString())) {
               final AMQPBridgeToAddressEntry entry = new AMQPBridgeToAddressEntry(addressInfo);

               addressTracking.put(entry.getLocalAddress(), entry);

               tryCreateBridgeSenderForAddress(entry);
            }
         } catch (Exception e) {
            logger.warn("Error looking up bindings for address {}.", addressInfo, e);
         }
      }
   }

   @Override
   public synchronized void afterRemoveAddress(SimpleString address, AddressInfo addressInfo) throws ActiveMQException {
      if (isActive()) {
         final AMQPBridgeToAddressEntry entry = addressTracking.remove(address.toString());

         if (entry != null) {
            logger.trace("Clearing sender tracking for removed bridged Address {}", entry.getLocalAddress());
            tryCloseBridgeSender(entry.releaseRecoveryHandler().clearSender());
         }
      }
   }

   private void tryCreateBridgeSenderForAddress(AMQPBridgeToAddressEntry entry) {
      final AddressInfo addressInfo = entry.getAddressInfo();

      if (!entry.hasSender()) {
         logger.trace("AMQP Bridge to Address Policy manager creating remote sender for address: {}", addressInfo);

         final AMQPBridgeSenderInfo senderInfo = createSenderInfo(addressInfo);
         final AMQPBridgeSender addressSender = createBridgeSender(senderInfo);

         // Handle remote open and cancel any additional link recovery attempts. Ensure that
         // thread safety is accounted for here as the notification come from the connection
         // thread.
         addressSender.setRemoteOpenHandler(openedSender -> {
            synchronized (this) {
               // We've connected so any existing recovery handler can now be closed and cleared
               // as we will create a new one if the link is forced closed by the remote and we
               // determine the outcome of that is not terminal to the connection.
               entry.releaseRecoveryHandler();
            }
         });

         // Handle remote close with remove of sender which means that future demand will
         // attempt to create a new sender for that demand. Ensure that thread safety is
         // accounted for here as the notification can be asynchronous.
         addressSender.setRemoteClosedHandler((closedSender) -> {
            synchronized (this) {
               try {
                  final AMQPBridgeToAddressEntry tracked = addressTracking.get(entry.getLocalAddress());

                  if (tracked != null) {
                     tracked.clearSender();
                  }
               } finally {
                  closedSender.close();
               }

               if (configuration.isLinkRecoveryEnabled() && isActive()) {
                  // If the close came from a previous attempt that is itself a recovery we use the
                  // existing entry's recovery handler, otherwise we need to create a new handler
                  // to deal with link recovery.
                  AMQPBridgeSenderRecoveryHandler<AMQPBridgeToAddressEntry> recoveryHandler = entry.getRecoveryHandler();
                  if (recoveryHandler == null) {
                     entry.setRecoveryHandler(
                        recoveryHandler = new AMQPBridgeSenderRecoveryHandler<>(entry, this::linkRecoveryHandler, configuration));
                  }

                  final boolean scheduled = recoveryHandler.tryScheduleNextRecovery(server.getScheduledPool());

                  if (!scheduled) {
                     entry.releaseRecoveryHandler();
                  }
               }
            }
         });

         entry.setSender(addressSender);

         addressSender.initialize();
      }
   }

   protected final void linkRecoveryHandler(AMQPBridgeToAddressEntry entry) {
      synchronized (this) {
         if (isActive()) {
            // This will check for existing demand and or an existing sender
            // in order to prevent duplicate links so we don't need to check here.
            tryCreateBridgeSenderForAddress(entry);
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
      return new AMQPBridgeToAddressSender(this, configuration, session, senderInfo, metrics.newSenderMetrics());
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
