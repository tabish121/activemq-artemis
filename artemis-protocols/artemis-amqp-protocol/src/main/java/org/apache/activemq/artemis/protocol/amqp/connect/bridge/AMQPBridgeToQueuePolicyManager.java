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

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.security.SecurityAuth;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerQueuePlugin;
import org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeSenderInfo.Role;
import org.apache.activemq.artemis.utils.CompositeAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AMQP Bridge policy manager that tracks local queues that match the policy configurations
 * and creates senders to the remote peer for that address until such time as the queue is
 * removed locally.
 */
public class AMQPBridgeToQueuePolicyManager extends AMQPBridgeToPolicyManager implements ActiveMQServerQueuePlugin {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private final AMQPBridgeQueuePolicy policy;
   private final Map<String, AMQPBridgeToQueueEntry> queueSenders = new HashMap<>();

   public AMQPBridgeToQueuePolicyManager(AMQPBridgeManager bridge, AMQPBridgeMetrics metrics, AMQPBridgeQueuePolicy policy) {
      super(bridge, metrics, policy.getPolicyName(), AMQPBridgeType.BRIDGE_TO_QUEUE);

      Objects.requireNonNull(policy, "The Queue match policy cannot be null");

      this.policy = policy;
   }

   /**
    * @return the policy that defines the bridged queue this policy manager monitors.
    */
   @Override
   public AMQPBridgeQueuePolicy getPolicy() {
      return policy;
   }

   @Override
   protected void scanManagedResources() {
      server.getPostOffice()
            .getAllBindings()
            .filter(binding -> binding instanceof QueueBinding)
            .forEach(binding -> checkQueueForMatch(((QueueBinding) binding).getQueue()));
   }

   @Override
   protected void safeCleanupManagerResources(boolean force) {
      try {
         queueSenders.forEach((k, v) -> {
            tryCloseBridgeSender(v.releaseRecoveryHandler().clearSender());
         });
      } finally {
         queueSenders.clear();
      }
   }

   @Override
   public void afterCreateQueue(Queue queue) throws ActiveMQException {
      if (isActive()) {
         checkQueueForMatch(queue);
      }
   }

   @Override
   public void afterDestroyQueue(Queue queue, SimpleString address, final SecurityAuth session, boolean checkConsumerCount,
                                 boolean removeConsumers, boolean autoDeleteAddress) throws ActiveMQException {
      if (isActive()) {
         final String fqqn = CompositeAddress.toFullyQualified(queue.getAddress(), queue.getName()).toString();
         final AMQPBridgeToQueueEntry entry = queueSenders.remove(fqqn);

         if (entry != null) {
            logger.trace("Clearing sender tracking for removed bridged Queues {}", entry.getLocalFqqn());
            tryCloseBridgeSender(entry.releaseRecoveryHandler().clearSender());
         }
      }
   }

   private boolean testIfQueueMatchesPolicy(String address, String queueName) {
      return policy.test(address, queueName);
   }

   protected final void checkQueueForMatch(Queue queue) {
      if (testIfQueueMatchesPolicy(queue.getAddress().toString(), queue.getName().toString())) {
         createOrUpdateQueueSenderForQueue(server.getPostOffice().getAddressInfo(queue.getAddress()), queue);
      }
   }

   protected final void createOrUpdateQueueSenderForQueue(AddressInfo addressInfo, Queue queue) {
      logger.trace("AMQP Bridge To Queue Policy matched on for demand on address: {} : binding: {}", addressInfo, queue);

      final AMQPBridgeToQueueEntry entry;
      final AMQPBridgeSenderInfo info = createSenderInfo(addressInfo, queue);

      // Check for existing receiver add demand from a additional local consumer to ensure
      // the remote receiver remains active until all local demand is withdrawn.
      if (queueSenders.containsKey(info.getLocalFqqn())) {
         entry = queueSenders.get(info.getLocalFqqn());
      } else {
         entry = new AMQPBridgeToQueueEntry(info);
         queueSenders.put(info.getLocalFqqn(), entry);
      }

      tryCreateBridgeSenderForQueue(entry);
   }

   private void tryCreateBridgeSenderForQueue(AMQPBridgeToQueueEntry queueEntry) {
      if (!queueEntry.hasSender()) {
         logger.trace("AMQP Bridge to Queue Policy manager creating remote sender for Queue: {}", queueEntry.getLocalFqqn());

         final AMQPBridgeSender queueSender = createBridgeSender(queueEntry.getSenderInfo());

         // Handle remote open and cancel any additional link recovery attempts. Ensure that
         // thread safety is accounted for here as the notification come from the connection
         // thread.
         queueSender.setRemoteOpenHandler(openedReceiver -> {
            synchronized (this) {
               // We've connected so any existing recovery handler can now be closed and cleared
               // as we will create a new one if the link is forced closed by the remote and we
               // determine the outcome of that is not terminal to the connection.
               queueEntry.releaseRecoveryHandler();
            }
         });

         // Handle remote close with remove of receiver which means that no bridging of the Queue will
         // occur again until the Queue is removed and added back.
         queueSender.setRemoteClosedHandler((closedSender) -> {
            synchronized (this) {
               try {
                  final AMQPBridgeToQueueEntry tracked = queueSenders.get(queueEntry.getLocalFqqn());

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
                  AMQPBridgeLinkRecoveryHandler<AMQPBridgeToQueueEntry> recoveryHandler = queueEntry.getRecoveryHandler();
                  if (recoveryHandler == null) {
                     queueEntry.setRecoveryHandler(
                        recoveryHandler = new AMQPBridgeLinkRecoveryHandler<>(queueEntry, this::linkRecoveryHandler, configuration));
                  }

                  final boolean scheduled = recoveryHandler.tryScheduleNextRecovery(server.getScheduledPool());

                  if (!scheduled) {
                     queueEntry.releaseRecoveryHandler();
                  }
               }
            }
         });

         queueEntry.setSender(queueSender);

         queueSender.start();
      }
   }

   protected final void linkRecoveryHandler(AMQPBridgeToQueueEntry entry) {
      synchronized (this) {
         if (isActive()) {
            // This will check for existing demand and or an existing sender
            // in order to prevent duplicate links so we don't need to check here.
            tryCreateBridgeSenderForQueue(entry);
         }
      }
   }

   private AMQPBridgeSenderInfo createSenderInfo(AddressInfo addressInfo, Queue queue) {
      final String addressName = addressInfo.getName().toString();
      final String queueName = queue.getName().toString();
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

      return new AMQPBridgeSenderInfo(Role.QUEUE_SENDER,
                                      addressName,
                                      queueName,
                                      addressInfo.getRoutingType(),
                                      remoteAddress);
   }

   private AMQPBridgeSender createBridgeSender(AMQPBridgeSenderInfo senderInfo) {
      Objects.requireNonNull(senderInfo, "AMQP Bridge Queue sender information object was null");

      if (logger.isTraceEnabled()) {
         logger.trace("AMQP Bridge {} creating Queue sender: {} for policy: {}", bridge.getName(), senderInfo, policy.getPolicyName());
      }

      // Don't initiate anything yet as the caller might need to register error handlers etc
      // before the attach is sent otherwise they could miss the failure case.
      return new AMQPBridgeToQueueSender(this, configuration, session, senderInfo, metrics.newSenderMetrics());
   }
}
