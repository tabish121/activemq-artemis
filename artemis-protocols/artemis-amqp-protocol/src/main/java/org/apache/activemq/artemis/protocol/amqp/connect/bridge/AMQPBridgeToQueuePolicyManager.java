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
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerQueuePlugin;
import org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeSenderInfo.Role;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPSessionContext;
import org.apache.activemq.artemis.utils.CompositeAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AMQP Bridge policy manager that tracks local queues that match the policy configurations
 * and creates senders to the remote peer for that address until such time as the queue is
 * removed locally.
 */
public class AMQPBridgeToQueuePolicyManager implements ActiveMQServerQueuePlugin {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private final ActiveMQServer server;
   private final AMQPBridgeManager bridge;
   private final AMQPBridgeQueuePolicy policy;
   private final Map<String, AMQPBridgeToQueueEntry> queueSenders = new HashMap<>();

   private volatile AMQPBridgeSenderConfiguration configuration;
   private volatile AMQPSessionContext session;
   private volatile boolean started;

   public AMQPBridgeToQueuePolicyManager(AMQPBridgeManager bridge, AMQPBridgeQueuePolicy policy) {
      Objects.requireNonNull(bridge, "The AMQP Bridge instance cannot be null");
      Objects.requireNonNull(policy, "The Queue match policy cannot be null");

      this.bridge = bridge;
      this.policy = policy;
      this.server = bridge.getServer();
   }

   /**
    * Start the queue policy manager which will initiate a scan of all broker queues
    * and create and matching remote senders. Start on a policy manager should only be
    * called after its parent {@link AMQPBridgeManager} is started and the remote
    * connection has been established.
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
         scanAllBindings();
      }
   }

   /**
    * Stops the queue policy manager which will close any open remote senders that are
    * active for local queue existence. Stop should generally be called whenever the parent
    * {@link AMQPBridgeManager} loses its connection to the remote.
    */
   public synchronized void stop() {
      if (started) {
         started = false;
         server.unRegisterBrokerPlugin(this);
         queueSenders.forEach((k, v) -> {
            if (v.hasSender()) {
               v.getSender().close();
            }
         });
         queueSenders.clear();
      }
   }

   @Override
   public void afterCreateQueue(Queue queue) throws ActiveMQException {
      if (started) {
         checkQueueForMatch(queue);
      }
   }

   @Override
   public void afterDestroyQueue(Queue queue, SimpleString address, final SecurityAuth session, boolean checkConsumerCount,
                                 boolean removeConsumers, boolean autoDeleteAddress) throws ActiveMQException {
      if (started) {
         final String fqqn = CompositeAddress.toFullyQualified(queue.getAddress(), queue.getName()).toString();
         final AMQPBridgeToQueueEntry entry = queueSenders.get(fqqn);

         if (entry != null) {
            // There is an entry for this queue so try and close any open sender
            tryRemoveSenderForQueue(entry, queue);
         }
      }
   }

   /**
    * Scans all bindings and push them through the normal bindings checks that
    * would be done on an add. We filter here based on whether diverts are enabled
    * just to reduce the result set but the check call should also filter as
    * during normal operations divert bindings could be added.
    */
   private void scanAllBindings() {
      server.getPostOffice()
            .getAllBindings()
            .filter(binding -> binding instanceof QueueBinding)
            .forEach(binding -> checkQueueForMatch(((QueueBinding) binding).getQueue()));
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
         entry = new AMQPBridgeToQueueEntry(info.getLocalAddress(), info.getLocalQueue());
         queueSenders.put(info.getLocalFqqn(), entry);
      }

      tryCreateBridgeSenderForQueue(entry, info);
   }

   private void tryCreateBridgeSenderForQueue(AMQPBridgeToQueueEntry entry, AMQPBridgeSenderInfo info) {
      if (!entry.hasSender()) {
         logger.trace("AMQP Bridge to Queue Policy manager creating remote sender for Queue: {}", entry.getFqqn());

         final AMQPBridgeSender queueSender = createBridgeSender(info);

         // Handle remote close with remove of receiver which means that no bridging of the Queue will
         // occur again until the Queue is removed and added back.
         queueSender.setRemoteClosedHandler((closedReceiver) -> {
            synchronized (this) {
               try {
                  final AMQPBridgeToQueueEntry tracked = queueSenders.get(info.getLocalFqqn());

                  if (tracked != null) {
                     tracked.clearSender();
                  }
               } finally {
                  closedReceiver.close();
               }
            }
         });

         entry.setSender(queueSender);

         queueSender.start();
      }
   }

   protected final void tryRemoveSenderForQueue(AMQPBridgeToQueueEntry entry, Queue queue) {
      if (entry != null) {
         logger.trace("Closing remote sender for bridged Queues {}", entry.getFqqn());

         if (entry.hasSender()) {
            final AMQPBridgeSender sender = entry.getSender();

            try {
               sender.close();
            } finally {
               queueSenders.remove(entry.getFqqn());
            }
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
      return new AMQPBridgeToQueueSender(bridge, configuration, session, senderInfo, policy);
   }
}
