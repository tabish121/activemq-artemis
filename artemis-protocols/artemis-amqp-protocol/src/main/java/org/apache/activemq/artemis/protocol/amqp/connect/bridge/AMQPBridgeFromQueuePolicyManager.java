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

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerBindingPlugin;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerConsumerPlugin;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeReceiverInfo.Role;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPSessionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AMQP Bridge policy manager that tracks local queues that match the policy configurations
 * for local demand and creates consumers to the remote peer configured.
 */
public class AMQPBridgeFromQueuePolicyManager implements ActiveMQServerConsumerPlugin, ActiveMQServerBindingPlugin {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private final ActiveMQServer server;
   private final AMQPBridgeQueuePolicy policy;
   private final Map<AMQPBridgeReceiverInfo, AMQPBridgeFromQueueEntry> demandTracking = new HashMap<>();
   private final AMQPBridgeManager bridge;

   private volatile AMQPBridgeReceiverConfiguration configuration;
   private volatile AMQPSessionContext session;
   private volatile boolean started;

   public AMQPBridgeFromQueuePolicyManager(AMQPBridgeManager bridge, AMQPBridgeQueuePolicy policy) {
      Objects.requireNonNull(bridge, "The AMQP Bridge instance cannot be null");
      Objects.requireNonNull(policy, "The Queue match policy cannot be null");

      this.bridge = bridge;
      this.policy = policy;
      this.server = bridge.getServer();
   }

   /**
    * Start the queue policy manager which will initiate a scan of all broker queue
    * bindings and create and matching remote receivers. Start on a policy manager
    * should only be called after its parent {@link AMQPBridgeManager} is started and
    * the remote connection has been established.
    *
    * @param session
    *    The {@link AMQPSessionContext} that this policy uses for its consumers.
    * @param bridgeConfiguration
    *    The configuration of the bridge for this connection's lifetime.
    */
   public synchronized void start(AMQPSessionContext session, AMQPBridgeConfiguration bridgeConfiguration) {
      if (!started) {
         started = true;
         configuration = new AMQPBridgeReceiverConfiguration(bridgeConfiguration, policy.getProperties());
         this.session = session;
         server.registerBrokerPlugin(this);
         scanAllQueueBindings(); // Create consumers for existing queue with demand.
      }
   }

   /**
    * Stops the queue policy manager which will close any open remote receivers that are
    * active for local queue demand. Stop should generally be called whenever the parent
    * {@link AMQPBridgeManager} loses its connection to the remote.
    */
   public synchronized void stop() {
      if (started) {
         server.unRegisterBrokerPlugin(this);
         started = false;
         demandTracking.forEach((k, v) -> {
            if (v.hasConsumer()) {
               v.getConsumer().close();
            }
         });
         demandTracking.clear();
      }
   }

   @Override
   public synchronized void afterCreateConsumer(ServerConsumer consumer) {
      if (started) {
         reactIfConsumerMatchesPolicy(consumer);
      }
   }

   @Override
   public synchronized void afterCloseConsumer(ServerConsumer consumer, boolean failed) {
      if (started) {
         final AMQPBridgeReceiverInfo receiverInfo = createReceiverInfo(consumer);
         final AMQPBridgeFromQueueEntry entry = demandTracking.get(receiverInfo);

         if (entry == null) {
            return;
         }

         entry.removeDemand(consumer);

         logger.trace("Reducing demand on bridged queue {}, remaining demand? {}", receiverInfo.getLocalQueue(), entry.hasDemand());

         if (!entry.hasDemand() && entry.hasConsumer()) {
            final AMQPBridgeReceiver federationConsuner = entry.getConsumer();

            try {
               federationConsuner.close();
            } finally {
               demandTracking.remove(receiverInfo);
            }
         }
      }
   }

   @Override
   public synchronized void afterRemoveBinding(Binding binding, Transaction tx, boolean deleteData) throws ActiveMQException {
      if (binding instanceof QueueBinding) {
         final QueueBinding queueBinding = (QueueBinding) binding;
         final String queueName = queueBinding.getQueue().getName().toString();

         demandTracking.values().forEach((entry) -> {
            if (entry.getQueueName().equals(queueName) && entry.hasConsumer()) {
               entry.getConsumer().close();
            }
         });
      }
   }

   protected final void scanAllQueueBindings() {
      server.getPostOffice()
            .getAllBindings()
            .filter(b -> b instanceof QueueBinding)
            .map(b -> (QueueBinding) b)
            .forEach(b -> checkQueueForMatch(b.getQueue()));
   }

   protected final void checkQueueForMatch(Queue queue) {
      queue.getConsumers()
           .stream()
           .filter(consumer -> consumer instanceof ServerConsumer)
           .map(c -> (ServerConsumer) c)
           .forEach(this::reactIfConsumerMatchesPolicy);
   }

   protected final void reactIfConsumerMatchesPolicy(ServerConsumer consumer) {
      final String queueName = consumer.getQueue().getName().toString();

      if (testIfQueueMatchesPolicy(consumer.getQueueAddress().toString(), queueName)) {
         logger.trace("AMQP Bridge from Queue Policy matched on consumer for binding: {}", consumer.getBinding());

         final AMQPBridgeFromQueueEntry entry;
         final AMQPBridgeReceiverInfo receiverInfo = createReceiverInfo(consumer);

         // Check for existing receiver and add demand from a additional local consumer to ensure
         // the remote receiver remains active until all local demand is withdrawn.
         if (demandTracking.containsKey(receiverInfo)) {
            logger.trace("AMQP Bridge from Queue Policy manager found existing demand for queue: {}, adding demand", queueName);
            entry = demandTracking.get(receiverInfo);
         } else {
            demandTracking.put(receiverInfo, entry = new AMQPBridgeFromQueueEntry(queueName, receiverInfo));
         }

         entry.addDemand(consumer);

         tryCreateFederationConsumerForQueue(entry, consumer.getQueue());
      }
   }

   private void tryCreateFederationConsumerForQueue(AMQPBridgeFromQueueEntry queueEntry, Queue queue) {
      if (queueEntry.hasDemand() && !queueEntry.hasConsumer()) {
         logger.trace("AMQP Bridge from Queue Policy manager creating remote consumer for queue: {}", queueEntry.getQueueName());

         final AMQPBridgeReceiverInfo receiverInfo = queueEntry.getReceiverInfo();
         final AMQPBridgeReceiver queueReceiver = createBridgeReceiver(receiverInfo);

         // Handle remote close with remove of receiver which means that future demand will
         // attempt to create a new receiver for that demand. Ensure that thread safety is
         // accounted for here as the notification can be asynchronous.
         queueReceiver.setRemoteClosedHandler((closedReceiver) -> {
            synchronized (this) {
               try {
                  final AMQPBridgeFromQueueEntry tracked = demandTracking.get(receiverInfo);

                  if (tracked != null) {
                     tracked.clearReceiver();
                  }
               } finally {
                  closedReceiver.close();
               }
            }
         });

         queueEntry.setReceiver(queueReceiver);

         // Now that we are tracking it we can start it
         queueReceiver.start();
      }
   }

   private boolean testIfQueueMatchesPolicy(String address, String queueName) {
      return policy.test(address, queueName);
   }

   protected AMQPBridgeReceiver createBridgeReceiver(AMQPBridgeReceiverInfo receiverInfo) {
      Objects.requireNonNull(receiverInfo, "AMQP Bridge Queue receiver information object was null");

      if (logger.isTraceEnabled()) {
         logger.trace("AMQP Bridge {} creating queue receiver: {} for policy: {}", bridge.getName(), receiverInfo, policy.getPolicyName());
      }

      // Don't initiate anything yet as the caller might need to register error handlers etc
      // before the attach is sent otherwise they could miss the failure case.
      return new AMQPBridgeFromQueueReceiver(bridge, configuration, session, receiverInfo, policy);
   }

   private AMQPBridgeReceiverInfo createReceiverInfo(ServerConsumer consumer) {
      final Queue queue = consumer.getQueue();

      final StringBuilder remoteAddressBuilder = new StringBuilder();

      if (policy.getRemoteAddressPrefix() != null) {
         remoteAddressBuilder.append(policy.getRemoteAddressPrefix());
      }

      if (policy.getRemoteAddress() != null && !policy.getRemoteAddress().isBlank()) {
         remoteAddressBuilder.append(policy.getRemoteAddress());
      } else {
         remoteAddressBuilder.append(queue.getName().toString());
      }

      if (policy.getRemoteAddressSuffix() != null) {
         remoteAddressBuilder.append(policy.getRemoteAddressSuffix());
      }

      final String filterString = selectFilter(
         policy.getFilter(),
         configuration.isIgnoreQueueFilters() ? null : queue.getFilter(),
         configuration.isIgnoreSubscriptionFilters() ? null : consumer.getFilter());

      final Integer priority = selectPriority(consumer);

      return new AMQPBridgeReceiverInfo(Role.QUEUE_RECEIVER,
                                        queue.getAddress().toString(),
                                        queue.getName().toString(),
                                        queue.getRoutingType(),
                                        remoteAddressBuilder.toString(),
                                        filterString,
                                        priority);
   }

   private Integer selectPriority(ServerConsumer consumer) {
      if (policy.getPriority() != null) {
         return policy.getPriority();
      } else if (!configuration.isIgnoreSubscriptionPriorities()) {
         return consumer.getPriority() + policy.getPriorityAdjustment();
      } else {
         return ActiveMQDefaultConfiguration.getDefaultConsumerPriority() + policy.getPriorityAdjustment();
      }
   }

   private static String selectFilter(String policyFilter, Filter queueFilter, Filter consumerFilter) {
      if (policyFilter != null && !policyFilter.isBlank()) {
         return policyFilter;
      } else if (consumerFilter != null) {
         return consumerFilter.getFilterString().toString();
      } else {
         return queueFilter != null ? queueFilter.getFilterString().toString() : null;
      }
   }
}
