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
import org.apache.qpid.proton.engine.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AMQP Bridge policy manager that tracks local queues that match the policy configurations
 * for local demand and creates consumers to the remote peer configured.
 */
public class AMQPBridgeFromQueuePolicyManager implements AMQPBridgePolicyManager, ActiveMQServerConsumerPlugin, ActiveMQServerBindingPlugin {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private final ActiveMQServer server;
   private final AMQPBridgeQueuePolicy policy;
   private final Map<AMQPBridgeReceiverInfo, AMQPBridgeFromQueueEntry> demandTracking = new HashMap<>();
   private final AMQPBridgeManager bridge;

   private volatile AMQPBridgeReceiverConfiguration configuration;
   private volatile AMQPSessionContext session;
   private volatile boolean started;
   private volatile boolean connected;

   public AMQPBridgeFromQueuePolicyManager(AMQPBridgeManager bridge, AMQPBridgeQueuePolicy policy) {
      Objects.requireNonNull(bridge, "The AMQP Bridge instance cannot be null");
      Objects.requireNonNull(policy, "The Queue match policy cannot be null");

      this.bridge = bridge;
      this.policy = policy;
      this.server = bridge.getServer();
   }

   /**
    * @return the policy that defines the bridged queue this policy manager monitors.
    */
   public AMQPBridgeQueuePolicy getPolicy() {
      return policy;
   }

   @Override
   public boolean isStarted() {
      return started;
   }

   /**
    * Start the queue policy manager which will initiate a scan of all broker queue
    * bindings and create and matching remote receivers. Start on a policy manager
    * should only be called after its parent {@link AMQPBridgeManager} is started.
    *
    * @throws ActiveMQException if an error occurs while starting the policy manager.
    */
   @Override
   public synchronized void start() throws ActiveMQException {
      if (!started && bridge.isStarted()) {
         started = true;
         if (connected) {
            startManagerServices();
         }
      }
   }

   /**
    * Stops the queue policy manager which will close any open remote receivers that are
    * active for local queue demand.
    */
   @Override
   public synchronized void stop() {
      if (started) {
         started = false;
         stopManagerServices();
      }
   }

   /**
    * Called by the parent AMQP bridge manager when the connection has failed and this AMQP policy
    * manager should tear down any active resources and await a reconnect if one is allowed.
    */
   @Override
   public synchronized void connectionDropped() {
      connected = false;

      if (started) {
         stopManagerServices();
      }
   }

   /**
    * Called by the parent AMQP bridge manager when the connection has been established and this
    * AMQP policy manager should build up its active state based on the configuration.
    *
    * @param session
    *    The new {@link Session} that was created for use by broker connection resources.
    * @param configuration
    *    The bridge configuration that hold state relative to the new active connection.
    *
    * @throws ActiveMQException if an error occurs processing the connection restored event
    */
   @Override
   public synchronized void connectionRestored(AMQPSessionContext session, AMQPBridgeConfiguration configuration) throws ActiveMQException {
      this.connected = true;
      this.configuration = new AMQPBridgeReceiverConfiguration(configuration, policy.getProperties());
      this.session = session;

      if (started) {
         startManagerServices();
      }
   }

   private void stopManagerServices() {
      server.unRegisterBrokerPlugin(this);
      demandTracking.forEach((k, v) -> {
         v.close();
      });
      demandTracking.clear();
   }

   private void startManagerServices() {
      server.registerBrokerPlugin(this);
      scanAllQueueBindings();
   }

   @Override
   public synchronized void afterCreateConsumer(ServerConsumer consumer) {
      if (started && !configuration.isReceiverDemandTrackingDisabled()) {
         reactIfQueueWithConsumerMatchesPolicy(consumer);
      }
   }

   @Override
   public synchronized void afterCloseConsumer(ServerConsumer consumer, boolean failed) {
      if (started && !configuration.isReceiverDemandTrackingDisabled()) {
         final AMQPBridgeReceiverInfo receiverInfo = createReceiverInfo(consumer, consumer.getQueue());
         final AMQPBridgeFromQueueEntry entry = demandTracking.get(receiverInfo);

         if (entry == null) {
            return;
         }

         entry.removeDemand(consumer);

         logger.trace("Reducing demand on bridged queue {}, remaining demand? {}", receiverInfo.getLocalQueue(), entry.hasDemand());

         if (!entry.hasDemand()) {
            try {
               entry.close();
            } finally {
               demandTracking.remove(receiverInfo);
            }
         }
      }
   }

   @Override
   public void afterAddBinding(Binding binding) throws ActiveMQException {
      if (started && configuration.isReceiverDemandTrackingDisabled() && binding instanceof QueueBinding) {
         reactIfQueueMatchesPolicy(((QueueBinding) binding).getQueue());
      }
   }

   @Override
   public synchronized void afterRemoveBinding(Binding binding, Transaction tx, boolean deleteData) throws ActiveMQException {
      if (started && binding instanceof QueueBinding) {
         final String queueName = ((QueueBinding) binding).getQueue().getName().toString();

         demandTracking.values().removeIf(entry -> {
            final boolean remove = entry.getQueueName().equals(queueName);

            if (remove) {
               entry.close();
            }

            return remove;
         });
      }
   }

   protected final void scanAllQueueBindings() {
      server.getPostOffice()
            .getAllBindings()
            .filter(b -> b instanceof QueueBinding)
            .map(b -> (QueueBinding) b)
            .forEach(b -> {
               if (configuration.isReceiverDemandTrackingDisabled()) {
                  reactIfQueueMatchesPolicy(b.getQueue());
               } else {
                  checkQueueWithConsumerForMatch(b.getQueue());
               }
            });
   }

   protected final void checkQueueWithConsumerForMatch(Queue queue) {
      queue.getConsumers()
           .stream()
           .filter(consumer -> consumer instanceof ServerConsumer)
           .map(c -> (ServerConsumer) c)
           .forEach(this::reactIfQueueWithConsumerMatchesPolicy);
   }

   protected final void reactIfQueueWithConsumerMatchesPolicy(ServerConsumer consumer) {
      final String queueName = consumer.getQueue().getName().toString();
      final String addressName = consumer.getQueueAddress().toString();

      if (testIfQueueMatchesPolicy(addressName, queueName)) {
         logger.trace("AMQP Bridge from Queue Policy matched on consumer for binding: {}", consumer.getBinding());

         final AMQPBridgeFromQueueEntry entry;
         final AMQPBridgeReceiverInfo receiverInfo = createReceiverInfo(consumer, consumer.getQueue());

         // Check for existing receiver and add demand from a additional local consumer to ensure
         // the remote receiver remains active until all local demand is withdrawn.
         if (demandTracking.containsKey(receiverInfo)) {
            logger.trace("AMQP Bridge from Queue Policy manager found existing demand for queue: {}, adding demand", queueName);
            entry = demandTracking.get(receiverInfo);
         } else {
            demandTracking.put(receiverInfo, entry = new AMQPBridgeFromQueueEntry(receiverInfo));
         }

         entry.addDemand(consumer);

         tryCreateBridgeReceiverForQueue(entry);
      }
   }

   protected final void reactIfQueueMatchesPolicy(Queue queue) {
      final String queueName = queue.getName().toString();
      final String addressName = queue.getAddress().toString();

      if (testIfQueueMatchesPolicy(addressName, queueName)) {
         logger.trace("AMQP Bridge from Queue Policy matched on Queue: {}", queueName);

         final AMQPBridgeFromQueueEntry entry;
         final AMQPBridgeReceiverInfo receiverInfo = createReceiverInfo(null, queue);

         // Check for existing receiver and add demand which shouldn't actually happen but as a safety check
         // we go ahead and handle it here just as a best practice.
         if (demandTracking.containsKey(receiverInfo)) {
            logger.trace("AMQP Bridge from Queue Policy manager found existing demand for queue: {}, adding demand", queueName);
            entry = demandTracking.get(receiverInfo);
         } else {
            demandTracking.put(receiverInfo, entry = new AMQPBridgeFromQueueEntry(receiverInfo));
         }

         entry.forceDemand();

         tryCreateBridgeReceiverForQueue(entry);
      }
   }

   private void tryCreateBridgeReceiverForQueue(AMQPBridgeFromQueueEntry queueEntry) {
      if (queueEntry.hasDemand() && !queueEntry.hasConsumer()) {
         logger.trace("AMQP Bridge from Queue Policy manager creating remote consumer for queue: {}", queueEntry.getQueueName());

         final AMQPBridgeReceiverInfo receiverInfo = queueEntry.getReceiverInfo();
         final AMQPBridgeReceiver queueReceiver = createBridgeReceiver(receiverInfo);

         // Handle remote open and cancel any additional link recovery attempts  Ensure that
         // thread safety is accounted for here as the notification come from the connection
         // thread.
         queueReceiver.setRemoteOpenHandler(openedReceiver -> {
            synchronized (this) {
               final AMQPBridgeLinkRecoveryHandler<AMQPBridgeFromQueueEntry> recoveryHandler = queueEntry.getRecoveryHandler();

               // We've connected so any existing recovery handler can now be closed and cleared
               // as we will create a new one if the link is forced closed by the remote and we
               // determine the outcome of that is not terminal to the connection.
               if (recoveryHandler != null) {
                  try {
                     recoveryHandler.close();
                  } finally {
                     queueEntry.clearRecoveryHandler();
                  }
               }
            }
         });

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

               if (configuration.isLinkRecoveryEnabled()) {
                  // If the close came from a previous attempt that is itself a recovery we use the
                  // existing entry's recovery handler, otherwise we need to create a new handler
                  // to deal with link recovery.
                  AMQPBridgeLinkRecoveryHandler<AMQPBridgeFromQueueEntry> recoveryHandler = queueEntry.getRecoveryHandler();
                  if (recoveryHandler == null) {
                     queueEntry.setRecoveryHandler(
                        recoveryHandler = new AMQPBridgeLinkRecoveryHandler<>(queueEntry, this::linkRecoveryHandler, configuration));
                  }

                  final boolean scheduled = recoveryHandler.tryScheduleNextRecovery(server.getScheduledPool());

                  if (!scheduled) {
                     try {
                        recoveryHandler.close();
                     } finally {
                        queueEntry.clearRecoveryHandler();
                     }
                  }
               }
            }
         });

         queueEntry.setReceiver(queueReceiver);

         // Now that we are tracking it we can start it
         queueReceiver.start();
      }
   }

   protected final void linkRecoveryHandler(AMQPBridgeFromQueueEntry entry) {
      synchronized (this) {
         if (started) {
            // This will check for existing demand and or an existing receiver
            // in order to prevent duplicate links so we don't need to check here.
            tryCreateBridgeReceiverForQueue(entry);
         }
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

   private AMQPBridgeReceiverInfo createReceiverInfo(ServerConsumer consumer, Queue queue) {
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
         configuration.isIgnoreSubscriptionFilters() || consumer == null ? null : consumer.getFilter());

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
      if (configuration.isReceiverPriorityDisabled()) {
         return null;
      } else if (policy.getPriority() != null) {
         return policy.getPriority();
      } else if (!configuration.isIgnoreSubscriptionPriorities() && consumer != null) {
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
