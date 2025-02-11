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
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerConsumerPlugin;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeReceiverInfo.ReceiverRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AMQP Bridge policy manager that tracks local queues that match the policy configurations
 * for local demand and creates consumers to the remote peer configured.
 */
public final class AMQPBridgeFromQueuePolicyManager extends AMQPBridgeFromPolicyManager implements ActiveMQServerConsumerPlugin {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private final AMQPBridgeQueuePolicy policy;
   private final Map<AMQPBridgeReceiverInfo, AMQPBridgeFromQueueEntry> demandTracking = new HashMap<>();

   public AMQPBridgeFromQueuePolicyManager(AMQPBridgeManager bridge, AMQPBridgeMetrics metric, AMQPBridgeQueuePolicy policy) {
      super(bridge, metric, policy.getPolicyName(), AMQPBridgeType.BRIDGE_FROM_QUEUE);

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

   @Override
   protected void safeCleanupManagerResources(boolean force) {
      try {
         demandTracking.values().forEach((entry) -> {
            if (entry != null) {
               // Ensure that the entry stops tracking any demand and cancels any recovery tasks scheduled.
               entry.removeAllDemand().releaseRecoveryHandler();

               if (isConnected() && !force) {
                  tryStopBridgeReceiver(entry);
               } else {
                  tryCloseBridgeReceiver(entry.clearReceiver());
               }
            }
         });
      } finally {
         demandTracking.clear();
      }
   }

   @Override
   public synchronized void afterCreateConsumer(ServerConsumer consumer) {
      if (isActive() && !configuration.isReceiverDemandTrackingDisabled()) {
         reactIfQueueWithConsumerMatchesPolicy(consumer);
      }
   }

   @Override
   public synchronized void afterCloseConsumer(ServerConsumer consumer, boolean failed) {
      if (isActive() && !configuration.isReceiverDemandTrackingDisabled()) {
         final AMQPBridgeReceiverInfo receiverInfo = createReceiverInfo(consumer, consumer.getQueue());
         final AMQPBridgeFromQueueEntry entry = demandTracking.get(receiverInfo);

         if (entry == null) {
            return;
         }

         entry.removeDemand(consumer);

         logger.trace("Reducing demand on bridged queue {}, remaining demand? {}", receiverInfo.getLocalQueue(), entry.hasDemand());

         if (!entry.hasDemand()) {
            // A started receiver should be allowed to stop before possible close either because demand
            // is still not present or the remote did not respond before the configured stop timeout elapsed.
            // A successfully stopped receiver can be restarted but if the stop times out the receiver should
            // be closed and a new receiver created if demand is present.
            tryStopBridgeReceiver(entry.releaseRecoveryHandler());
         }
      }
   }

   @Override
   public void afterAddBinding(Binding binding) throws ActiveMQException {
      if (isActive() && configuration.isReceiverDemandTrackingDisabled() && binding instanceof QueueBinding) {
         reactIfQueueMatchesPolicy(((QueueBinding) binding).getQueue());
      }
   }

   @Override
   public synchronized void afterRemoveBinding(Binding binding, Transaction tx, boolean deleteData) throws ActiveMQException {
      if (isActive() && binding instanceof QueueBinding) {
         final String queueName = ((QueueBinding) binding).getQueue().getName().toString();

         demandTracking.values().removeIf((entry) -> {
            if (entry.getQueueName().equals(queueName)) {
               logger.trace("Bridged queue {} was removed, closing bridge receiver", queueName);

               // Demand is gone because the Queue binding is gone and any in-flight messages
               // can be allowed to be released back to the remote as they will not be processed.
               // We removed the receiver information from demand tracking to prevent build up
               // of data for entries that may never return and to prevent interference from the
               // next set of events which will be the close of all local receivers for this now
               // removed Queue.
               tryCloseBridgeReceiver(entry.removeAllDemand().releaseRecoveryHandler().clearReceiver());

               return true;
            } else {
               return false;
            }
         });
      }
   }

   private void tryStopBridgeReceiver(AMQPBridgeFromQueueEntry entry) {
      if (entry.hasReceiver()) {
         entry.getReceiver().stopAsync(new AMQPBridgeAsyncCompletion<AMQPBridgeReceiver>() {

            @Override
            public void onComplete(AMQPBridgeReceiver context) {
               handleBridgeReceiverStopped(entry, true);
            }

            @Override
            public void onException(AMQPBridgeReceiver context, Exception error) {
               logger.trace("Stop of bridge receiver {} failed, closing receiver: ", context, error);
               handleBridgeReceiverStopped(entry, false);
            }
         });
      } else {
         // There's no receiver but there might be a retry handler attempting to reconnect one
         // and since we are stopping the receiver we can also stop recovery efforts
         entry.releaseRecoveryHandler();
      }
   }

   private synchronized void handleBridgeReceiverStopped(AMQPBridgeFromQueueEntry entry, boolean didStop) {
      final AMQPBridgeReceiver bridgeReceiver = entry.getReceiver();

      // Remote close or local queue remove could have beaten us here and already cleaned up the receiver.
      if (bridgeReceiver != null) {
         // If the receiver has no demand or it didn't stop in time or some other error occurred we
         // assume the worst and close it here, the follow on code will recreate or cleanup as needed.
         if (!didStop || !entry.hasDemand()) {
            tryCloseBridgeReceiver(entry.releaseRecoveryHandler().clearReceiver());
         }

         // Demand may have returned while the receiver was stopping in which case
         // we either restart an existing stopped receiver or recreate if the stop
         // timed out and we closed it above. If there's still no demand then we
         // should remove it from demand tracking to reduce resource consumption.
         if (isActive() && entry.hasDemand()) {
            tryRestartBridgeReceiverForAddress(entry);
         } else {
            demandTracking.remove(entry.getReceiverInfo());
         }
      }
   }

   private void checkQueueWithConsumerForMatch(Queue queue) {
      queue.getConsumers()
           .stream()
           .filter(consumer -> consumer instanceof ServerConsumer)
           .map(c -> (ServerConsumer) c)
           .forEach(this::reactIfQueueWithConsumerMatchesPolicy);
   }

   private void reactIfQueueWithConsumerMatchesPolicy(ServerConsumer consumer) {
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

   private void reactIfQueueMatchesPolicy(Queue queue) {
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

   private void tryCreateBridgeReceiverForQueue(AMQPBridgeFromQueueEntry entry) {
      if (entry.hasDemand() && !entry.hasReceiver()) {
         logger.trace("AMQP Bridge from Queue Policy manager creating remote consumer for queue: {}", entry.getQueueName());

         final AMQPBridgeReceiverInfo receiverInfo = entry.getReceiverInfo();
         final AMQPBridgeReceiver queueReceiver = createBridgeReceiver(receiverInfo);

         // Handle remote open and cancel any additional link recovery attempts  Ensure that
         // thread safety is accounted for here as the notification come from the connection
         // thread.
         queueReceiver.setRemoteOpenHandler(openedReceiver -> {
            synchronized (this) {
               // We've connected so any existing recovery handler can now be closed and cleared
               // as we will create a new one if the link is forced closed by the remote and we
               // determine the outcome of that is not terminal to the connection.
               entry.releaseRecoveryHandler();
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

               if (configuration.isLinkRecoveryEnabled() && isActive()) {
                  // If the close came from a previous attempt that is itself a recovery we use the
                  // existing entry's recovery handler, otherwise we need to create a new handler
                  // to deal with link recovery.
                  AMQPBridgeLinkRecoveryHandler<AMQPBridgeFromQueueEntry> recoveryHandler = entry.getRecoveryHandler();
                  if (recoveryHandler == null) {
                     entry.setRecoveryHandler(
                        recoveryHandler = new AMQPBridgeLinkRecoveryHandler<>(entry, this::linkRecoveryHandler, configuration));
                  }

                  final boolean scheduled = recoveryHandler.tryScheduleNextRecovery(server.getScheduledPool());

                  if (!scheduled) {
                     entry.releaseRecoveryHandler();
                  }
               }
            }
         });

         entry.setReceiver(queueReceiver);

         // Now that we are tracking it we can initialize it which will start it once
         // the link has fully attached.
         queueReceiver.initialize();
      }
   }

   private void tryRestartBridgeReceiverForAddress(AMQPBridgeFromQueueEntry entry) {
      // There might be a receiver that was previously stopped due to demand having been
      // removed in which case we can attempt to recover it with a simple restart but if
      // that fails ensure the old receiver is closed and then attempt to recreate as we
      // know there is demand currently.
      if (entry.hasReceiver()) {
         final AMQPBridgeReceiver bridgeReceiver = entry.getReceiver();

         try {
            bridgeReceiver.startAsync(new AMQPBridgeAsyncCompletion<AMQPBridgeReceiver>() {

               @Override
               public void onComplete(AMQPBridgeReceiver context) {
                  logger.trace("Restarted bridge receiver after new demand added.");
               }

               @Override
               public void onException(AMQPBridgeReceiver context, Exception error) {
                  if (error instanceof IllegalStateException) {
                     // The receiver might be stopping or it could be closed, either of which
                     // was initiated from this manager so we can ignore and let those complete.
                     return;
                  } else {
                     // This is unexpected and our reaction is to close the consumer since we
                     // have no idea what its state is now. Later new demand or remote events
                     // will trigger a new consumer to get added.
                     logger.trace("Start of bridge receiver {} threw unexpected error, closing receiver: ", context, error);
                     tryCloseBridgeReceiver(entry.releaseRecoveryHandler().clearReceiver());
                  }
               }
            });
         } catch (Exception ex) {
            // The receiver might have been remotely closed, we can't be certain but since we
            // are responding to demand having been added we will close it and clear the entry
            // so that the follow on code can try and create a new one.
            logger.trace("Caught error on attempted restart of existing federation consumer", ex);
            tryCloseBridgeReceiver(entry.releaseRecoveryHandler().clearReceiver());
            tryCreateBridgeReceiverForQueue(entry);
         }
      } else {
         // The receiver was likely closed because it didn't stop in time, create a new one and
         // let the normal setup process start bridging again.
         tryCreateBridgeReceiverForQueue(entry);
      }
   }

   private void linkRecoveryHandler(AMQPBridgeFromQueueEntry entry) {
      synchronized (this) {
         if (isActive()) {
            // This will check for existing demand and or an existing receiver
            // in order to prevent duplicate links so we don't need to check here.
            tryCreateBridgeReceiverForQueue(entry);
         }
      }
   }

   private boolean testIfQueueMatchesPolicy(String address, String queueName) {
      return policy.test(address, queueName);
   }

   private AMQPBridgeReceiver createBridgeReceiver(AMQPBridgeReceiverInfo receiverInfo) {
      Objects.requireNonNull(receiverInfo, "AMQP Bridge Queue receiver information object was null");

      if (logger.isTraceEnabled()) {
         logger.trace("AMQP Bridge {} creating queue receiver: {} for policy: {}", bridge.getName(), receiverInfo, policy.getPolicyName());
      }

      // Don't initiate anything yet as the caller might need to register error handlers etc
      // before the attach is sent otherwise they could miss the failure case.
      return new AMQPBridgeFromQueueReceiver(this, configuration, session, receiverInfo, policy, metrics.newReceiverMetrics());
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

      return new AMQPBridgeReceiverInfo(ReceiverRole.QUEUE_RECEIVER,
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
