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

package org.apache.activemq.artemis.protocol.amqp.federation.internal;

import static org.apache.activemq.artemis.protocol.amqp.federation.FederationConstants.FEDERATION_NAME;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.filter.impl.FilterImpl;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.federation.Federation;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerBindingPlugin;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerConsumerPlugin;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationConsumer;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationConsumerInfo;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationReceiveFromQueuePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manager for a federation which has queue federation configuration which requires
 * monitoring broker queues for demand and creating a consumer for on the remote side
 * to federate messages back to this peer.
 */
public abstract class FederationQueuePolicyManager extends FederationPolicyManager implements ActiveMQServerConsumerPlugin, ActiveMQServerBindingPlugin {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   protected final Predicate<ServerConsumer> federationConsumerMatcher;
   protected final FederationReceiveFromQueuePolicy policy;
   protected final Map<FederationConsumerInfo, FederationQueueEntry> demandTracking = new HashMap<>();

   public FederationQueuePolicyManager(FederationInternal federation, FederationReceiveFromQueuePolicy queuePolicy) throws ActiveMQException {
      super(federation);

      Objects.requireNonNull(queuePolicy, "The Queue match policy cannot be null");

      this.policy = queuePolicy;
      this.federationConsumerMatcher = createFederationConsumerMatcher(server, queuePolicy);
   }

   /**
    * @return the receive from queue policy that backs the queue policy manager.
    */
   @Override
   public FederationReceiveFromQueuePolicy getPolicy() {
      return policy;
   }

   /**
    * Start the queue policy manager which will initiate a scan of all broker queue
    * bindings and create and matching remote receivers. Start on a policy manager
    * should only be called after its parent {@link Federation} is started and the
    * federation connection has been established.
    */
   public synchronized void start() {
      if (!federation.isStarted()) {
         throw new IllegalStateException("Cannot start a federation policy manager when the federation is stopped.");
      }

      if (!started) {
         started = true;
         handlePolicyManagerStarted(policy);
         server.registerBrokerPlugin(this);
         scanAllQueueBindings(); // Create consumers for existing queue with demand.
      }
   }

   /**
    * Stops the queue policy manager which will close any open remote receivers that are
    * active for local queue demand. Stop should generally be called whenever the parent
    * {@link Federation} loses its connection to the remote.
    */
   public void stop() {
      final Collection<FederationConsumerInternal> consumers;

      synchronized (this) {
         if (started) {
            // Ensures that on shutdown of a federation broker connection we don't leak
            // broker plugin instances.
            server.unRegisterBrokerPlugin(this);
            started = false;
            consumers = new ArrayList<>(demandTracking.size());
            demandTracking.values().forEach((entry) -> {
               if (entry != null && entry.removeAllDemand().hasConsumer()) {
                  consumers.add(entry.clearConsumer());
               }
            });
            demandTracking.clear();
         } else {
            consumers = Collections.emptyList();
         }
      }

      // Close these outside the instance lock as there could be races on remote close
      // or pending stops that deadlock calling into this manager in the opposite direction.
      // We've unlinked them from any state tracking that was previously held that might
      // cause a stopped instance from attempting to restart itself.
      consumers.forEach((consumer) -> {
         if (consumer != null) {
            try {
               consumer.close();
            } catch (Exception ex) {
               logger.trace("Igored exception on close of federation consumer during policy stop: ", ex);
            }
         }
      });
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
         final String queueName = consumer.getQueue().getName().toString();
         final FederationConsumerInfo consumerInfo = createConsumerInfo(consumer);
         final FederationQueueEntry entry = demandTracking.get(consumerInfo);

         if (entry == null) {
            return;
         }

         entry.removeDemand(consumer);

         logger.trace("Reducing demand on federated queue {}, remaining demand? {}", queueName, entry.hasDemand());

         if (!entry.hasDemand() && entry.hasConsumer()) {
            // A started consumer should be allowed to stop before possible close either because demand
            // is still not present or the remote did not respond before the configured stop timeout elapsed.
            // A successfully stopped receiver can be restarted but if the stop times out the receiver should
            // be closed and a new receiver created if demand is present.
            if (entry.getConsumer().isStarted()) {
               entry.getConsumer().stop((didStop) -> handleFederationConsumerStopped(consumer.getQueue(), entry, didStop));
            }
         }
      }
   }

   @Override
   public void afterRemoveBinding(Binding binding, Transaction tx, boolean deleteData) throws ActiveMQException {
      if (binding instanceof QueueBinding) {
         final QueueBinding queueBinding = (QueueBinding) binding;
         final String queueName = queueBinding.getQueue().getName().toString();
         final AtomicReference<FederationConsumerInternal> capture = new AtomicReference<>();

         synchronized (this) {
            demandTracking.values().removeIf((entry) -> {
               if (entry.getConsumerInfo().getQueueName().equals(queueName) && entry.hasConsumer()) {
                  logger.trace("Federated queue {} was remvoed, closing federation consumer", queueName);

                  // Capture the consumer and remove any tracked demand from the entry to
                  // ensure that a stop that was in progress doesn't attempt to recreate
                  // the consumer as it will report no demand and no longer carry a consumer.
                  capture.set(entry.removeAllDemand().clearConsumer());

                  return true;
               } else {
                  return false;
               }
            });
         }

         final FederationConsumerInternal federationConsuner = capture.get();

         // Demand is gone because the Queue binding is gone and any in-flight messages
         // can be allowed to be released back to the remote as they will not be processed.
         // We removed the consumer information from demand tracking to prevent build up
         // of data for entries that may never return and to prevent interference from the
         // next set of events which will be the close of all local consumers for this now
         // removed Queue.
         if (federationConsuner != null) {
            try {
               signalBeforeCloseFederationConsumer(federationConsuner);
               federationConsuner.close();
               signalAfterCloseFederationConsumer(federationConsuner);
            } catch (Exception ex) {
               logger.trace("Error suppressed during close of consumer for removed Queue: ", ex);
            }
         }
      }
   }

   private void handleFederationConsumerStopped(Queue queue, FederationQueueEntry entry, boolean didStop) {
      final FederationConsumerInternal federationConsuner = entry.getConsumer();

      // Remote close or local queue remove could have beaten us here and already cleaned up the consumer.
      if (federationConsuner != null) {
         if (!didStop || !entry.hasDemand()) {
            entry.clearConsumer();

            try {
               signalBeforeCloseFederationConsumer(federationConsuner);
               federationConsuner.close();
               signalAfterCloseFederationConsumer(federationConsuner);
            } catch (Exception ex) {
               logger.trace("Error suppressed during close of consumer after stopped: ", ex);
            }
         }

         // Demand may have returned while the consumer was stopping in which case
         // we either restart an existing stopped consumer or recreate if the stop
         // timed out and we closed it due to the stop having timed out.
         if (entry.hasDemand()) {
            tryCreateFederationConsumerForQueue(entry, queue);
         } else {
            demandTracking.remove(entry.getConsumerInfo());
         }
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
         // We should ignore federation consumers from remote peers but configuration does allow
         // these to be federated again for some very specific use cases so we check before then
         // moving onto any server plugin checks kick in.
         if (federationConsumerMatcher.test(consumer)) {
            return;
         }

         logger.trace("Federation Policy matched on consumer for binding: {}", consumer.getBinding());

         final FederationQueueEntry entry;
         final FederationConsumerInfo consumerInfo = createConsumerInfo(consumer);

         // Check for existing consumer add demand from a additional local consumer to ensure
         // the remote consumer remains active until all local demand is withdrawn.
         if (demandTracking.containsKey(consumerInfo)) {
            logger.trace("Federation Queue Policy manager found existing demand for queue: {}, adding demand", queueName);
            entry = demandTracking.get(consumerInfo);
         } else {
            demandTracking.put(consumerInfo, entry = createConsumerEntry(consumerInfo));
         }

         // Demand passed all binding plugin blocking checks so we track it, plugin can still
         // stop federation of the queue based on some external criteria but once it does
         // (if ever) allow it we will have tracked all allowed demand.
         entry.addDemand(consumer);

         tryCreateFederationConsumerForQueue(entry, consumer.getQueue());
      }
   }

   private void tryCreateFederationConsumerForQueue(FederationQueueEntry queueEntry, Queue queue) {
      if (queueEntry.hasDemand()) {
         // There might be a consumer that was previously stopped due to demand having been
         // removed in which case we can attempt to recover it with a simple restart but if
         // that fails ensure the old consumer is closed and then attempt to recreate as we
         // know there is demand currently.
         if (queueEntry.hasConsumer()) {
            final FederationConsumerInternal federationConsuner = queueEntry.getConsumer();

            if (federationConsuner.isStopping()) {
               return; // Allow stop to complete before restarting.
            } else if (federationConsuner.isStopped()) {
               try {
                  federationConsuner.start();
               } catch (Exception ex) {
                  logger.trace("Caught error on attempted restart of existing federation consumer", ex);
                  queueEntry.clearConsumer();

                  try {
                     signalBeforeCloseFederationConsumer(federationConsuner);
                     federationConsuner.close();
                     signalAfterCloseFederationConsumer(federationConsuner);
                  } catch (Exception ignore) {
                     logger.trace("Caught error on attempted close of existing federation consumer", ignore);
                  }
               }
            } else if (federationConsuner.isClosed()) {
               queueEntry.clearConsumer();
            }
         }

         if (!queueEntry.hasConsumer() && !isPluginBlockingFederationConsumerCreate(queue)) {
            logger.trace("Federation Queue Policy manager creating remote consumer for queue: {}", queueEntry.getQueueName());

            signalBeforeCreateFederationConsumer(queueEntry.getConsumerInfo());

            final FederationConsumerInternal queueConsumer = createFederationConsumer(queueEntry.getConsumerInfo());

            // Handle remote close with remove of consumer which means that future demand will
            // attempt to create a new consumer for that demand. Ensure that thread safety is
            // accounted for here as the notification can be asynchronous.
            queueConsumer.setRemoteClosedHandler((closedConsumer) -> {
               synchronized (this) {
                  try {
                     final FederationQueueEntry tracked = demandTracking.get(closedConsumer.getConsumerInfo());

                     if (tracked != null) {
                        tracked.clearConsumer();
                     }
                  } finally {
                     closedConsumer.close();
                  }
               }
            });

            queueEntry.setConsumer(queueConsumer);

            // Now that we are tracking it we can start it
            queueConsumer.start();

            signalAfterCreateFederationConsumer(queueConsumer);
         }
      }
   }

   /**
    * Checks if the remote queue added falls within the set of queues that match the
    * configured queue policy and if so scans for local demand on that queue to see
    * if a new attempt to federate the queue is needed.
    *
    * @param addressName
    *    The address that was added on the remote.
    * @param queueName
    *    The queue that was added on the remote.
    *
    * @throws Exception if an error occurs while processing the queue added event.
    */
   public synchronized void afterRemoteQueueAdded(String addressName, String queueName) throws Exception {
      // We ignore the remote address as locally the policy can be a wild card match and we can
      // try to federate based on the Queue only, if the remote rejects the federation consumer
      // binding again the request will once more be recorded and we will get another event if
      // the queue were recreated such that a match could be made. We retain all the current
      // demand and don't need to re-check the server state before trying to create the
      // remote queue consumer.
      if (started && testIfQueueMatchesPolicy(queueName)) {
         final Queue queue = server.locateQueue(queueName);

         if (queue != null) {
            demandTracking.forEach((k, v) -> {
               if (k.getQueueName().equals(queueName)) {
                  tryCreateFederationConsumerForQueue(v, queue);
               }
            });
         }
      }
   }

   /**
    * Performs the test against the configured queue policy to check if the target
    * queue and its associated address is a match or not. A subclass can override
    * this method and provide its own match tests in combination with the configured
    * matching policy.
    *
    * @param address
    *    The address that is being tested for a policy match.
    * @param queueName
    *    The name of the queue that is being tested for a policy match.
    *
    * @return <code>true</code> if the address given is a match against the policy.
    */
   protected boolean testIfQueueMatchesPolicy(String address, String queueName) {
      return policy.test(address, queueName);
   }

   /**
    * Performs the test against the configured queue policy to check if the target
    * queue minus its associated address is a match or not. A subclass can override
    * this method and provide its own match tests in combination with the configured
    * matching policy.
    *
    * @param queueName
    *    The name of the queue that is being tested for a policy match.
    *
    * @return <code>true</code> if the address given is a match against the policy.
    */
   protected boolean testIfQueueMatchesPolicy(String queueName) {
      return policy.testQueue(queueName);
   }

   /**
    * Create a new {@link FederationConsumerInfo} based on the given {@link ServerConsumer}
    * and the configured {@link FederationReceiveFromQueuePolicy}. A subclass must override this
    * method to return a consumer information object with additional data used be that implementation.
    *
    * @param consumer
    *    The {@link ServerConsumer} to use as a basis for the consumer information object.
    *
    * @return a new {@link FederationConsumerInfo} instance based on the server consumer
    */
   protected abstract FederationConsumerInfo createConsumerInfo(ServerConsumer consumer);

   /**
    * Creates a {@link FederationQueueEntry} instance that will be used to store an instance of
    * a {@link FederationConsumer} along with other state data needed to manage a federation consumer
    * instance. A subclass can override this method to return a more customized entry type with additional
    * state data.
    *
    * @param consumerInfo
    *    The consumer information that defines characteristics of the federation queue consumer
    *
    * @return a new {@link FederationQueueEntry} that holds the given queue name.
    */
   protected FederationQueueEntry createConsumerEntry(FederationConsumerInfo consumerInfo) {
      return new FederationQueueEntry(consumerInfo);
   }

   /**
    * Creates a {@link Predicate} that should return true if the given consumer is a federation
    * created consumer which should not be further federated.
    *
    * @param server
    *    The server instance for use in creating the filtering {@link Predicate}.
    * @param policy
    *    The configured Queue matching policy that can provide additional match criteria.
    *
    * @return a {@link Predicate} that will return true if the consumer should be filtered.
    *
    * @throws ActiveMQException if an error occurs while creating the new consumer filter.
    */
   protected Predicate<ServerConsumer> createFederationConsumerMatcher(ActiveMQServer server, FederationReceiveFromQueuePolicy policy) throws ActiveMQException {
      if (policy.isIncludeFederated()) {
         return (consumer) -> false; // Configuration says to federate these
      } else {
         // This filter matches on the same criteria as the original Core client based
         // Federation code which allows this implementation to see those consumers as
         // well as its own which in this methods implementation must also use this same
         // mechanism to mark federation resources.

         final Filter metaDataMatcher =
            FilterImpl.createFilter("\"" + FEDERATION_NAME + "\" IS NOT NULL");

         return (consumer) -> {
            final ServerSession serverSession = server.getSessionByID(consumer.getSessionID());

            if (serverSession != null && serverSession.getMetaData() != null) {
               return metaDataMatcher.match(serverSession.getMetaData());
            } else {
               return false;
            }
         };
      }
   }

   /**
    * Query all registered plugins for this federation instance to determine if any wish to
    * prevent a federation consumer from being created for the given Queue.
    *
    * @param queue
    *    The {@link Queue} that the federation queue manager is attempting to create a remote consumer for.
    *
    * @return true if any registered plugin signaled that creation should be suppressed.
    */
   protected abstract boolean isPluginBlockingFederationConsumerCreate(Queue queue);

}
