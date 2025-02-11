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
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.postoffice.impl.DivertBinding;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerAddressPlugin;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeReceiverInfo.ReceiverRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AMQP Bridge policy manager that tracks local addresses that match the policy configurations
 * for local demand and creates receivers to the remote peer.
 */
public final class AMQPBridgeFromAddressPolicyManager extends AMQPBridgeFromPolicyManager implements ActiveMQServerAddressPlugin {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private final AMQPBridgeAddressPolicy policy;
   private final Map<String, AMQPBridgeFromAddressEntry> demandTracking = new HashMap<>();
   private final Map<DivertBinding, Set<QueueBinding>> divertsTracking = new HashMap<>();

   public AMQPBridgeFromAddressPolicyManager(AMQPBridgeManager bridge, AMQPBridgeMetrics metrics, AMQPBridgeAddressPolicy addressPolicy) {
      super(bridge, metrics, addressPolicy.getPolicyName(), AMQPBridgeType.BRIDGE_FROM_ADDRESS);

      Objects.requireNonNull(addressPolicy, "The Address match policy cannot be null");

      this.policy = addressPolicy;
   }

   @Override
   public AMQPBridgeAddressPolicy getPolicy() {
      return policy;
   }

   @Override
   protected void scanManagedResources() {
      if (configuration.isReceiverDemandTrackingDisabled()) {
         scanAllAddresses();
      } else {
         scanAllBindings();
      }
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
         divertsTracking.clear();
      }
   }

   @Override
   public synchronized void afterRemoveAddress(SimpleString address, AddressInfo addressInfo) throws ActiveMQException {
      if (isActive()) {
         final AMQPBridgeFromAddressEntry entry = demandTracking.remove(address.toString());

         if (entry != null) {
            // Demand is gone because the Address is gone and any in-flight messages can be
            // allowed to be released back to the remote as they will not be processed.
            // We removed the receiver information from demand tracking to prevent build up
            // of data for entries that may never return and to prevent interference from the
            // next set of events which will be the close of all local receivers for this now
            // removed Address.
            tryCloseBridgeReceiver(entry.releaseRecoveryHandler().clearReceiver());
         }
      }
   }

   @Override
   public synchronized void afterRemoveBinding(Binding binding, Transaction tx, boolean deleteData) throws ActiveMQException {
      if (isActive() && !configuration.isReceiverDemandTrackingDisabled()) {
         if (binding instanceof QueueBinding) {
            final AMQPBridgeFromAddressEntry entry = demandTracking.get(binding.getAddress().toString());

            if (entry != null) {
               // This is QueueBinding that was mapped to a bridged address so we can directly remove
               // demand from the bridge receiver and close it if demand is now gone.
               tryRemoveDemandOnAddress(entry, binding);
            } else if (policy.isIncludeDivertBindings()) {
               // See if there is any matching diverts that are forwarding to an address where this QueueBinding
               // is bound and remove the mapping for any matches, diverts can have a composite set of address
               // forwards so each divert must be checked in turn to see if it contains the address the removed
               // binding was bound to.
               divertsTracking.entrySet().forEach(divertEntry -> {
                  final String sourceAddress = divertEntry.getKey().getAddress().toString();
                  final SimpleString forwardAddress = divertEntry.getKey().getDivert().getForwardAddress();

                  if (isAddressInDivertForwards(binding.getAddress(), forwardAddress)) {
                     // Try and remove the queue binding from the set of registered bindings
                     // for the divert and if that removes all mapped bindings then we can
                     // remove the divert from the bridged address entry and check if that
                     // removed all local demand which means we can close the receiver.
                     divertEntry.getValue().remove(binding);

                     if (divertEntry.getValue().isEmpty()) {
                        tryRemoveDemandOnAddress(demandTracking.get(sourceAddress), divertEntry.getKey());
                     }
                  }
               });
            }
         } else if (policy.isIncludeDivertBindings() && binding instanceof DivertBinding) {
            final DivertBinding divert = (DivertBinding) binding;

            if (divertsTracking.remove(divert) != null) {
               // The divert binding is treated as one unit of demand on a bridged address and when
               // the divert is removed that unit of demand is removed regardless of existing bindings
               // still remaining on the divert forwards. If the divert demand was the only thing
               // keeping the bridge address receiver open this will result in it bring closed.
               try {
                  tryRemoveDemandOnAddress(demandTracking.get(divert.getAddress().toString()), divert);
               } catch (Exception e) {
                  logger.warn("Error looking up binding for divert forward address {}", divert.getDivert().getForwardAddress(), e);
               }
            }
         }
      }
   }

   private void tryRemoveDemandOnAddress(AMQPBridgeFromAddressEntry entry, Binding binding) {
      if (entry != null) {
         entry.removeDemand(binding);

         logger.trace("Reducing demand on bridged address {}, remaining demand? {}", entry.getLocalAddress(), entry.hasDemand());

         if (!entry.hasDemand()) {
            // A started receiver should be allowed to stop before possible close either because demand
            // is still not present or the remote did not respond before the configured stop timeout elapsed.
            // A successfully stopped receiver can be restarted but if the stop times out the receiver should
            // be closed and a new receiver created if demand is present. The completions occur on the connection
            // thread which requires the handler method to use synchronized to ensure thread safety.
            tryStopBridgeReceiver(entry.releaseRecoveryHandler());
         }
      }
   }

   private void tryStopBridgeReceiver(AMQPBridgeFromAddressEntry entry) {
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
      }
   }

   private synchronized void handleBridgeReceiverStopped(AMQPBridgeFromAddressEntry entry, boolean didStop) {
      final AMQPBridgeReceiver bridgeReceiver = entry.getReceiver();

      // Remote close or local address remove could have beaten us here and already cleaned up the receiver.
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
            demandTracking.remove(entry.getLocalAddress());
         }
      }
   }

   /**
    * When address demand tracking is disabled we just scan for any address the matches the policy and
    * create a receiver for that address, we don't monitor any other demand as we just want to receive
    * for as long as the lifetime of the address.
    */
   private void scanAllAddresses() {
      server.getPostOffice()
            .getAddresses()
            .stream()
            .map(address -> server.getAddressInfo(address))
            .filter(addressInfo -> testIfAddressMatchesPolicy(addressInfo))
            .forEach(addressInfo -> createOrUpdateAddressReceiverForUnboundDemand(addressInfo));
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
            .filter(binding -> binding instanceof QueueBinding || (policy.isIncludeDivertBindings() && binding instanceof DivertBinding))
            .forEach(binding -> afterAddBinding(binding));
   }

   @Override
   public synchronized void afterAddAddress(AddressInfo addressInfo, boolean reload) {
      if (isActive() && policy.test(addressInfo)) {
         // If demand tracking is disabled we create a receiver regardless of other demand
         if (configuration.isReceiverDemandTrackingDisabled()) {
            createOrUpdateAddressReceiverForUnboundDemand(addressInfo);
         } else if (policy.isIncludeDivertBindings()) {
            try {
               // A Divert can exist in configuration prior to the address having been auto created etc so
               // upon address add this check needs to be run to capture addresses that now match the divert.
               server.getPostOffice()
                     .getDirectBindings(addressInfo.getName())
                     .stream()
                     .filter(binding -> binding instanceof DivertBinding)
                     .forEach(this::checkBindingForMatch);
            } catch (Exception e) {
               logger.warn("Error looking up bindings for address {}.", addressInfo, e);
            }
         }
      }
   }

   @Override
   public synchronized void afterAddBinding(Binding binding) {
      if (isActive() && !configuration.isReceiverDemandTrackingDisabled()) {
         checkBindingForMatch(binding);
      }
   }

   /**
    * Called under lock this method should check if the given {@link Binding} matches the
    * configured address bridging policy and bridge the address if so. The incoming
    * {@link Binding} can be either a {@link QueueBinding} or a {@link DivertBinding} so
    * the code should check both.
    *
    * @param binding
    *       The binding that should be checked against the bridge from address policy,
    */
   private void checkBindingForMatch(Binding binding) {
      if (binding instanceof QueueBinding) {
         final QueueBinding queueBinding = (QueueBinding) binding;
         final AddressInfo addressInfo = server.getPostOffice().getAddressInfo(binding.getAddress());

         if (testIfAddressMatchesPolicy(addressInfo)) {
            createOrUpdateAddressReceiverForBinding(addressInfo, queueBinding);
         } else {
            reactIfQueueBindingMatchesAnyDivertTarget(queueBinding);
         }
      } else if (binding instanceof DivertBinding) {
         reactIfAnyQueueBindingMatchesDivertTarget((DivertBinding) binding);
      }
   }

   private void reactIfAnyQueueBindingMatchesDivertTarget(DivertBinding divertBinding) {
      if (!policy.isIncludeDivertBindings()) {
         return;
      }

      final AddressInfo addressInfo = server.getPostOffice().getAddressInfo(divertBinding.getAddress());

      if (!testIfAddressMatchesPolicy(addressInfo)) {
         return;
      }

      // We only need to check if we've never seen the divert before, afterwards we will
      // be checking it any time a new QueueBinding is added instead.
      if (divertsTracking.get(divertBinding) == null) {
         final Set<QueueBinding> matchingQueues = new HashSet<>();
         divertsTracking.put(divertBinding, matchingQueues);

         // We must account for the composite divert case by splitting the address and
         // getting the bindings on each one.
         final SimpleString forwardAddress = divertBinding.getDivert().getForwardAddress();
         final SimpleString[] forwardAddresses = forwardAddress.split(',');

         try {
            for (SimpleString forward : forwardAddresses) {
               server.getPostOffice().getBindingsForAddress(forward).getBindings()
                     .stream()
                     .filter(b -> b instanceof QueueBinding)
                     .map(b -> (QueueBinding) b)
                     .forEach(queueBinding -> {
                        matchingQueues.add(queueBinding);
                        createOrUpdateAddressReceiverForBinding(addressInfo, divertBinding);
                     });
            }
         } catch (Exception e) {
            // This logger has a bad name, the actual message is not federation specific.
            ActiveMQServerLogger.LOGGER.federationBindingsLookupError(forwardAddress, e);
         }
      }
   }

   private void reactIfQueueBindingMatchesAnyDivertTarget(QueueBinding queueBinding) {
      if (!policy.isIncludeDivertBindings()) {
         return;
      }

      final SimpleString queueAddress = queueBinding.getAddress();

      divertsTracking.entrySet().forEach((e) -> {
         final SimpleString forwardAddress = e.getKey().getDivert().getForwardAddress();
         final DivertBinding divertBinding = e.getKey();

         // Check matched diverts to see if the QueueBinding address matches the address or
         // addresses (composite diverts) of the Divert and if so then we can check if we need
         // to create demand on the source address on the remote if we haven't done so already.

         if (!e.getValue().contains(queueBinding) && isAddressInDivertForwards(queueAddress, forwardAddress)) {
            // Each divert that forwards to the address the queue is bound to we add demand
            // in the diverts tracker.
            e.getValue().add(queueBinding);

            final AddressInfo addressInfo = server.getPostOffice().getAddressInfo(divertBinding.getAddress());

            createOrUpdateAddressReceiverForBinding(addressInfo, divertBinding);
         }
      });
   }

   private static boolean isAddressInDivertForwards(final SimpleString targetAddress, final SimpleString forwardAddress) {
      final SimpleString[] forwardAddresses = forwardAddress.split(',');

      for (SimpleString forward : forwardAddresses) {
         if (targetAddress.equals(forward)) {
            return true;
         }
      }

      return false;
   }

   private void createOrUpdateAddressReceiverForUnboundDemand(AddressInfo addressInfo) {
      createOrUpdateAddressReceiverForBinding(addressInfo, null);
   }

   private void createOrUpdateAddressReceiverForBinding(AddressInfo addressInfo, Binding binding) {
      final String addressName = addressInfo.getName().toString();
      final AMQPBridgeFromAddressEntry entry;

      // Check for existing receiver add demand from a additional local consumer to ensure
      // the remote receiver remains active until all local demand is withdrawn.
      if (demandTracking.containsKey(addressName)) {
         entry = demandTracking.get(addressName);
      } else {
         entry = new AMQPBridgeFromAddressEntry(addressInfo);
         demandTracking.put(addressName, entry);
      }

      if (binding == null) {
         logger.trace("AMQP Bridge Address Policy matched on address: {} when demand tracking disabled", addressInfo);
         entry.forceDemand();
      } else {
         logger.trace("AMQP Bridge Address Policy matched on for demand on address: {} : binding: {}", addressInfo, binding);
         entry.addDemand(binding);
      }

      tryCreateBridgeReceiverForAddress(entry);
   }

   private void tryCreateBridgeReceiverForAddress(AMQPBridgeFromAddressEntry entry) {
      final AddressInfo addressInfo = entry.getAddressInfo();

      if (entry.hasDemand() && !entry.hasReceiver()) {
         logger.trace("AMQP Bridge from Address Policy manager creating remote receiver for address: {}", addressInfo);

         final AMQPBridgeReceiverInfo receiverInfo = createReceiverInfo(addressInfo);
         final AMQPBridgeReceiver addressReceiver = createBridgeReceiver(receiverInfo);

         // Handle remote open and cancel any additional link recovery attempts  Ensure that
         // thread safety is accounted for here as the notification come from the connection
         // thread.
         addressReceiver.setRemoteOpenHandler(openedReceiver -> {
            synchronized (AMQPBridgeFromAddressPolicyManager.this) {
               // We've connected so any existing recovery handler can now be closed and cleared
               // as we will create a new one if the link is forced closed by the remote and we
               // determine the outcome of that is not terminal to the connection.
               entry.releaseRecoveryHandler();
            }
         });

         // Handle remote close with remove of receiver which means that future demand will
         // attempt to create a new receiver for that demand but in the mean time we can also
         // attempt to recover the receiver if configured to do so. Ensure that thread safety
         // is accounted for here as the notification come from the connection thread.
         addressReceiver.setRemoteClosedHandler(closedReceiver -> {
            synchronized (AMQPBridgeFromAddressPolicyManager.this) {
               try {
                  final AMQPBridgeFromAddressEntry tracked = demandTracking.get(closedReceiver.getReceiverInfo().getLocalAddress());

                  if (tracked != null) {
                     tracked.clearReceiver();
                  }
               } finally {
                  closedReceiver.close();
               }

               if (configuration.isLinkRecoveryEnabled() && isActive()) {
                  // If the close came from a previous attempt that is itself a recovery we use the
                  // existing entry's recovery handler, otherwise we need to create a new handler
                  // to deal with link recovery and track attempt counts.
                  AMQPBridgeLinkRecoveryHandler<AMQPBridgeFromAddressEntry> recoveryHandler = entry.getRecoveryHandler();
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

         entry.setReceiver(addressReceiver);

         // Now that we are tracking it we can initialize it which will start it once
         // the link has fully attached.
         addressReceiver.initialize();
      }
   }

   private void tryRestartBridgeReceiverForAddress(AMQPBridgeFromAddressEntry entry) {
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
                     // have no idea what its state is now. Later new demand will trigger a new
                     // receiver attach attempt to get initiated.
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
            tryCreateBridgeReceiverForAddress(entry);
         }
      } else {
         // The receiver was likely closed because it didn't stop in time, create a new one and
         // let the normal setup process start bridging again.
         tryCreateBridgeReceiverForAddress(entry);
      }
   }

   private void linkRecoveryHandler(AMQPBridgeFromAddressEntry entry) {
      synchronized (this) {
         if (isActive()) {
            // This will check for existing demand and or an existing receiver
            // in order to prevent duplicate links so we don't need to check here.
            tryCreateBridgeReceiverForAddress(entry);
         }
      }
   }

   private AMQPBridgeReceiverInfo createReceiverInfo(AddressInfo address) {
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

      return new AMQPBridgeReceiverInfo(ReceiverRole.ADDRESS_RECEIVER,
                                        addressName,
                                        null,
                                        address.getRoutingType(),
                                        remoteAddressBuilder.toString(),
                                        policy.getFilter(),
                                        configuration.isReceiverPriorityDisabled() ? null : policy.getPriority());
   }

   private AMQPBridgeReceiver createBridgeReceiver(AMQPBridgeReceiverInfo receiverInfo) {
      Objects.requireNonNull(receiverInfo, "AMQP Bridge Address receiver information object was null");

      if (logger.isTraceEnabled()) {
         logger.trace("AMQP Bridge {} creating address receiver: {} for policy: {}", bridge.getName(), receiverInfo, policy.getPolicyName());
      }

      // Don't initiate anything yet as the caller might need to register error handlers etc
      // before the attach is sent otherwise they could miss the failure case.
      return new AMQPBridgeFromAddressReceiver(this, configuration, session, receiverInfo, policy, metrics.newReceiverMetrics());
   }

   private boolean testIfAddressMatchesPolicy(AddressInfo addressInfo) {
      if (!policy.test(addressInfo)) {
         return false;
      }

      // Address receivers can't pull as we have no real metric to indicate when / how much
      // we should pull so instead we refuse to match if credit set to zero.
      if (configuration.getReceiverCredits() <= 0) {
         logger.debug("AMQP Bridge address policy rejecting match on {} because credit is set to zero:", addressInfo.getName());
         return false;
      } else {
         return true;
      }
   }
}
