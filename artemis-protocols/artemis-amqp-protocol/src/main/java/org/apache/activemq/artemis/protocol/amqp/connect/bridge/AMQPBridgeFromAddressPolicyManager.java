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
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerAddressPlugin;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerBindingPlugin;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeReceiverInfo.Role;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPSessionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AMQP Bridge policy manager that tracks local addresses that match the policy configurations
 * for local demand and creates receivers to the remote peer.
 */
public class AMQPBridgeFromAddressPolicyManager implements ActiveMQServerBindingPlugin, ActiveMQServerAddressPlugin {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private final ActiveMQServer server;
   private final AMQPBridgeManager bridge;
   private final AMQPBridgeAddressPolicy policy;
   private final Map<String, AMQPBridgeFromAddressEntry> demandTracking = new HashMap<>();
   private final Map<DivertBinding, Set<QueueBinding>> divertsTracking = new HashMap<>();

   private volatile boolean started;
   private volatile AMQPBridgeReceiverConfiguration configuration;
   private volatile AMQPSessionContext session;

   public AMQPBridgeFromAddressPolicyManager(AMQPBridgeManager bridge, AMQPBridgeAddressPolicy addressPolicy) {
      Objects.requireNonNull(bridge, "The AMQP Bridge instance cannot be null");
      Objects.requireNonNull(addressPolicy, "The Address match policy cannot be null");

      this.bridge = bridge;
      this.policy = addressPolicy;
      this.server = bridge.getServer();
   }

   /**
    * Start the address policy manager which will initiate a scan of all broker divert
    * bindings and create and matching remote receivers. Start on a policy manager
    * should only be called after its parent {@link AMQPBridgeManager} is started and the
    * broker connection has been established.
    *
    * @param session
    *    The {@link AMQPSessionContext} that this policy uses for its receivers.
    * @param bridgeConfiguration
    *    The configuration of the bridge for this connection's lifetime.
    */
   public synchronized void start(AMQPSessionContext session, AMQPBridgeConfiguration bridgeConfiguration) {
      if (!started) {
         started = true;
         configuration = new AMQPBridgeReceiverConfiguration(bridgeConfiguration, policy.getProperties());
         this.session = session;
         server.registerBrokerPlugin(this);
         if (configuration.isReceiverDemandTrackingDisabled()) {
            scanAllAddresses();
         } else {
            scanAllBindings();
         }
      }
   }

   /**
    * Stops the address policy manager which will close any open remote receivers that are
    * active for local address demand. Stop should generally be called whenever the parent
    * {@link AMQPBridgeManager} loses its connection to the remote.
    */
   public synchronized void stop() {
      if (started) {
         started = false;
         server.unRegisterBrokerPlugin(this);
         demandTracking.forEach((k, v) -> {
            v.close();
         });
         demandTracking.clear();
         divertsTracking.clear();
      }
   }

   @Override
   public synchronized void afterRemoveAddress(SimpleString address, AddressInfo addressInfo) throws ActiveMQException {
      if (started) {
         final AMQPBridgeFromAddressEntry entry = demandTracking.remove(address.toString());

         if (entry != null) {
            entry.close();
         }
      }
   }

   @Override
   public synchronized void afterRemoveBinding(Binding binding, Transaction tx, boolean deleteData) throws ActiveMQException {
      if (started && !configuration.isReceiverDemandTrackingDisabled()) {
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

   protected final void tryRemoveDemandOnAddress(AMQPBridgeFromAddressEntry entry, Binding binding) {
      if (entry != null) {
         entry.removeDemand(binding);

         logger.trace("Reducing demand on bridged address {}, remaining demand? {}", entry.getLocalAddress(), entry.hasDemand());

         if (!entry.hasDemand()) {
            try {
               entry.close();
            } finally {
               demandTracking.remove(entry.getLocalAddress());
            }
         }
      }
   }

   /**
    * When address demand tracking is disabled we just scan for any address the matches the policy and
    * create a receiver for that address, we don't monitor any other demand as we just want to receive
    * for as long as the lifetime of the address.
    */
   protected final void scanAllAddresses() {
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
   protected final void scanAllBindings() {
      server.getPostOffice()
            .getAllBindings()
            .filter(binding -> binding instanceof QueueBinding || (policy.isIncludeDivertBindings() && binding instanceof DivertBinding))
            .forEach(binding -> afterAddBinding(binding));
   }

   @Override
   public synchronized void afterAddAddress(AddressInfo addressInfo, boolean reload) {
      if (started && policy.test(addressInfo)) {
         // If demand tracking is disabled we create a receiver regardless of other demand
         if (configuration.isReceiverDemandTrackingDisabled()) {
            createOrUpdateAddressReceiverForUnboundDemand(addressInfo);
         } else {
            if (policy.isIncludeDivertBindings()) {
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
   }

   @Override
   public synchronized void afterAddBinding(Binding binding) {
      if (started && !configuration.isReceiverDemandTrackingDisabled()) {
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
   protected final void checkBindingForMatch(Binding binding) {
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

   protected final void reactIfAnyQueueBindingMatchesDivertTarget(DivertBinding divertBinding) {
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

   protected final void reactIfQueueBindingMatchesAnyDivertTarget(QueueBinding queueBinding) {
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

   protected final void createOrUpdateAddressReceiverForUnboundDemand(AddressInfo addressInfo) {
      logger.trace("AMQP Bridge Address Policy matched on address: {} when demand tracking disabled", addressInfo);

      final String addressName = addressInfo.getName().toString();
      final AMQPBridgeFromAddressEntry entry;

      // Check for instance of this address and force demand, we should never find existing entry
      // but we handle it here as a best practice.
      if (demandTracking.containsKey(addressName)) {
         entry = demandTracking.get(addressName);
      } else {
         entry = new AMQPBridgeFromAddressEntry(addressInfo);
         demandTracking.put(addressName, entry);
      }

      entry.forceDemand();

      tryCreateBridgeReceiverForAddress(entry);
   }

   protected final void createOrUpdateAddressReceiverForBinding(AddressInfo addressInfo, Binding binding) {
      logger.trace("AMQP Bridge Address Policy matched on for demand on address: {} : binding: {}", addressInfo, binding);

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

      entry.addDemand(binding);

      tryCreateBridgeReceiverForAddress(entry);
   }

   private void tryCreateBridgeReceiverForAddress(AMQPBridgeFromAddressEntry addressEntry) {
      final AddressInfo addressInfo = addressEntry.getAddressInfo();

      if (addressEntry.hasDemand() && !addressEntry.hasReceiver()) {
         logger.trace("AMQP Brigde from Address Policy manager creating remote receiver for address: {}", addressInfo);

         final AMQPBridgeReceiverInfo receiverInfo = createReceiverInfo(addressInfo);
         final AMQPBridgeReceiver addressReceiver = createBridgeReceiver(receiverInfo);

         // Handle remote open and cancel any additional link recovery attempts  Ensure that
         // thread safety is accounted for here as the notification come from the connection
         // thread.
         addressReceiver.setRemoteOpenHandler(openedReceiver -> {
            synchronized (this) {
               final AMQPBridgeLinkRecoveryHandler<AMQPBridgeFromAddressEntry> recoveryHandler = addressEntry.getRecoveryHandler();

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
         // accounted for here as the notification come from the connection thread.
         addressReceiver.setRemoteClosedHandler(closedReceiver -> {
            synchronized (this) {
               try {
                  final AMQPBridgeFromAddressEntry tracked = demandTracking.get(closedReceiver.getReceiverInfo().getLocalAddress());

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
                  AMQPBridgeLinkRecoveryHandler<AMQPBridgeFromAddressEntry> recoveryHandler = addressEntry.getRecoveryHandler();
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

         addressEntry.setReceiver(addressReceiver);

         addressReceiver.start();
      }
   }

   protected final void linkRecoveryHandler(AMQPBridgeFromAddressEntry entry) {
      synchronized (this) {
         if (started) {
            // This will check for existing demand and or an existing receiver
            // in order to prevent duplicate links so we don't need to check here.
            tryCreateBridgeReceiverForAddress(entry);
         }
      }
   }

   protected AMQPBridgeReceiverInfo createReceiverInfo(AddressInfo address) {
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

      return new AMQPBridgeReceiverInfo(Role.ADDRESS_RECEIVER,
                                        addressName,
                                        null,
                                        address.getRoutingType(),
                                        remoteAddressBuilder.toString(),
                                        policy.getFilter(),
                                        configuration.isReceiverPriorityDisabled() ? null : policy.getPriority());
   }

   protected AMQPBridgeReceiver createBridgeReceiver(AMQPBridgeReceiverInfo receiverInfo) {
      Objects.requireNonNull(receiverInfo, "AMQP Bridge Address receiver information object was null");

      if (logger.isTraceEnabled()) {
         logger.trace("AMQP Bridge {} creating address receiver: {} for policy: {}", bridge.getName(), receiverInfo, policy.getPolicyName());
      }

      // Don't initiate anything yet as the caller might need to register error handlers etc
      // before the attach is sent otherwise they could miss the failure case.
      return new AMQPBridgeFromAddressReceiver(bridge, configuration, session, receiverInfo, policy);
   }

   protected boolean testIfAddressMatchesPolicy(AddressInfo addressInfo) {
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
