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

import java.io.Closeable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.apache.activemq.artemis.core.server.ServerConsumer;

/**
 * An queue entry type class used to hold a {@link AMQPBridgeReceiver} and any other
 * state data needed by the manager that is creating them based on the policy configuration
 * for the bridge instance.
 *
 * This entry type provides reference tracking state for current demand (bindings)
 * on a bridged resource such that it is not torn down until all demand has been
 * removed from the local resource.
 */
public class AMQPBridgeFromQueueEntry implements Closeable {

   private final AMQPBridgeReceiverInfo receiverInfo;
   private final Set<String> queueDemand = new HashSet<>();

   private AMQPBridgeLinkRecoveryHandler<AMQPBridgeFromQueueEntry> recoveryHandler;
   private boolean forceDemand;
   private AMQPBridgeReceiver receiver;

   /**
    * Creates a new queue entry with a single reference
    *
    * @param receiverInfo
    *       Receiver information object used to define the AMQP Bridge from queue receiver
    */
   public AMQPBridgeFromQueueEntry(AMQPBridgeReceiverInfo receiverInfo) {
      this.receiverInfo = receiverInfo;
   }

   @Override
   public void close() {
      if (recoveryHandler != null) {
         try {
            recoveryHandler.close();
         } catch (Exception e) {
            // Nothing to do at this point.
         } finally {
            recoveryHandler = null;
         }
      }

      if (receiver != null) {
         try {
            receiver.close();
         } catch (Exception e) {
            // Nothing to do at this point.
         } finally {
            receiver = null;
         }
      }
   }

   /**
    * @return the name of the queue that this entry tracks demand for.
    */
   public String getQueueName() {
      return receiverInfo.getLocalQueue();
   }

   /**
    * @return the receiver information that defines the properties of the AMQP bridge receiver.
    */
   public AMQPBridgeReceiverInfo getReceiverInfo() {
      return receiverInfo;
   }

   /**
    * @return <code>true</code> if a receiver is currently set on this entry.
    */
   public boolean hasConsumer() {
      return receiver != null;
   }

   /**
    * @return the receiver managed by this entry
    */
   public AMQPBridgeReceiver getConsumer() {
      return receiver;
   }

   /**
    * Sets the receiver assigned to this entry to the given instance.
    *
    * @param receiver
    *    The federation consumer that is currently active for this entry.
    *
    * @return this AMQP bridged queue entry instance.
    */
   public AMQPBridgeFromQueueEntry setReceiver(AMQPBridgeReceiver receiver) {
      Objects.requireNonNull(receiver, "Cannot assign a null consumer to this entry, call clear to unset");
      this.receiver = receiver;
      return this;
   }

   /**
    * Clears the currently assigned receiver from this entry.
    *
    * @return this AMQP bridged queue entry instance.
    */
   public AMQPBridgeFromQueueEntry clearReceiver() {
      this.receiver = null;
      return this;
   }

   /**
    * Assigns a recovery handler to this bridge entry which will handle scheduling recovery attempts
    *
    * @param recoveryHandler
    *       The recovery handler assigned to this entry
    *
    * @return this bridged receiver entry.
    */
   public AMQPBridgeFromQueueEntry setRecoveryHandler(AMQPBridgeLinkRecoveryHandler<AMQPBridgeFromQueueEntry> recoveryHandler) {
      Objects.requireNonNull(recoveryHandler, "The recovery handler assigned cannot be null");
      this.recoveryHandler = recoveryHandler;
      return this;
   }

   /**
    * @return the assigned recovery handler or null if none currently active.
    */
   public AMQPBridgeLinkRecoveryHandler<AMQPBridgeFromQueueEntry> getRecoveryHandler() {
      return recoveryHandler;
   }

   /**
    * @return <code>true</code> if the entry currently has an assigned recovery handler.
    */
   public boolean hasRecoveryHandler() {
      return recoveryHandler != null;
   }

   /**
    * Clears any previously assigned link recovery handler.
    *
    * @return this bridged receiver entry.
    */
   public AMQPBridgeFromQueueEntry clearRecoveryHandler() {
      this.recoveryHandler = null;
      return this;
   }

   /**
    * @return true if there are bindings that are mapped to this AMQP bridge entry.
    */
   public boolean hasDemand() {
      return forceDemand || !queueDemand.isEmpty();
   }

   /**
    * Forces this entry to report demand regardless of any server consumers having been added
    * to the demand tracking.
    *
    * @return this AMQP bridged queue entry instance.
    */
   public AMQPBridgeFromQueueEntry forceDemand() {
      this.forceDemand = true;
      return this;
   }

   /**
    * Add additional demand on the resource associated with this entries receiver.
    *
    * @param consumer
    *    The {@link ServerConsumer} that generated the demand on the bridged resource.
    *
    * @return this AMQP bridged queue entry instance.
    */
   public AMQPBridgeFromQueueEntry addDemand(ServerConsumer consumer) {
      queueDemand.add(identifyConsumer(consumer));
      return this;
   }

   /**
    * Remove the known demand on the resource from the given {@link ServerConsumer}.
    *
    * @param consumer
    *    The {@link ServerConsumer} that generated the demand on the queue.
    *
    * @return this AMQP bridged queue entry instance.
    */
   public AMQPBridgeFromQueueEntry removeDemand(ServerConsumer consumer) {
      queueDemand.remove(identifyConsumer(consumer));
      return this;
   }

   private static String identifyConsumer(ServerConsumer consumer) {
      return consumer.getConnectionID().toString() + ":" +
             consumer.getSessionID() + ":" +
             consumer.getID();
   }
}
