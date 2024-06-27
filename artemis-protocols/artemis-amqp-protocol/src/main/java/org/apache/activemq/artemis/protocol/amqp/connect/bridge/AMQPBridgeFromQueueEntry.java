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
public class AMQPBridgeFromQueueEntry {

   private final String queueName;
   private final AMQPBridgeReceiverInfo receiverInfo;
   private final Set<String> queueDemand = new HashSet<>();

   private AMQPBridgeReceiver receiver;

   /**
    * Creates a new queue entry with a single reference
    *
    * @param queueName
    *       The name of the local Queue being bridged.
    * @param receiverInfo
    *       Receiver information object used to define the AMQP Bridge from queue receiver
    */
   public AMQPBridgeFromQueueEntry(String queueName, AMQPBridgeReceiverInfo receiverInfo) {
      this.queueName = queueName;
      this.receiverInfo = receiverInfo;
   }

   /**
    * @return the name of the queue that this entry tracks demand for.
    */
   public String getQueueName() {
      return queueName;
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
    * @return true if there are bindings that are mapped to this AMQP bridge entry.
    */
   public boolean hasDemand() {
      return !queueDemand.isEmpty();
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
