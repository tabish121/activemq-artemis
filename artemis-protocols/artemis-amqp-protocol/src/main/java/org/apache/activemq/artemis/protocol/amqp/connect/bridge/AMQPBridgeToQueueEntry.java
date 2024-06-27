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

import java.util.Objects;

import org.apache.activemq.artemis.utils.CompositeAddress;

/**
 * An entry type class used to hold a {@link AMQPBridgeSender} and any other
 * state data needed by the manager that is creating them based on the policy
 * configuration for the AMQP bridge instance.
 */
public class AMQPBridgeToQueueEntry {

   private final String queueName;
   private final String addressName;
   private final String fqqn;

   private AMQPBridgeSender sender;

   /**
    * Creates a new address entry for tracking demand on a bridged address
    *
    * @param queueName
    *       The name of the local Queue being bridged to.
    */
   public AMQPBridgeToQueueEntry(String addressName, String queueName) {
      this.addressName = addressName;
      this.queueName = queueName;
      this.fqqn = CompositeAddress.toFullyQualified(addressName, queueName).toString();
   }

   /**
    * @return the fully qualified queue name for the tracked queue demand.
    */
   public String getFqqn() {
      return fqqn;
   }

   /**
    * @return the name of the address that this entry tracks queue demand for.
    */
   public String getAddressName() {
      return addressName;
   }

   /**
    * @return the name of the queue that this entry tracks demand for.
    */
   public String getQueueName() {
      return queueName;
   }

   /**
    * @return <code>true</code> if a sender is currently set on this entry.
    */
   public boolean hasSender() {
      return sender != null;
   }

   /**
    * @return the sender managed by this entry
    */
   public AMQPBridgeSender getSender() {
      return sender;
   }

   /**
    * Sets the sender assigned to this entry to the given instance.
    *
    * @param sender
    *        The bridge sender that is currently active for this entry.
    *
    * @return this bridged queue sender entry.
    */
   public AMQPBridgeToQueueEntry setSender(AMQPBridgeSender sender) {
      Objects.requireNonNull(sender, "Cannot assign a null sender to this entry, call clear to unset");
      this.sender = sender;
      return this;
   }

   /**
    * Clears the currently assigned sender from this entry.
    *
    * @return this bridged queue sender entry.
    */
   public AMQPBridgeToQueueEntry clearSender() {
      this.sender = null;
      return this;
   }
}
