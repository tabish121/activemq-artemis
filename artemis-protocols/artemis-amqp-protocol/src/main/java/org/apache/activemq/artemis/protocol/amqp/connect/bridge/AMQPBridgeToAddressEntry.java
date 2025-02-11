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
import org.apache.activemq.artemis.core.server.impl.AddressInfo;

/**
 * An entry type class used to hold a {@link AMQPBridgeSender} and any other
 * state data needed by the manager that is creating them based on the policy
 * configuration for the AMQP bridge instance.
 */
public class AMQPBridgeToAddressEntry {

   private final AddressInfo addressInfo;

   private AMQPBridgeLinkRecoveryHandler<AMQPBridgeToAddressEntry> recoveryHandler;
   private AMQPBridgeSender sender;

   /**
    * Creates a new address entry for tracking demand on a bridged address
    *
    * @param addressInfo
    *        The address information object that this entry hold demand state for.
    */
   public AMQPBridgeToAddressEntry(AddressInfo addressInfo) {
      this.addressInfo = addressInfo;
   }

   /**
    * @return the address information that this entry is acting to bridge.
    */
   public AddressInfo getAddressInfo() {
      return addressInfo;
   }

   /**
    * @return the address that this entry is acting to bridge.
    */
   public String getLocalAddress() {
      return addressInfo.getName().toString();
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
    * @return this bridged address sender entry.
    */
   public AMQPBridgeToAddressEntry setSender(AMQPBridgeSender sender) {
      Objects.requireNonNull(sender, "Cannot assign a null sender to this entry, call clear to unset");
      this.sender = sender;
      return this;
   }

   /**
    * Clears the currently assigned sender from this entry.
    *
    * @return the sender that was stored here previously or null if none was set
    */
   public AMQPBridgeSender clearSender() {
      final AMQPBridgeSender taken = sender;

      this.sender = null;

      return taken;
   }

   /**
    * Assigns a recovery handler to this bridge entry which will handle scheduling recovery attempts
    *
    * @param recoveryHandler
    *       The recovery handler assigned to this entry
    *
    * @return this bridged sender entry.
    */
   public AMQPBridgeToAddressEntry setRecoveryHandler(AMQPBridgeLinkRecoveryHandler<AMQPBridgeToAddressEntry> recoveryHandler) {
      Objects.requireNonNull(recoveryHandler, "The recovery handler assigned cannot be null");
      this.recoveryHandler = recoveryHandler;
      return this;
   }

   /**
    * @return the assigned recovery handler or null if none currently active.
    */
   public AMQPBridgeLinkRecoveryHandler<AMQPBridgeToAddressEntry> getRecoveryHandler() {
      return recoveryHandler;
   }

   /**
    * @return <code>true</code> if the entry currently has an assigned recovery handler.
    */
   public boolean hasRecoveryHandler() {
      return recoveryHandler != null;
   }

   /**
    * Closes and clears any previously assigned link recovery handler.
    *
    * @return this bridged address sender entry.
    */
   public AMQPBridgeToAddressEntry releaseRecoveryHandler() {
      if (recoveryHandler != null) {
         try {
            recoveryHandler.close();
         } finally {
            recoveryHandler = null;
         }
      }

      return this;
   }
}
