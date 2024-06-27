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

import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;

/**
 * An entry type class used to hold a {@link AMQPBridgeReceiver} and any other
 * state data needed by the manager that is creating them based on the policy
 * configuration for the AMQP bridge instance.
 *
 * This entry type provides reference tracking state for current demand (bindings)
 * on a bridged resource such that it is not torn down until all demand has been
 * removed from the local resource.
 */
public class AMQPBridgeFromAddressEntry {

   private final AddressInfo addressInfo;
   private final Set<Binding> demandBindings = new HashSet<>();

   private AMQPBridgeReceiver receiver;

   /**
    * Creates a new address entry for tracking demand on a bridged address
    *
    * @param addressInfo
    *    The address information object that this entry hold demand state for.
    */
   public AMQPBridgeFromAddressEntry(AddressInfo addressInfo) {
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
    * @return <code>true</code> if a receiver is currently set on this entry.
    */
   public boolean hasReceiver() {
      return receiver != null;
   }

   /**
    * @return the receiver managed by this entry
    */
   public AMQPBridgeReceiver getReceiver() {
      return receiver;
   }

   /**
    * Sets the receiver assigned to this entry to the given instance.
    *
    * @param receiver
    *    The bridge receiver that is currently active for this entry.
    *
    * @return this bridged address receiver entry.
    */
   public AMQPBridgeFromAddressEntry setReceiver(AMQPBridgeReceiver receiver) {
      Objects.requireNonNull(receiver, "Cannot assign a null receiver to this entry, call clear to unset");
      this.receiver = receiver;
      return this;
   }

   /**
    * Clears the currently assigned receiver from this entry.
    *
    * @return this bridged address receiver entry.
    */
   public AMQPBridgeFromAddressEntry clearReceiver() {
      this.receiver = null;
      return this;
   }

   /**
    * @return true if there are bindings that are mapped to this bridge entry.
    */
   public boolean hasDemand() {
      return !demandBindings.isEmpty();
   }

   /**
    * Add demand on this bridge address receiver from the given binding.
    *
    * @return this bridged address receiver entry.
    */
   public AMQPBridgeFromAddressEntry addDemand(Binding binding) {
      demandBindings.add(binding);
      return this;
   }

   /**
    * Reduce demand on this bridge address receiver from the given binding.
    *
    * @return this bridged address receiver entry.
    */
   public AMQPBridgeFromAddressEntry removeDemand(Binding binding) {
      demandBindings.remove(binding);
      return this;
   }
}
