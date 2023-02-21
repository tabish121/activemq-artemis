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

package org.apache.activemq.artemis.protocol.amqp.support;

import org.apache.qpid.protonj2.types.Symbol;

/**
 * Utility class used to process the offered and desired properties for each
 * endpoint of an AMQP connection and present simple APIs for managing them.
 */
public final class AMQPConnectionProperties {

   private static final Symbol[] EMPTY = new Symbol[0];

   /**
    * This set of connection capabilities is the default set we will apply to
    * any connection that the broker creates or answer as supported to any incoming
    * connection to the broker.
    */
   public static final Symbol[] DEFAULT_CONNECTION_OFFERED_CAPABILITIES = new Symbol[] {
      AMQPConnectionConstants.SOLE_CONNECTION_CAPABILITY,
      AMQPConnectionConstants.DELAYED_DELIVERY,
      AMQPConnectionConstants.SHARED_SUBS,
      AMQPConnectionConstants.ANONYMOUS_RELAY};

   private boolean anonymousRelayOffered;
   private boolean anonymousRelayDesired;

   private boolean sharedSubsOffered;
   private boolean sharedSubsDesired;

   private boolean soleConnectionPerContainerIdOffered;
   private boolean soleConnectionPerContainerIdDesired;

   private boolean delayedDeliveryOffered;
   private boolean delayedDeliveryDesired;

   public AMQPConnectionProperties discoverProperties(Symbol[] offeredCapabilities, Symbol[] desiredCapabilities) {
      processOfferedCapabilieis(offeredCapabilities == null ? EMPTY : offeredCapabilities);
      processDesiredCapabilieis(desiredCapabilities == null ? EMPTY : desiredCapabilities);

      return this;
   }

   public boolean isSharedSubsDesired() {
      return sharedSubsDesired;
   }

   public boolean isSharedSubsOffered() {
      return sharedSubsOffered;
   }

   public boolean isSoleConnectionPerContainerIdDesired() {
      return soleConnectionPerContainerIdDesired;
   }

   public boolean isSoleConnectionPerContainerIdOffered() {
      return soleConnectionPerContainerIdOffered;
   }

   public boolean isDelayedDeliveryDesired() {
      return delayedDeliveryDesired;
   }

   public boolean isDelayedDeliveryOffered() {
      return delayedDeliveryOffered;
   }

   public boolean isAnonymousRelayDesired() {
      return anonymousRelayDesired;
   }

   public boolean isAnonymousRelayOffered() {
      return anonymousRelayOffered;
   }

   /**
    * @return the default set of connection offered capabilities that should be sent on open
    */
   public static Symbol[] getDefaultOfferedCapabilities() {
      return DEFAULT_CONNECTION_OFFERED_CAPABILITIES;
   }

   /**
    * @return the default set of connection desired capabilities that should be sent on open
    */
   public static Symbol[] getDefaultDesiredCapabilities() {
      return null; // None yet but could be added in future if needed
   }

   /**
    *
    * @param additional
    *       Additional offered capabilities to send along with the defaults
    *
    * @return the array of offered capabilities to send.
    */
   public static Symbol[] getOfferedCapabilitiesWithDefaults(Symbol[] additional) {
      if (additional == null || additional.length == 0) {
         return DEFAULT_CONNECTION_OFFERED_CAPABILITIES;
      } else {
         final Symbol[] result = new Symbol[additional.length + DEFAULT_CONNECTION_OFFERED_CAPABILITIES.length];
         System.arraycopy(additional, 0, result, 0, additional.length);
         System.arraycopy(DEFAULT_CONNECTION_OFFERED_CAPABILITIES, 0, result, additional.length, DEFAULT_CONNECTION_OFFERED_CAPABILITIES.length);
         return result;
      }
   }

   public static Symbol[] getDesiredCapabilitiesWithDefaults(Symbol[] additional) {
      return additional; // No defaults yet so we can just pass through
   }

   private void processOfferedCapabilieis(Symbol[] offeredCapabilities) {
      for (Symbol offered : offeredCapabilities) {
         if (AMQPConnectionConstants.SOLE_CONNECTION_CAPABILITY.equals(offered)) {
            anonymousRelayOffered = true;
         } else if (AMQPConnectionConstants.DELAYED_DELIVERY.equals(offered)) {
            soleConnectionPerContainerIdOffered = true;
         } else if (AMQPConnectionConstants.SHARED_SUBS.equals(offered)) {
            sharedSubsOffered = true;
         } else if (AMQPConnectionConstants.ANONYMOUS_RELAY.equals(offered)) {
            delayedDeliveryOffered = true;
         }
      }
   }

   private void processDesiredCapabilieis(Symbol[] desiredCapabilities) {
      for (Symbol desired : desiredCapabilities) {
         if (AMQPConnectionConstants.SOLE_CONNECTION_CAPABILITY.equals(desired)) {
            anonymousRelayDesired = true;
         } else if (AMQPConnectionConstants.DELAYED_DELIVERY.equals(desired)) {
            soleConnectionPerContainerIdDesired = true;
         } else if (AMQPConnectionConstants.SHARED_SUBS.equals(desired)) {
            sharedSubsDesired = true;
         } else if (AMQPConnectionConstants.ANONYMOUS_RELAY.equals(desired)) {
            delayedDeliveryDesired = true;
         }
      }
   }
}
