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

package org.apache.activemq.artemis.protocol.amqp;

import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;

/**
 * Configuration object that carries all the various configuration options from the
 * broker and acceptor configuration into a client connection.  All new AMQP configuration
 * options should be added here as well as any new broker configuration options that
 * need to be carried into a connection for later use.
 */
public class AMQPConnectionConfiguration {

   public static final int DEFAULT_SENDER_LINK_CREDITS = 1000;
   public static final int DEFAULT_SENDER_LINK_CREDITS_LOW = 300;
   public static final int DEFAULT_MAX_FRAME_SIZE = 128 * 1024;
   public static final boolean TREAT_REJECT_AS_UNMODIFIED_DELIVERY_FAILURE = false;
   public static final boolean USE_MODIFIED_FOR_TRANSIENT_DELIVERY_ERRORS = false;

   private final Map<SimpleString, RoutingType> prefixes = new HashMap<>();
   private int minLargeMessageSize = 100 * 1024;
   private int senderCredits = DEFAULT_SENDER_LINK_CREDITS;
   private int senderCreditsLow = DEFAULT_SENDER_LINK_CREDITS_LOW;
   private boolean duplicateDetection = true;
   private boolean useModifiedForTransientDeliveryErrors = USE_MODIFIED_FOR_TRANSIENT_DELIVERY_ERRORS;
   private boolean treatRejectAsUnmodifiedDeliveryFailed = TREAT_REJECT_AS_UNMODIFIED_DELIVERY_FAILURE;
   private int initialRemoteMaxFrameSize = 4 * 1024;
   private String[] saslMechanisms = new String[] { "ANONYMOUS", "PLAIN" };
   private String saslLoginConfigScope = "amqp-sasl-gssapi";
   private long connectionIdleTimeout = -1;
   private boolean directDeliver = true;
   private int maxFrameSize = DEFAULT_MAX_FRAME_SIZE;

   /**
    * @return the configured threshold for considering a message to be large
    */
   public int getAmqpMinLargeMessageSize() {
      return minLargeMessageSize;
   }

   /**
    * The minLargeMessageSize configuration determines when a message should be considered to be a
    * large message by the server, setting this value to minus one disable large message processing
    * for AMQP connections.
    *
    * @param minLargeMessageSize
    *       The value to use when deciding if a message should be treated as large.
    *
    * @return this configuration instance.
    */
   public AMQPConnectionConfiguration setMinLargeMessageSize(int minLargeMessageSize) {
      this.minLargeMessageSize = minLargeMessageSize;
      return this;
   }

   /**
    * @return <code>true</code> if duplicate detection is enabled for AMQP connections.
    */
   public boolean isDuplicateDetection() {
      return duplicateDetection;
   }

   /**
    * Controls if duplicate detection is enabled or disabled for AMQP connections.
    *
    * @param duplicateDetection
    *       boolean used to enable or disable duplicate detection.
    *
    * @return this configuration instance.
    */
   public AMQPConnectionConfiguration setDuplicateDetection(boolean duplicateDetection) {
      this.duplicateDetection = duplicateDetection;
      return this;
   }

   /**
    * @return the configured Idle timeout that will be conveyed to the remote peer.
    */
   public long getConnectionIdleTimeout() {
      return connectionIdleTimeout;
   }

   /**
    * Configures the connection idle timeout value that is sent in the Open performative to
    * the connecting remote peer.
    *
    * @param ttl
    *       The AMQP connection idle timeout value sent to a remote peer.
    *
    * @return this configuration instance.
    */
   public AMQPConnectionConfiguration setConnectionIdleTimeout(long ttl) {
      this.connectionIdleTimeout = ttl;
      return this;
   }

   /**
    * @return <code>true</code> if direct deliver has been configured for this connection.
    */
   public boolean isDirectDeliver() {
      return directDeliver;
   }

   /**
    * Configures the broker side direct deliver setting for AMQP connections.
    *
    * @param directDeliver
    *       should direct deliver be enabled (default is off).
    *
    * @return this configuration instance.
    */
   public AMQPConnectionConfiguration setDirectDeliver(boolean directDeliver) {
      this.directDeliver = directDeliver;
      return this;
   }

   /**
    * @return the number of credits to use when granting a receiver
    */
   public int getSenderCredits() {
      return senderCredits;
   }

   /**
    * Configures the link credit that will be supplied to a remote sender link endpoint
    * once opened which controls the amount of incoming deliveries the sender can send
    * before credit is replenished.
    *
    * @param senderCredits
    *       The number of link credits to flow to the sender endpoint.
    *
    * @return this configuration instance.
    */
   public AMQPConnectionConfiguration setSenderCredits(int senderCredits) {
      this.senderCredits = senderCredits;
      return this;
   }

   /**
    * @return the configured low water mark for sender credits needing replenished.
    */
   public int getSenderCreditsLow() {
      return senderCreditsLow;
   }

   /**
    * Configures a low water mark for sender links outstanding credits before a
    * replenishment should be done to maintain a steady stream of incoming deliveries.
    * The broker may choose not to replenish link credits even if the low water mark
    * is hit.
    *
    * @param senderCreditsLow
    *       The low water mark for sender link credits before refresh
    *
    * @return this configuration instance.
    */
   public AMQPConnectionConfiguration setSenderCreditsLow(int senderCreditsLow) {
      this.senderCreditsLow = senderCreditsLow;
      return this;
   }

   /**
    * @return the configured max frame size to supply to the remote.
    */
   public int getMaxFrameSize() {
      return maxFrameSize;
   }

   /**
    * Configures the max frame size value which will be supplied to the remote peer in the
    * Open performative that indicates the maximum size any given frame that the remote
    * sends is allowed to be.  The can control the chunk sizes on the remote for larger
    * messages sent to the broker allowing more efficient streaming.
    *
    * @param maxFrameSize
    *       The max frame size to send to the remote peer.
    *
    * @return this configuration instance.
    */
   public AMQPConnectionConfiguration setMaxFrameSize(int maxFrameSize) {
      this.maxFrameSize = maxFrameSize;
      return this;
   }

   /**
    * @return the configured set of SASL mechanisms to offer to the remote peer.
    */
   public String[] getSaslMechanisms() {
      return saslMechanisms;
   }

   /**
    * Allows configuration of a set of SASL mechanisms other than the defaults to
    * offer to a remote peer during SASL authentication.  If not set the broker
    * defaults will be supplied.
    *
    * @param saslMechanisms
    *       The set of SASL mechanisms that should be offered to the remote peer.
    *
    * @return this configuration instance.
    */
   public AMQPConnectionConfiguration setSaslMechanisms(String[] saslMechanisms) {
      this.saslMechanisms = saslMechanisms;
      return this;
   }

   /**
    * @return a login configuration scope to use during the SASL authentication process.
    */
   public String getSaslLoginConfigScope() {
      return saslLoginConfigScope;
   }

   /**
    * Allows configuration of a configuration scope value for use during SASL authentication.
    *
    * @param saslLoginConfigScope
    *       The configuration scope to use during SASL authentication.
    *
    * @return this configuration instance.
    */
   public AMQPConnectionConfiguration setSaslLoginConfigScope(String saslLoginConfigScope) {
      this.saslLoginConfigScope = saslLoginConfigScope;
      return this;
   }

   /**
    * Configures the prefix(es) that can be used by the remote when opening a link to
    * attempt to control the address routing type as ANYCAST.
    *
    * @param anycastPrefix
    *       a comma delimited set of address prefix values for ANYCAST addresses.
    *
    * @return this configuration instance.
    */
   public AMQPConnectionConfiguration setAnycastPrefix(String anycastPrefix) {
      for (String prefix : anycastPrefix.split(",")) {
         prefixes.put(SimpleString.toSimpleString(prefix), RoutingType.ANYCAST);
      }
      return this;
   }

   /**
    * Configures the prefix(es) that can be used by the remote when opening a link to
    * attempt to control the address routing type as MULTICAST.
    *
    * @param multicastPrefix
    *       a comma delimited set of address prefix values for MULTICAST addresses.
    *
    * @return this configuration instance.
    */
   public AMQPConnectionConfiguration setMulticastPrefix(String multicastPrefix) {
      for (String prefix : multicastPrefix.split(",")) {
         prefixes.put(SimpleString.toSimpleString(prefix), RoutingType.MULTICAST);
      }
      return this;
   }

   /**
    * @return a {@link Map} containing the configured address prefix values to use during link creation.
    */
   public Map<SimpleString, RoutingType> getPrefixes() {
      return prefixes;
   }

   /**
    * @return the configured initial max frame size value.
    */
   public int getInitialRemoteMaxFrameSize() {
      return initialRemoteMaxFrameSize;
   }

   /**
    * Configures the initial max frame size value which will be used to validate SASL frames
    * that arrive before the initial Open frame is sent to the remote which indicates what our
    * defined max frame size is.  This is useful to allow larger SASL frames that would otherwise
    * fit into the specification defined default max frame size of 512 bytes.
    *
    * @param initialRemoteMaxFrameSize
    *       The initial max frame size (in bytes) value to apply before the Open performative is sent.
    *
    * @return this configuration instance.
    */
   public AMQPConnectionConfiguration setInitialRemoteMaxFrameSize(int initialRemoteMaxFrameSize) {
      this.initialRemoteMaxFrameSize = initialRemoteMaxFrameSize;
      return this;
   }

   /**
    * @return <code>true</code> if transient delivery errors should be handled with a Modified
    *         disposition (if permitted by link).
    */
   public boolean isUseModifiedForTransientDeliveryErrors() {
      return this.useModifiedForTransientDeliveryErrors;
   }

   /**
    * Sets if transient delivery errors should be handled with a Modified disposition
    * (if permitted by link).
    *
    * @param useModifiedForTransientDeliveryErrors
    *       controls if a modified disposition should be used for transient errors.
    *
    * @return this configuration instance.
    */
   public AMQPConnectionConfiguration setUseModifiedForTransientDeliveryErrors(boolean useModifiedForTransientDeliveryErrors) {
      this.useModifiedForTransientDeliveryErrors = useModifiedForTransientDeliveryErrors;
      return this;
   }

   /**
    * @return <code>true</code> if Rejected dispositions from the remote should be treated as Modified.
    */
   public boolean isTreatRejectAsUnmodifiedDeliveryFailed() {
      return this.treatRejectAsUnmodifiedDeliveryFailed;
   }

   /**
    * Configures if the broker should treat a Rejected disposition as a Modified disposition
    * with the delivery failed flag set to true.  When enabled this would result in what would
    * normally be a terminal outcome for a delivery with one that instead would allow the delivery
    * to be delivered again if no maximum deliveries value is not exceeded.
    *
    * @param treatRejectAsUnmodifiedDeliveryFailed
    *       controls if a Modified disposition should be used in place of Rejected dispositions.
    *
    * @return this configuration instance.
    */
   public AMQPConnectionConfiguration setTreatRejectAsUnmodifiedDeliveryFailed(final boolean treatRejectAsUnmodifiedDeliveryFailed) {
      this.treatRejectAsUnmodifiedDeliveryFailed = treatRejectAsUnmodifiedDeliveryFailed;
      return this;
   }

   private boolean useCoreSubscriptionNaming;

   /**
    * @return if Core subscription naming for AMQP is enabled or not.
    */
   public boolean isUseCoreSubscriptionNaming() {
      return useCoreSubscriptionNaming;
   }

   /**
    * Configures if the AMQP protocol handler should be using core subscription naming
    * rules or not (default is false).
    *
    * @param useCoreSubscriptionNaming
    *       controls if core subscription naming will be used or not.
    *
    * @return this configuration instance.
    */
   public AMQPConnectionConfiguration setAmqpUseCoreSubscriptionNaming(boolean useCoreSubscriptionNaming) {
      this.useCoreSubscriptionNaming = useCoreSubscriptionNaming;
      return this;
   }

   /**
    * Used to create a stable copy of the configuration obtained from the protocol
    * manager upon creation of a new connection.  Updates to the broker configuration
    * after this copy are not reflected in the existing connection.
    *
    * @return a stable copy of the configuration instance.
    */
   public AMQPConnectionConfiguration copy() {
      AMQPConnectionConfiguration copy = new AMQPConnectionConfiguration();

      copy.prefixes.putAll(prefixes);
      copy.minLargeMessageSize = minLargeMessageSize;
      copy.senderCredits = senderCredits;
      copy.senderCreditsLow = senderCreditsLow;
      copy.duplicateDetection = duplicateDetection;
      copy.useModifiedForTransientDeliveryErrors = useCoreSubscriptionNaming;
      copy.treatRejectAsUnmodifiedDeliveryFailed = treatRejectAsUnmodifiedDeliveryFailed;
      copy.initialRemoteMaxFrameSize = initialRemoteMaxFrameSize;
      copy.saslMechanisms = saslMechanisms.clone();
      copy.saslLoginConfigScope = saslLoginConfigScope;
      copy.connectionIdleTimeout = connectionIdleTimeout;
      copy.directDeliver = directDeliver;
      copy.maxFrameSize = maxFrameSize;

      return copy;
   }
}
