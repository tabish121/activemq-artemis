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

import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.LARGE_MESSAGE_THRESHOLD;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.LINK_ATTACH_TIMEOUT;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.LINK_RECOVERY_DELAY;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.LINK_RECOVERY_INITIAL_DELAY;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.MAX_LINK_RECOVERY_ATTEMPTS;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.PRESETTLE_SEND_MODE;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.DISABLE_RECEIVER_PRIORITY;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.PULL_RECEIVER_BATCH_SIZE;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.RECEIVER_CREDITS;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.RECEIVER_CREDITS_LOW;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.IGNORE_QUEUE_FILTERS;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.DEFAULT_CORE_MESSAGE_TUNNELING_ENABLED;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.DEFAULT_DISABLE_RECEIVER_DEMAND_TRACKING;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.DEFAULT_IGNNORE_QUEUE_CONSUMER_FILTERS;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.DEFAULT_IGNNORE_QUEUE_CONSUMER_PRIORITIES;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.DEFAULT_IGNNORE_QUEUE_FILTERS;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.DEFAULT_LINK_ATTACH_TIMEOUT;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.DEFAULT_LINK_RECOVERY_DELAY;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.DEFAULT_LINK_RECOVERY_INITIAL_DELAY;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.DEFAULT_MAX_LINK_RECOVERY_ATTEMPTS;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.DEFAULT_DISABLE_RECEIVER_PRIORITY;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.DEFAULT_PULL_CREDIT_BATCH_SIZE;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.DEFAULT_SEND_SETTLED;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.DISABLE_RECEIVER_DEMAND_TRACKING;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.IGNORE_QUEUE_CONSUMER_FILTERS;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.IGNORE_QUEUE_CONSUMER_PRIORITIES;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.activemq.artemis.protocol.amqp.proton.AMQPConnectionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport;
import org.apache.qpid.proton.engine.Receiver;

/**
 * A configuration class that contains API for getting AMQP bridge specific configuration
 * either from a {@link Map} of configuration elements or from the connection associated
 * with the bridge instance, or possibly from a set default value.
 */
public class AMQPBridgeConfiguration {

   private final Map<String, Object> properties;
   private final AMQPConnectionContext connection;

   @SuppressWarnings("unchecked")
   public AMQPBridgeConfiguration(AMQPConnectionContext connection, Map<String, Object> properties) {
      Objects.requireNonNull(connection, "Connection provided cannot be null");

      this.connection = connection;

      if (properties != null && !properties.isEmpty()) {
         this.properties = new HashMap<>(properties);
      } else {
         this.properties = Collections.EMPTY_MAP;
      }
   }

   /**
    * @return the credit batch size offered to a {@link Receiver} link.
    */
   public int getReceiverCredits() {
      final Object property = properties.get(RECEIVER_CREDITS);
      if (property instanceof Number) {
         return ((Number) property).intValue();
      } else if (property instanceof String) {
         return Integer.parseInt((String) property);
      } else {
         return connection.getAmqpCredits();
      }
   }

   /**
    * @return the number of remaining credits on a {@link Receiver} before the batch is replenished.
    */
   public int getReceiverCreditsLow() {
      final Object property = properties.get(RECEIVER_CREDITS_LOW);
      if (property instanceof Number) {
         return ((Number) property).intValue();
      } else if (property instanceof String) {
         return Integer.parseInt((String) property);
      } else {
         return connection.getAmqpLowCredits();
      }
   }

   /**
    * @return the credit batch size offered to a {@link Receiver} link that is in pull mode.
    */
   public int getPullReceiverBatchSize() {
      final Object property = properties.get(PULL_RECEIVER_BATCH_SIZE);
      if (property instanceof Number) {
         return ((Number) property).intValue();
      } else if (property instanceof String) {
         return Integer.parseInt((String) property);
      } else {
         return DEFAULT_PULL_CREDIT_BATCH_SIZE;
      }
   }

   /**
    * @return the size in bytes of an incoming message after which the {@link Receiver} treats it as large.
    */
   public int getLargeMessageThreshold() {
      final Object property = properties.get(LARGE_MESSAGE_THRESHOLD);
      if (property instanceof Number) {
         return ((Number) property).intValue();
      } else if (property instanceof String) {
         return Integer.parseInt((String) property);
      } else {
         return connection.getProtocolManager().getAmqpMinLargeMessageSize();
      }
   }

   /**
    * @return the size in bytes of an incoming message after which the {@link Receiver} treats it as large.
    */
   public int getLinkAttachTimeout() {
      final Object property = properties.get(LINK_ATTACH_TIMEOUT);
      if (property instanceof Number) {
         return ((Number) property).intValue();
      } else if (property instanceof String) {
         return Integer.parseInt((String) property);
      } else {
         return DEFAULT_LINK_ATTACH_TIMEOUT;
      }
   }

   /**
    * @return true if the bridge is configured to tunnel core messages as AMQP custom messages.
    */
   public boolean isCoreMessageTunnelingEnabled() {
      final Object property = properties.get(AmqpSupport.TUNNEL_CORE_MESSAGES);
      if (property instanceof Boolean) {
         return (Boolean) property;
      } else if (property instanceof String) {
         return Boolean.parseBoolean((String) property);
      } else {
         return DEFAULT_CORE_MESSAGE_TUNNELING_ENABLED;
      }
   }

   /**
    * @return <code>true</code> if the bridge is configured to ignore filters on individual queue consumers
    */
   public boolean isIgnoreSubscriptionFilters() {
      final Object property = properties.get(IGNORE_QUEUE_CONSUMER_FILTERS);
      if (property instanceof Boolean) {
         return (Boolean) property;
      } else if (property instanceof String) {
         return Boolean.parseBoolean((String) property);
      } else {
         return DEFAULT_IGNNORE_QUEUE_CONSUMER_FILTERS;
      }
   }

   /**
    * @return <code>true</code> if the bridge is configured to ignore filters on the bridged Queue.
    */
   public boolean isIgnoreQueueFilters() {
      final Object property = properties.get(IGNORE_QUEUE_FILTERS);
      if (property instanceof Boolean) {
         return (Boolean) property;
      } else if (property instanceof String) {
         return Boolean.parseBoolean((String) property);
      } else {
         return DEFAULT_IGNNORE_QUEUE_FILTERS;
      }
   }

   /**
    * @return <code>true</code> if bridge is configured to ignore priorities on individual queue consumers
    */
   public boolean isIgnoreSubscriptionPriorities() {
      final Object property = properties.get(IGNORE_QUEUE_CONSUMER_PRIORITIES);
      if (property instanceof Boolean) {
         return (Boolean) property;
      } else if (property instanceof String) {
         return Boolean.parseBoolean((String) property);
      } else {
         return DEFAULT_IGNNORE_QUEUE_CONSUMER_PRIORITIES;
      }
   }

   /**
    * @return <code>true</code> if bridge is configured to omit any priority properties on receiver links.
    */
   public boolean isReceiverPriorityDisabled() {
      final Object property = properties.get(DISABLE_RECEIVER_PRIORITY);
      if (property instanceof Boolean) {
         return (Boolean) property;
      } else if (property instanceof String) {
         return Boolean.parseBoolean((String) property);
      } else {
         return DEFAULT_DISABLE_RECEIVER_PRIORITY;
      }
   }

   /**
    * @return <code>true</code> if bridge is configured to ignore local demand and always create a receiver.
    */
   public boolean isReceiverDemandTrackingDisabled() {
      final Object property = properties.get(DISABLE_RECEIVER_DEMAND_TRACKING);
      if (property instanceof Boolean) {
         return (Boolean) property;
      } else if (property instanceof String) {
         return Boolean.parseBoolean((String) property);
      } else {
         return DEFAULT_DISABLE_RECEIVER_DEMAND_TRACKING;
      }
   }

   /**
    * @return <code>true</code> if bridge is configured to create links with the sender settle mode set to settled.
    */
   public boolean isUsingPresettledSenders() {
      final Object property = properties.get(PRESETTLE_SEND_MODE);
      if (property instanceof Boolean) {
         return (Boolean) property;
      } else if (property instanceof String) {
         return Boolean.parseBoolean((String) property);
      } else {
         return DEFAULT_SEND_SETTLED;
      }
   }

   /**
    * @return the maximum number of link recovery attempts, or zero if no attempts allowed.
    */
   public int getMaxLinkRecoveryAttempts() {
      final Object property = properties.get(MAX_LINK_RECOVERY_ATTEMPTS);
      if (property instanceof Number) {
         return ((Number) property).intValue();
      } else if (property instanceof String) {
         return Integer.parseInt((String) property);
      } else {
         return DEFAULT_MAX_LINK_RECOVERY_ATTEMPTS;
      }
   }

   /**
    * @return the initial delay before a link recovery attempt is made.
    */
   public long getLinkRecoveryInitialDelay() {
      final Object property = properties.get(LINK_RECOVERY_INITIAL_DELAY);
      if (property instanceof Number) {
         return ((Number) property).intValue();
      } else if (property instanceof String) {
         return Integer.parseInt((String) property);
      } else {
         return DEFAULT_LINK_RECOVERY_INITIAL_DELAY;
      }
   }

   /**
    * @return the delay that will be used between successive link recovery attempts.
    */
   public long getLinkRecoveryDelay() {
      final Object property = properties.get(LINK_RECOVERY_DELAY);
      if (property instanceof Number) {
         return ((Number) property).intValue();
      } else if (property instanceof String) {
         return Integer.parseInt((String) property);
      } else {
         return DEFAULT_LINK_RECOVERY_DELAY;
      }
   }
}
