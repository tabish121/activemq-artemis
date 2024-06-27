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

import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.IGNORE_QUEUE_CONSUMER_FILTERS;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.IGNORE_QUEUE_FILTERS;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.RECEIVER_CREDITS;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.RECEIVER_CREDITS_LOW;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.IGNORE_QUEUE_CONSUMER_PRIORITIES;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.PULL_RECEIVER_BATCH_SIZE;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.LARGE_MESSAGE_THRESHOLD;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.LINK_ATTACH_TIMEOUT;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport;
import org.apache.qpid.proton.engine.Receiver;

/**
 * Configuration options applied to a receiver created from bridge from policies for
 * address or queue bridging. The options first check the policy properties for
 * matching configuration settings before looking at the bridge's own configuration
 * for the options managed here.
 */
public final class AMQPBridgeReceiverConfiguration {

   private final Map<String, Object> properties;
   private final AMQPBridgeConfiguration configuration;

   @SuppressWarnings("unchecked")
   public AMQPBridgeReceiverConfiguration(AMQPBridgeConfiguration configuration, Map<String, ?> properties) {
      Objects.requireNonNull(configuration, "Bridge configuration cannot be null");

      this.configuration = configuration;

      if (properties == null || properties.isEmpty()) {
         this.properties = Collections.EMPTY_MAP;
      } else {
         this.properties = (Map<String, Object>) Collections.unmodifiableMap(new HashMap<>(properties));
      }
   }

   public int getReceiverCredits() {
      final Object property = properties.get(RECEIVER_CREDITS);
      if (property instanceof Number) {
         return ((Number) property).intValue();
      } else if (property instanceof String) {
         return Integer.parseInt((String) property);
      } else {
         return configuration.getReceiverCredits();
      }
   }

   public int getReceiverCreditsLow() {
      final Object property = properties.get(RECEIVER_CREDITS_LOW);
      if (property instanceof Number) {
         return ((Number) property).intValue();
      } else if (property instanceof String) {
         return Integer.parseInt((String) property);
      } else {
         return configuration.getReceiverCreditsLow();
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
         return configuration.getPullReceiverBatchSize();
      }
   }

   public int getLargeMessageThreshold() {
      final Object property = properties.get(LARGE_MESSAGE_THRESHOLD);
      if (property instanceof Number) {
         return ((Number) property).intValue();
      } else if (property instanceof String) {
         return Integer.parseInt((String) property);
      } else {
         return configuration.getLargeMessageThreshold();
      }
   }

   public int getLinkAttachTimeout() {
      final Object property = properties.get(LINK_ATTACH_TIMEOUT);
      if (property instanceof Number) {
         return ((Number) property).intValue();
      } else if (property instanceof String) {
         return Integer.parseInt((String) property);
      } else {
         return configuration.getLinkAttachTimeout();
      }
   }

   public boolean isCoreMessageTunnelingEnabled() {
      final Object property = properties.get(AmqpSupport.TUNNEL_CORE_MESSAGES);
      if (property instanceof Boolean) {
         return (Boolean) property;
      } else if (property instanceof String) {
         return Boolean.parseBoolean((String) property);
      } else {
         return configuration.isCoreMessageTunnelingEnabled();
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
         return configuration.isIgnoreSubscriptionFilters();
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
         return configuration.isIgnoreQueueFilters();
      }
   }

   /**
    * @return <code>true</code> if the bridge is configured to ignore priorities of local Queue consumers.
    */
   public boolean isIgnoreSubscriptionPriorities() {
      final Object property = properties.get(IGNORE_QUEUE_CONSUMER_PRIORITIES);
      if (property instanceof Boolean) {
         return (Boolean) property;
      } else if (property instanceof String) {
         return Boolean.parseBoolean((String) property);
      } else {
         return configuration.isIgnoreSubscriptionPriorities();
      }
   }
}
