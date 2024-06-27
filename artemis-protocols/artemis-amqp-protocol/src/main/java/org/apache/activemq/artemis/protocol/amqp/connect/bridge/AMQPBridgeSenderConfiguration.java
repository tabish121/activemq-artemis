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

import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.LINK_ATTACH_TIMEOUT;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport;

/**
 * Configuration options applied to a sender created from bridge from policies for
 * address or queue bridging. The options first check the policy properties for
 * matching configuration settings before looking at the bridge's own configuration
 * for the options managed here.
 */
public final class AMQPBridgeSenderConfiguration {

   private final Map<String, Object> properties;
   private final AMQPBridgeConfiguration configuration;

   @SuppressWarnings("unchecked")
   public AMQPBridgeSenderConfiguration(AMQPBridgeConfiguration configuration, Map<String, ?> properties) {
      Objects.requireNonNull(configuration, "Bridge configuration cannot be null");

      this.configuration = configuration;

      if (properties == null || properties.isEmpty()) {
         this.properties = Collections.EMPTY_MAP;
      } else {
         this.properties = (Map<String, Object>) Collections.unmodifiableMap(new HashMap<>(properties));
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
}
