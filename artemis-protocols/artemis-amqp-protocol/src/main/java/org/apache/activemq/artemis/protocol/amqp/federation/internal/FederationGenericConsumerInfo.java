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

package org.apache.activemq.artemis.protocol.amqp.federation.internal;

import java.util.Objects;
import java.util.UUID;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationConsumerInfo;

/**
 * Information and identification class for Federation consumers created to federate
 * queues and addresses. Instances of this class should be usable in Collections
 * classes where equality and hashing support is needed.
 */
public class FederationGenericConsumerInfo implements FederationConsumerInfo {

   private final Role role;
   private final String address;
   private final String queueName;
   private final RoutingType routingType;
   private final String filterString;
   private final String fqqn;
   private final int priority;
   private final String id;

   public FederationGenericConsumerInfo(Role role, String address, String queueName, RoutingType routingType,
                                        String filterString, String fqqn, int priority) {
      this.role = role;
      this.address = address;
      this.queueName = queueName;
      this.routingType = routingType;
      this.filterString = filterString;
      this.fqqn = fqqn;
      this.priority = priority;
      this.id = UUID.randomUUID().toString();
   }

   @Override
   public String getId() {
      return id;
   }

   @Override
   public Role getRole() {
      return role;
   }

   @Override
   public String getQueueName() {
      return queueName;
   }

   @Override
   public String getAddress() {
      return address;
   }

   @Override
   public String getFqqn() {
      return fqqn;
   }

   @Override
   public RoutingType getRoutingType() {
      return routingType;
   }

   @Override
   public String getFilterString() {
      return filterString;
   }

   @Override
   public int getPriority() {
      return priority;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }

      if (!(o instanceof FederationGenericConsumerInfo)) {
         return false;
      }

      final FederationGenericConsumerInfo that = (FederationGenericConsumerInfo) o;

      return role == that.role &&
             priority == that.priority &&
             Objects.equals(address, that.address) &&
             Objects.equals(queueName, that.queueName) &&
             routingType == that.routingType &&
             Objects.equals(filterString, that.filterString) &&
             Objects.equals(fqqn, that.fqqn);
   }

   @Override
   public int hashCode() {
      return Objects.hash(role, address, queueName, routingType, filterString, fqqn, priority);
   }

   @Override
   public String toString() {
      return "FederationConsumerInfo: { " + getRole() + ", " + getFqqn() + "}";
   }
}
