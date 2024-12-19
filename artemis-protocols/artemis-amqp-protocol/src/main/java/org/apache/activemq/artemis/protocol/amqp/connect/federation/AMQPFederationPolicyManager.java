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
package org.apache.activemq.artemis.protocol.amqp.connect.federation;

import java.util.Objects;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerBindingPlugin;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationType;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPSessionContext;

/**
 * Base Federation policy manager that declares some common APIs that address or queue policy
 * managers must provide implementations for.
 */
public abstract class AMQPFederationPolicyManager implements ActiveMQServerBindingPlugin {

   protected final AMQPFederationMetrics metrics;
   protected final ActiveMQServer server;
   protected final AMQPFederation federation;
   protected final String policyName;
   protected final FederationType policyType;

   protected volatile AMQPSessionContext session;

   public AMQPFederationPolicyManager(AMQPFederation federation, AMQPFederationMetrics metrics, String policyName, FederationType policyType) {
      Objects.requireNonNull(federation, "The Federation instance cannot be null");

      this.federation = federation;
      this.server = federation.getServer();
      this.policyName = policyName;
      this.policyType = policyType;
      this.metrics = metrics;
   }

   /**
    * @return the number of message sent to the remote from senders under this policy.
    */
   public final long getMessagesSent() {
      return metrics.getMessagesSent();
   }

   /**
    * @return the federation type this policy manager implements.
    */
   public final FederationType getPolicyType() {
      return policyType;
   }

   /**
    * @return the assigned name of the policy that is being managed.
    */
   public final String getPolicyName() {
      return policyName;
   }

   /**
    * @return the {@link AMQPFederation} instance that owns this policy manager.
    */
   public AMQPFederation getFederation() {
      return federation;
   }
}
