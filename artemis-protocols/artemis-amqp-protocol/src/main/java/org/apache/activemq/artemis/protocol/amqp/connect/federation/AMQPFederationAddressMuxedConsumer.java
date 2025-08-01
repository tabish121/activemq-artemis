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

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationMetrics.ConsumerMetrics;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationConsumerInfo;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPSessionContext;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;

/**
 * Consumer implementation for Federated Addresses that receives from a remote AMQP peer and forwards those messages
 * onto the internal bindings on a given address that matched criteria of the given {@link FederationConsumerInfo}.
 */
public final class AMQPFederationAddressMuxedConsumer extends AMQPFederationAddressConsumer {

   public AMQPFederationAddressMuxedConsumer(AMQPFederationAddressPolicyManager manager,
                                             AMQPFederationConsumerConfiguration configuration,
                                             AMQPSessionContext session, FederationConsumerInfo consumerInfo,
                                             ConsumerMetrics metrics) {
      super(manager, configuration, session, consumerInfo, metrics);
   }

   @Override
   protected AMQPFederatedAddressDeliveryHandler createDeliveryHandler(Receiver receiver) {
      return new AMQPFederatedAddressMuxedDeliveryReceiver(session, consumerInfo, receiver);
   }

   /**
    * Wrapper around the standard receiver context that provides federation specific entry points and customizes inbound
    * delivery handling for this Address receiver.
    */
   private class AMQPFederatedAddressMuxedDeliveryReceiver extends AMQPFederatedAddressDeliveryHandler {

      /**
       * Creates the federation receiver instance.
       *
       * @param session
       *    The server session context bound to the receiver instance.
       * @param consumerInfo
       *    The {@link FederationConsumerInfo} that defines the remote consumer link.
       * @param receiver
       *    The proton receiver that will be wrapped in this server context instance.
       */
      AMQPFederatedAddressMuxedDeliveryReceiver(AMQPSessionContext session, FederationConsumerInfo consumerInfo, Receiver receiver) {
         super(session, consumerInfo, receiver);
      }

      @Override
      protected void routeFederatedMessage(Message message, Delivery delivery, Receiver receiver, Transaction tx) {
         // TODO Route the message to all Bindings that are matched to the consumer info that created this instance.
      }
   }
}
