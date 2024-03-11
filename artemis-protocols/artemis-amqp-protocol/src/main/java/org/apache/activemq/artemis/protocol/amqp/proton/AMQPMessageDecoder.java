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

package org.apache.activemq.artemis.protocol.amqp.proton;

import java.util.function.BiConsumer;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;

import io.netty.util.internal.shaded.org.jctools.queues.MessagePassingQueue.Consumer;

/**
 *
 */
public class AMQPMessageDecoder extends AbstractMessageDecoder {

   private final AMQPSessionContext sessionContext;

   public AMQPMessageDecoder(AMQPSessionContext sessionContext, BiConsumer<Message, DeliveryAnnotations> messageHandler, Consumer<Throwable> errorHandler) {
      super(messageHandler, errorHandler);

      this.sessionContext = sessionContext;
   }

   @Override
   public void processDeliveryBytes(Delivery delivery) throws Exception {
      if (delivery.isPartial()) {
         return; // Only receive payload when complete
      }

      try {
         final Receiver receiver = ((Receiver) delivery.getLink());
         final ReadableBuffer payload = receiver.recv();
         final AMQPMessage message = sessionContext.getSessionSPI().createStandardMessage(delivery, payload);
         final DeliveryAnnotations deliveryAnnotations = message.getDeliveryAnnotations();

         messageHandler.accept(message, deliveryAnnotations);
      } catch (Exception error) {
         errorHandler.accept(error);
      }
   }
}
