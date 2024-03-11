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
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPLargeMessage;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPSessionCallback;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;

import io.netty.util.internal.shaded.org.jctools.queues.MessagePassingQueue.Consumer;

/**
 *
 */
public class AMQPLargeMessageDecoder extends AbstractMessageDecoder {

   private final AMQPSessionContext sessionContext;
   private final AMQPConnectionContext connection;

   private volatile AMQPLargeMessage currentMessage;
   private boolean closed = true;

   public AMQPLargeMessageDecoder(AMQPSessionContext sessionContext, BiConsumer<Message, DeliveryAnnotations> messageHandler, Consumer<Throwable> errorHandler) {
      super(messageHandler, errorHandler);

      this.sessionContext = sessionContext;
      this.connection = sessionContext.getAMQPConnectionContext();
   }

   public void close() {
      if (!closed) {
         try {
            AMQPLargeMessage localCurrentMessage = currentMessage;
            if (localCurrentMessage != null) {
               localCurrentMessage.deleteFile();
            }
         } catch (Throwable error) {
            ActiveMQServerLogger.LOGGER.errorDeletingLargeMessageFile(error);
         } finally {
            currentMessage = null;
         }

         closed = true;
      }
   }

   @Override
   public void processDeliveryBytes(Delivery delivery) throws Exception {
      if (closed) {
         throw new IllegalStateException("AMQP Large Message Decoder is closed and read cannot proceed");
      }

      try {
         connection.disableAutoRead();

         final Receiver receiver = ((Receiver) delivery.getLink());
         final ReadableBuffer dataBuffer = receiver.recv();
         final AMQPSessionCallback sessionSPI = sessionContext.getSessionSPI();
         final boolean isComplete = !delivery.isPartial();

         if (currentMessage == null) {
            final long id = sessionSPI.getStorageManager().generateID();
            final AMQPLargeMessage localCurrentMessage = new AMQPLargeMessage(
               id, delivery.getMessageFormat(), null, sessionSPI.getCoreMessageObjectPools(), sessionSPI.getStorageManager());

            localCurrentMessage.parseHeader(dataBuffer);

            sessionSPI.getStorageManager().onLargeMessageCreate(id, localCurrentMessage);
            currentMessage = localCurrentMessage;
         }

         sessionSPI.execute(() -> addBytes(currentMessage, dataBuffer, isComplete));
      } catch (Exception e) {
         errorHandler.accept(e);
      }
   }

   private void addBytes(AMQPLargeMessage message, ReadableBuffer dataBuffer, boolean isComplete) {
      try {
         message.addBytes(dataBuffer);

         if (isComplete) {
            message.releaseResources(connection.isLargeMessageSync(), true);
            // We don't want a close to delete the file now, we've released the resources.
            currentMessage = null;
            connection.runNow(() -> messageHandler.accept(message, message.getDeliveryAnnotations()));
         }

         connection.runNow(connection::enableAutoRead); // Open the gate on more incoming bytes of a delivery
      } catch (Throwable e) {
         connection.runNow(() -> errorHandler.accept(e));
      }
   }
}
