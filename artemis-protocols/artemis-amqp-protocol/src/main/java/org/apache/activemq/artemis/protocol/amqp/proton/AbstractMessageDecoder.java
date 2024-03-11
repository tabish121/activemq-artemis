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
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.engine.Delivery;

import io.netty.util.internal.shaded.org.jctools.queues.MessagePassingQueue.Consumer;

/**
 * Base class for incoming AMQP Message decoders that provides generic event points
 * for the completed work of the decoder.
 */
public abstract class AbstractMessageDecoder {

   protected final Consumer<Throwable> errorHandler;
   protected final BiConsumer<Message, DeliveryAnnotations> messageHandler;

   public AbstractMessageDecoder(BiConsumer<Message, DeliveryAnnotations> messageHandler, Consumer<Throwable> errorHandler) {
      this.messageHandler = messageHandler;
      this.errorHandler = errorHandler;
   }

   /**
    * Reads the bytes from an incoming delivery which might not be complete yet
    * but allows the reader to consume pending bytes to prevent stalling the sender
    * because the session window was exhausted. Once a delivery has been fully read
    * and is no longer partial the readBytes method will return the decoded message
    * for dispatch.
    *
    * Notice that asynchronous Readers will never return the Message but will rather call a complete operation on the
    * Server Receiver.
    *
    * @param delivery
    *    The delivery that has pending incoming bytes.
    */
   public abstract void processDeliveryBytes(Delivery delivery) throws Exception;

}
