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

import java.io.Closeable;
import java.util.function.Consumer;

import org.apache.qpid.proton.amqp.transport.Detach;

/**
 * Parent interface for AMQP Bridge receiver implementations
 */
public interface AMQPBridgeReceiver extends Closeable {

   /**
    * Starts the receiver instance which includes creating the remote resources and
    * performing any internal initialization needed to fully establish the receiver
    * instance. This call should not block and any errors encountered on creation of
    * the backing receiver resources should utilize the error handling mechanisms of
    * this AMQP bridge receiver.
    */
   void start();

   /**
    * Close the AMQP bridge receiver instance and cleans up its resources. This method
    * should not block and the actual resource shutdown work should occur asynchronously.
    */
   @Override
   void close();

   /**
    * @return the {@link AMQPBridgeManager} that this receiver operates under.
    */
   AMQPBridgeManager getBridge();

   /**
    * @return an information object that defines the characteristics of the {@link AMQPBridgeReceiver}
    */
   AMQPBridgeReceiverInfo getReceiverInfo();

   /**
    * Provides and event point for notification of the receiver having been opened successfully
    * by the remote. This handler will not be called if the remote rejects the link attach and
    * a {@link Detach} is expected to follow.
    *
    * @param handler
    *    The handler that will be invoked when the remote opens this receiver.
    *
    * @return this receiver instance.
    */
   AMQPBridgeReceiver setRemoteOpenHandler(Consumer<AMQPBridgeReceiver> handler);

   /**
    * Provides and event point for notification of the receiver having been closed by
    * the remote.
    *
    * @param handler
    *    The handler that will be invoked when the remote closes this receiver.
    *
    * @return this receiver instance.
    */
   AMQPBridgeReceiver setRemoteClosedHandler(Consumer<AMQPBridgeReceiver> handler);

}
