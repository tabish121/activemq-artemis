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

/**
 * Parent interface for AMQP Bridge sender implementations
 */
public interface AMQPBridgeSender extends Closeable {

   /**
    * Starts the sender instance which includes creating the remote resources and
    * performing any internal initialization needed to fully establish the sender
    * instance. This call should not block and any errors encountered on creation of
    * the backing sender resources should utilize the error handling mechanisms of
    * this AMQP bridge sender.
    */
   void start();

   /**
    * Close the AMQP bridge sender instance and cleans up its resources. This method
    * should not block and the actual resource shutdown work should occur asynchronously.
    */
   @Override
   void close();

   /**
    * @return the {@link AMQPBridgeManager} that this sender operates under.
    */
   AMQPBridgeManager getBridge();

   /**
    * @return an information object that defines the characteristics of the {@link AMQPBridgeSender}
    */
   AMQPBridgeSenderInfo getSenderInfo();

   /**
    * Provides and event point for notification of the sender having been closed by
    * the remote.
    *
    * @param handler
    *    The handler that will be invoked when the remote closes this sender.
    *
    * @return this sender instance.
    */
   AMQPBridgeSender setRemoteClosedHandler(Consumer<AMQPBridgeSender> handler);

}
