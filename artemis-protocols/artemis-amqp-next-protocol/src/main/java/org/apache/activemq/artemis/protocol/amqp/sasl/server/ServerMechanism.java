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

package org.apache.activemq.artemis.protocol.amqp.sasl.server;

import org.apache.activemq.artemis.protocol.amqp.AMQPRemotingConnection;
import org.apache.qpid.protonj2.buffer.ProtonBuffer;

/**
 * Base interface that defines the API that will be employed by the server
 * side SASL authentication layer.
 */
public interface ServerMechanism {

   /**
    * @return a unique name for this SASL mechanism which aids in log output.
    */
   String getName();

   /**
    * Given a {@link ProtonBuffer} containing the client initial response to a server
    * in the SASL initial frame that selected this mechanism either complete the SASL
    * authentication (success or fail) or return a new challenge that will be sent to
    * the remote for another round of processing.
    *
    * @param connection
    *       The server remote connection object
    * @param buffer
    *       The buffer that contains the SASL client response to the last challenge.
    *
    * @return a new buffer that contains the SASL challenge for the next round.
    */
   ProtonBuffer handleInitialResponse(AMQPRemotingConnection connection, ProtonBuffer buffer);

   /**
    * Given a {@link ProtonBuffer} containing the client response to a server
    * challenge either complete the SASL authentication (success or fail) or
    * return a new challenge that will be sent to the remote for another round
    * of processing.
    *
    * @param connection
    *       The server remote connection object
    * @param buffer
    *       The buffer that contains the SASL client response to the last challenge.
    *
    * @return a new buffer that contains the SASL challenge for the next round.
    */
   ProtonBuffer handleResponse(AMQPRemotingConnection connection, ProtonBuffer buffer);

   /**
    * @return quick check if the SASL mechanism has marked the exchange as done.
    */
   boolean isDone();

   /**
    * Will return the a {@link SASLResult} object once the exchange is considered done
    * and should throw an {@link IllegalStateException} if called before the {@link #isDone()}
    * method returns true.
    *
    * @return a {@link SASLResult} with details of the completed exchange.
    */
   SASLResult getResult();

}
