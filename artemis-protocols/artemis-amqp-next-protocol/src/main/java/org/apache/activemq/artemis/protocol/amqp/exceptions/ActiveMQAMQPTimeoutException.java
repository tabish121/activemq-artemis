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
package org.apache.activemq.artemis.protocol.amqp.exceptions;

import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.qpid.protonj2.types.transport.AmqpError;

/**
 * A wrapper exception for the errors that occur due to a timeout of an operation which
 * we will here map to the error condition "amqp:illegal-state".
 */
public class ActiveMQAMQPTimeoutException extends ActiveMQAMQPException {

   private static final long serialVersionUID = 5740642123760774458L;

   public ActiveMQAMQPTimeoutException(String message) {
      super(AmqpError.ILLEGAL_STATE.toString(), message, ActiveMQExceptionType.CONNECTION_TIMEDOUT);
   }
}
