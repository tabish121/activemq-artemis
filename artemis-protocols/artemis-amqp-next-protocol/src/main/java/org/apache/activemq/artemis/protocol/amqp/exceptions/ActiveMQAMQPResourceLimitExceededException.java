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
 * A wrapper exception for the errors that occur due to a configured resource limit having been
 * exceeded which we will map to the error condition "amqp:resource-limit-exceeded".
 */
public class ActiveMQAMQPResourceLimitExceededException extends ActiveMQAMQPException {

   private static final long serialVersionUID = -2838551882841160071L;

   public ActiveMQAMQPResourceLimitExceededException(String message) {
      super(AmqpError.RESOURCE_LIMIT_EXCEEDED.toString(), message, ActiveMQExceptionType.ADDRESS_FULL);
   }
}
