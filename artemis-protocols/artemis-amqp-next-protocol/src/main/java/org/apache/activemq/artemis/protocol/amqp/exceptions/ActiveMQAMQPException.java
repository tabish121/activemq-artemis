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

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;

public class ActiveMQAMQPException extends ActiveMQException {

   private static final long serialVersionUID = -1192173210322691191L;

   public String getErrorCondition() {
      return condition;
   }

   private final String condition;

   public ActiveMQAMQPException(String condition, String message, Throwable e, ActiveMQExceptionType t) {
      super(message, e, t);
      this.condition = condition;
   }

   public ActiveMQAMQPException(String condition, String message, ActiveMQExceptionType t) {
      super(message, t);
      this.condition = condition;
   }
}
