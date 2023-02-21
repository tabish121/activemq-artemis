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
import org.apache.qpid.protonj2.engine.sasl.SaslOutcome;

/**
 * Server side SASL Anonymous mechanism
 */
public final class AnonymousMechanism implements ServerMechanism {

   public static String NAME = "ANONYMOUS";

   public SASLResult result;

   @Override
   public String getName() {
      return NAME;
   }

   @Override
   public ProtonBuffer handleInitialResponse(AMQPRemotingConnection connection, ProtonBuffer buffer) {
      try {
         if (connection.getServer().getSecurityStore().isSecurityEnabled()) {
            // Should throw if anonymous logins are not allowed.
            connection.getServer().getSecurityStore().authenticate(null, null, null);
         }
         result = new SimpleSASLResult(null, null, SaslOutcome.SASL_OK);
      } catch (Exception ex) {
         result = new SimpleSASLResult(null, null, SaslOutcome.SASL_SYS);
      }

      return null;
   }

   @Override
   public ProtonBuffer handleResponse(AMQPRemotingConnection connection, ProtonBuffer buffer) {
      return null;
   }

   @Override
   public boolean isDone() {
      return true;
   }

   @Override
   public SASLResult getResult() {
      return result;
   }
}
