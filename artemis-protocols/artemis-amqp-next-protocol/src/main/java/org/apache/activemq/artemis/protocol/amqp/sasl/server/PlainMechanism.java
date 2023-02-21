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

import java.nio.charset.StandardCharsets;

import org.apache.activemq.artemis.protocol.amqp.AMQPRemotingConnection;
import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.engine.sasl.SaslOutcome;

/**
 * SASL Server Plain authentication mechanism
 */
public final class PlainMechanism implements ServerMechanism {

   public static String NAME = "PLAIN";

   private SASLResult result;

   @Override
   public String getName() {
      return NAME;
   }

   @Override
   public ProtonBuffer handleInitialResponse(AMQPRemotingConnection connection, ProtonBuffer buffer) {
      String username = null;
      String password = null;
      String bytes = buffer.toString(StandardCharsets.UTF_8);
      String[] credentials = bytes.split(Character.toString((char) 0));

      switch (credentials.length) {
         case 2:
            username = credentials[0];
            password = credentials[1];
            break;
         case 3:
            username = credentials[1];
            password = credentials[2];
            break;
         default:
            break;
      }

      if (connection.getServer().getSecurityStore().isSecurityEnabled()) {
         try {
            connection.getServer().getSecurityStore().authenticate(
               username, password, connection, connection.getProtocolManager().getSecurityDomain());
            result = new SimpleSASLResult(username, null, SaslOutcome.SASL_OK);
         } catch (Exception e) {
            result = new SimpleSASLResult(username, null, SaslOutcome.SASL_AUTH);
         }
      } else {
         result = new SimpleSASLResult(username, null, SaslOutcome.SASL_OK);
      }

      return null;
   }

   @Override
   public ProtonBuffer handleResponse(AMQPRemotingConnection connection, ProtonBuffer buffer) {
      return null;
   }

   @Override
   public boolean isDone() {
      return result != null;
   }

   @Override
   public SASLResult getResult() {
      return result;
   }
}
