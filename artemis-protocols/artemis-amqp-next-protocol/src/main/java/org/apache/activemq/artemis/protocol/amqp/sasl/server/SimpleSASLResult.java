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

import javax.security.auth.Subject;

import org.apache.qpid.protonj2.engine.sasl.SaslOutcome;

/**
 * A simple {@link SASLResult} type that only carries provided data and has no
 * logic and is immutable.
 */
public final class SimpleSASLResult implements SASLResult {

   private final String username;
   private final Subject subject;
   private final SaslOutcome outcome;

   public SimpleSASLResult(String username, Subject subject, SaslOutcome outcome) {
      this.username = username;
      this.subject = subject;
      this.outcome = outcome;
   }

   @Override
   public String getUsername() {
      return username;
   }

   @Override
   public Subject getSubject() {
      return subject;
   }

   @Override
   public SaslOutcome outcome() {
      return outcome;
   }
}
