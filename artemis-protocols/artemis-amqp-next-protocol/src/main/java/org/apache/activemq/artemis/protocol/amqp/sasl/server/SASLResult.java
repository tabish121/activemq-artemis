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
 * Interface used to convey the outcome of SASL authentication for a remote peer that
 * is connecting to the server instance.
 */
public interface SASLResult {

   /**
    * The authenticated user name if the SASL mechanism carries this information
    *
    * @return the user name provided in the SASL exchange if available or null.
    */
   String getUsername();

   /**
    * The Authenticated Subject that resulted from the SASL exchange.
    *
    * @return the {@link Subject} that results from this SASL exchange, never null.
    */
   Subject getSubject();

   /**
    * @return the outcome of the SASL authentication process.
    */
   SaslOutcome outcome();

   /**
    * @return true if the outcome value is SASL OK otherwise return false.
    */
   default boolean isOK() {
      return SaslOutcome.SASL_OK.equals(outcome());
   }
}
