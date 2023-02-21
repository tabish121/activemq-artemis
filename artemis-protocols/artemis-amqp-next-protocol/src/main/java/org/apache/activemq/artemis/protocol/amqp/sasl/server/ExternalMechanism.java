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

import java.lang.invoke.MethodHandles;
import java.security.Principal;

import javax.security.auth.Subject;

import org.apache.activemq.artemis.core.remoting.CertificateUtil;
import org.apache.activemq.artemis.protocol.amqp.AMQPRemotingConnection;
import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.engine.sasl.SaslOutcome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the SASL External mechanism for the server side.
 */
public class ExternalMechanism implements ServerMechanism {

   public static final String NAME = "EXTERNAL";

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static final ProtonBuffer CHALLENGE = ProtonBufferAllocator.defaultAllocator().copy(new byte[0]).convertToReadOnly();

   private SASLResult result;

   @Override
   public String getName() {
      return NAME;
   }

   @Override
   public ProtonBuffer handleInitialResponse(AMQPRemotingConnection connection, ProtonBuffer buffer) {
      if (buffer != null) {
         validatePeerChallengeResponse(connection, buffer);
         return null;
      } else {
         // Client didn't offer preferred identity in initial response so prompt for it.
         return CHALLENGE;
      }
   }

   @Override
   public ProtonBuffer handleResponse(AMQPRemotingConnection connection, ProtonBuffer buffer) {
      if (buffer != null) {
         validatePeerChallengeResponse(connection, buffer);
      } else {
         // We challenged but the remote again did not respond so we stop things here
         // and fail the SASL authentication.
         result = new SimpleSASLResult(null, null, SaslOutcome.SASL_AUTH);
      }

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

   private void validatePeerChallengeResponse(AMQPRemotingConnection connection, ProtonBuffer response) {
      if (response.isReadable()) {
         // We don't current accept client provided identities so failed authentication here
         result = new SimpleSASLResult(null, null, SaslOutcome.SASL_AUTH);
      } else {
         final Principal principal = CertificateUtil.getPeerPrincipalFromConnection(connection);

         if (principal == null) {
            logger.debug("SASL EXTERNAL mechanism requires a TLS peer principal");
            // We should have validated that we could support this mechanism before offering
            // it to the remote but in case we get here, fail the authentication step.
            result = new SimpleSASLResult(null, null, SaslOutcome.SASL_AUTH);
         } else {
            final Subject subject = new Subject();
            subject.getPrivateCredentials().add(principal);
            result = new SimpleSASLResult(principal.getName(), subject, SaslOutcome.SASL_AUTH);
         }
      }
   }
}
