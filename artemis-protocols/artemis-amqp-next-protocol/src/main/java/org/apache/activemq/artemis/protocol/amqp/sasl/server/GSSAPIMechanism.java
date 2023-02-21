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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.LoginContext;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.apache.activemq.artemis.protocol.amqp.AMQPRemotingConnection;
import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.engine.sasl.SaslOutcome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GSSAPI SASL server mechanism which delegates to the JDK
 */
public final class GSSAPIMechanism implements ServerMechanism {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public static final String NAME = "GSSAPI";

   private SaslServer saslServer;
   private Subject jaasId;
   private SASLResult result;

   @Override
   public String getName() {
      return NAME;
   }

   @Override
   public ProtonBuffer handleInitialResponse(AMQPRemotingConnection connection, ProtonBuffer buffer) {
      try {
         // Populate subject with acceptor private credentials
         LoginContext loginContext = new LoginContext(connection.getConfiguration().getSaslLoginConfigScope());
         loginContext.login();
         jaasId = loginContext.getSubject();

         saslServer = Subject.doAs(jaasId, (PrivilegedExceptionAction<SaslServer>) () -> Sasl.createSaslServer(NAME, null, null, new HashMap<String, String>(), new CallbackHandler() {
            @Override
            public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
               for (Callback callback : callbacks) {
                  if (callback instanceof AuthorizeCallback) {
                     AuthorizeCallback authorizeCallback = (AuthorizeCallback) callback;
                     // only ok to authenticate as self
                     authorizeCallback.setAuthorized(authorizeCallback.getAuthenticationID().equals(authorizeCallback.getAuthorizationID()));
                  }
               }
            }
         }));

         return processResponse(buffer);
      } catch (Exception fatal) {
         logger.info("Error on sasl input: {}", fatal.toString(), fatal);
         result = new SimpleSASLResult(null, null, SaslOutcome.SASL_SYS);
      } finally {
         if (result != null) {
            try {
               saslServer.dispose();
            } catch (SaslException error) {
               logger.trace("Exception on GSSAPI SASL server dispose", error);
            }
         }
      }

      return null;
   }

   @Override
   public ProtonBuffer handleResponse(AMQPRemotingConnection connection, ProtonBuffer buffer) {
      try {
         return processResponse(buffer);
      } catch (Exception fatal) {
         logger.info("Error on sasl input: {}", fatal.toString(), fatal);
         result = new SimpleSASLResult(null, null, SaslOutcome.SASL_SYS);
      } finally {
         if (result != null) {
            try {
               saslServer.dispose();
            } catch (SaslException error) {
               logger.trace("Exception on GSSAPI SASL server dispose", error);
            }
         }
      }

      return null;
   }

   private ProtonBuffer processResponse(ProtonBuffer buffer) throws PrivilegedActionException {
      final byte[] response = new byte[buffer.getReadableBytes()];
      buffer.readBytes(response, 0, response.length);

      final byte[] challenge = Subject.doAs(jaasId, (PrivilegedExceptionAction<byte[]>) () -> saslServer.evaluateResponse(response));
      if (saslServer.isComplete()) {
         final Principal principal = new KerberosPrincipal(saslServer.getAuthorizationID());
         final Subject identity = new Subject();

         identity.getPrivateCredentials().add(principal);

         result = new SimpleSASLResult(principal.getName(), identity, SaslOutcome.SASL_OK);
      }

      return ProtonBufferAllocator.defaultAllocator().copy(challenge);
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
