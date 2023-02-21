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

package org.apache.activemq.artemis.protocol.amqp;

import java.security.Principal;
import java.util.Arrays;
import java.util.concurrent.Executor;

import org.apache.activemq.artemis.protocol.amqp.sasl.server.SASLResult;
import org.apache.activemq.artemis.protocol.amqp.sasl.server.ServerMechanism;
import org.apache.activemq.artemis.protocol.amqp.sasl.server.ServerMechanisms;
import org.apache.activemq.artemis.protocol.amqp.server.AMQPIncomingConnectionContext;
import org.apache.activemq.artemis.protocol.amqp.server.AMQPOutgoingConnectionContext;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.engine.Engine;
import org.apache.qpid.protonj2.engine.EngineFactory;
import org.apache.qpid.protonj2.engine.sasl.SaslClientContext;
import org.apache.qpid.protonj2.engine.sasl.SaslServerContext;
import org.apache.qpid.protonj2.engine.sasl.SaslServerListener;
import org.apache.qpid.protonj2.engine.sasl.SaslSystemException;
import org.apache.qpid.protonj2.engine.sasl.client.SaslAuthenticator;
import org.apache.qpid.protonj2.engine.sasl.client.SaslCredentialsProvider;
import org.apache.qpid.protonj2.engine.sasl.client.SaslMechanismSelector;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.transport.AMQPHeader;

/**
 * Utility factory class that provides the setup and configuration of an
 * {@link AMQPRemotingConnection} for various purposes such as client inbound
 * and server outbound connections.
 */
public final class AMQPRemotingConnectionFactory {

   /**
    * Creates a new {@link AMQPRemotingConnection} configured for handling inbound client connections.
    *
    * @param manager
    *      The protocol manager that is requesting the new remote connection instance.
    * @param connection
    *       The transport connection to bind the remote connection to.
    * @param executor
    *       The connection executor that will be assigned to the connection.
    *
    * @return a newly configured remote connection instance that will handle a client connection.
    */
   public static AMQPRemotingConnection clinetInbound(AMQPProtocolManager manager, Connection connection, Executor executor) {
      final Engine engine = EngineFactory.PROTON.createEngine();  // Default engine is SASL only.
      final SaslServerContext saslContext = engine.saslDriver().server();
      final AMQPRemotingConnection remotingConnection = new AMQPRemotingConnection(manager, connection, executor, engine);
      final AMQPIncomingConnectionContext context = new AMQPIncomingConnectionContext(remotingConnection, executor, engine.connection());

      saslContext.setListener(new SaslServerListener() {

         @Override
         public void handleSaslHeader(SaslServerContext context, AMQPHeader header) {
            // Client has sent its header now we need to send out set of mechanisms which
            // we have already validated and configured in the initialization phase
            context.sendMechanisms(manager.getSaslMechanisms());
         }

         @Override
         public void handleSaslInit(SaslServerContext context, Symbol mechanism, ProtonBuffer initResponse) {
            final ServerMechanisms chosenMechanismType = ServerMechanisms.valueOf(mechanism);
            if (chosenMechanismType == null) {
               context.saslFailure(new SaslSystemException(true,
                  "Received a SASL init but no supported mechanism found for: " + mechanism));
            }

            final ServerMechanism chosenMechanism = chosenMechanismType.createMechanism();
            final ProtonBuffer challenege = chosenMechanism.handleInitialResponse(remotingConnection, initResponse);

            if (challenege != null) {
               context.sendChallenge(challenege);
            } else if (chosenMechanism.isDone()) {
               final SASLResult result = chosenMechanism.getResult();
               context.sendOutcome(result.outcome(), null);
               remotingConnection.handleSaslOutcome(result);
            } else {
               context.saslFailure(new SaslSystemException(true,
                  "SASL authentication not complete but no challenege generated: " + mechanism));
            }
         }

         @Override
         public void handleSaslResponse(SaslServerContext context, ProtonBuffer response) {
            final ServerMechanism chosenMechanism = context.getLinkedResource();

            if (chosenMechanism == null) {
               context.saslFailure(new SaslSystemException(true,
                  "Received a SASL response but no mechanism chosen beforehand."));
            }

            final ProtonBuffer challenege = chosenMechanism.handleResponse(remotingConnection, response);
            if (challenege != null) {
               context.sendChallenge(challenege);
            } else if (chosenMechanism.isDone()) {
               final SASLResult result = chosenMechanism.getResult();
               context.sendOutcome(result.outcome(), null);
               remotingConnection.handleSaslOutcome(result);
            } else {
               context.saslFailure(new SaslSystemException(true,
                  "SASL authentication not complete but no challenege generated: " + chosenMechanism.getName()));
            }
         }
      });

      // The connection will be tied to the specific context which will handle all the events
      // and create new resources for incoming sessions and links.
      engine.connection().setLinkedResource(context);
      engine.start();

      return remotingConnection;
   }

   /**
    * Creates a new {@link AMQPRemotingConnection} configured for handling outbound server connections.
    *
    * @param manager
    *      The protocol manager that is requesting the new remote connection instance.
    * @param connection
    *       The transport connection to bind the remote connection to.
    * @param executor
    *       The connection executor that will be assigned to the connection.
    *
    * @return a newly configured remote connection instance that will handle an outbound connection.
    */
   public static AMQPRemotingConnection serverOutbound(AMQPProtocolManager manager, Connection connection, Executor executor) {
      final Engine engine = EngineFactory.PROTON.createEngine();  // Default engine is SASL only.
      final SaslClientContext saslContext = engine.saslDriver().client();
      final AMQPRemotingConnection remotingConnection = new AMQPRemotingConnection(manager, connection, executor, engine);
      final AMQPOutgoingConnectionContext context = new AMQPOutgoingConnectionContext(remotingConnection, executor, engine.connection());

      SaslMechanismSelector mechSelector =
         new SaslMechanismSelector(Arrays.asList(remotingConnection.getConfiguration().getSaslMechanisms()));

      saslContext.setListener(new SaslAuthenticator(mechSelector, new SaslCredentialsProvider() {

         // TODO : Figure out where credentials come from

         @Override
         public String vhost() {
            return null;
         }

         @Override
         public String username() {
            return null;
         }

         @Override
         public String password() {
            return null;
         }

         @Override
         public Principal localPrincipal() {
            return null;
         }
      }));

      // TODO: This trigger outgoing client SASL negotiations with the remote and then opens the AMQP connection.
      //       if SASL succeeds.  Should this go elsewhere ?? Depends on who opens the connection etc.
      // The connection will be tied to the specific context which will handle all the events
      // and create new resources for incoming sessions and links.
      engine.connection().setLinkedResource(context);
      engine.start();
      engine.connection().open();

      return remotingConnection;
   }
}
