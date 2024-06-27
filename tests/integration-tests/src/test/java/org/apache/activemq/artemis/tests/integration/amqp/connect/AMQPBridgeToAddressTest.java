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
package org.apache.activemq.artemis.tests.integration.amqp.connect;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;

import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBridgeAddressPolicyElement;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBridgeBrokerConnectionElement;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.tests.integration.amqp.AmqpClientTestSupport;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.qpid.protonj2.test.driver.ProtonTestServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the AMQP Bridge to address configuration and protocol behaviors
 */
class AMQPBridgeToAddressTest  extends AmqpClientTestSupport {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   @Override
   protected String getConfiguredProtocols() {
      return "AMQP,CORE";
   }

   @Override
   protected ActiveMQServer createServer() throws Exception {
      // Creates the broker used to make the outgoing connection. The port passed is for
      // that brokers acceptor. The test server connected to by the broker binds to a random port.
      return createServer(AMQP_PORT, false);
   }

   @Test
   @Timeout(20)
   public void testBridgeCreatesAddressSenderWhenLocalAddressIsStaticlyDefined() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPBridgeAddressPolicyElement sendToAddress = new AMQPBridgeAddressPolicyElement();
         sendToAddress.setName("address-policy");
         sendToAddress.addToIncludes("test");

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeToAddressPolicy(sendToAddress);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofSender()
                            .withTarget().withAddress("test").also()
                            .withSource().withAddress("test").also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test"),
                                            containsString("address-sender"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();
         peer.remoteFlow().withLinkCredit(1).queue();

         server.addAddressInfo(new AddressInfo(SimpleString.of("test"), RoutingType.MULTICAST));

         Wait.assertTrue(() -> server.addressQuery(SimpleString.of("test")).isExists());
         Wait.assertTrue(() -> server.bindingQuery(SimpleString.of("test")).getQueueNames().size() > 0);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectDetach().respond();

         // This should trigger the bridged sender to be shut down as the statically defined address
         // should be the thing that triggers the bridging.
         logger.info("Removing Addres from bridged address to eliminate sender");
         server.removeAddressInfo(SimpleString.of("test"), null, true);
         Wait.assertFalse(() -> server.addressQuery(SimpleString.of("test")).isExists());

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectClose();
         peer.remoteClose().now();
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test
   @Timeout(20)
   public void testBridgeAddressSenderRoutesMessageFromLocalProducerToTheRemote() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPBridgeAddressPolicyElement sendToAddress = new AMQPBridgeAddressPolicyElement();
         sendToAddress.setName("address-policy");
         sendToAddress.addToIncludes("test");

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeToAddressPolicy(sendToAddress);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofSender()
                            .withTarget().withAddress("test").also()
                            .withSource().withAddress("test").also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test"),
                                            containsString("address-sender"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();
         peer.remoteFlow().withLinkCredit(1).queue();

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         // Producer connect should create the address and initiate the bridge sender attach
         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = session.createProducer(session.createTopic("test"));

            // Await bridge sender attach.
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withMessageFormat(0)
                                 .withMessage().withHeader().also()
                                               .withMessageAnnotations().also()
                                               .withProperties().also()
                                               .withValue("Hello")
                                               .and()
                                 .respond()
                                 .withSettled(true)
                                 .withState().accepted();

            connection.start();

            Wait.assertTrue(() -> server.addressQuery(SimpleString.of("test")).isExists());
            Wait.assertTrue(() -> server.bindingQuery(SimpleString.of("test")).getQueueNames().size() > 0);

            producer.send(session.createTextMessage("Hello"));
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectDetach().respond();

         // This should trigger the bridged sender to be shut down as the statically defined address
         // should be the thing that triggers the bridging.
         logger.info("Removing Addres from bridged address to eliminate sender");
         server.removeAddressInfo(SimpleString.of("test"), null, true);
         Wait.assertFalse(() -> server.addressQuery(SimpleString.of("test")).isExists());

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectClose();
         peer.remoteClose().now();
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test
   @Timeout(20)
   public void testBridgeAddressSenderRoutesMessageMatchingFilterFromLocalProducerToTheRemote() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPBridgeAddressPolicyElement sendToAddress = new AMQPBridgeAddressPolicyElement();
         sendToAddress.setName("address-policy");
         sendToAddress.addToIncludes("test");
         sendToAddress.setFilter("color = 'red'");

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeToAddressPolicy(sendToAddress);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofSender()
                            .withTarget().withAddress("test").also()
                            .withSource().withAddress("test").also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test"),
                                            containsString("address-sender"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();
         peer.remoteFlow().withLinkCredit(1).queue();

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         // Producer connect should create the address and initiate the bridge sender attach
         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = session.createProducer(session.createTopic("test"));

            // Await bridge sender attach.
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withMessageFormat(0)
                                 .withMessage().withHeader().also()
                                               .withMessageAnnotations().also()
                                               .withProperties().also()
                                               .withApplicationProperties().withProperty("color", "red").also()
                                               .withValue("red")
                                               .and()
                                 .respond()
                                 .withSettled(true)
                                 .withState().accepted();

            connection.start();

            Wait.assertTrue(() -> server.addressQuery(SimpleString.of("test")).isExists());
            Wait.assertTrue(() -> server.bindingQuery(SimpleString.of("test")).getQueueNames().size() > 0);

            final TextMessage blue = session.createTextMessage("blue");
            blue.setStringProperty("color", "blue");

            final TextMessage red = session.createTextMessage("red");
            red.setStringProperty("color", "red");

            producer.send(blue); // Should be filtered out
            producer.send(red);
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectDetach().respond();

         // This should trigger the bridged sender to be shut down as the statically defined address
         // should be the thing that triggers the bridging.
         logger.info("Removing Addres from bridged address to eliminate sender");
         server.removeAddressInfo(SimpleString.of("test"), null, true);
         Wait.assertFalse(() -> server.addressQuery(SimpleString.of("test")).isExists());

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectClose();
         peer.remoteClose().now();
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }
}
