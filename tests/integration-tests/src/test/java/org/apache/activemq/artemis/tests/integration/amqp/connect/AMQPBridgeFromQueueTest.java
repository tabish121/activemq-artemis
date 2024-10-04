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

import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.IGNORE_QUEUE_FILTERS;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.LINK_RECOVERY_DELAY;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.LINK_RECOVERY_INITIAL_DELAY;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.MAX_LINK_RECOVERY_ATTEMPTS;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.PRESETTLE_SEND_MODE;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.RECEIVER_CREDITS;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.RECEIVER_CREDITS_LOW;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.BRIDGE_RECEIVER_PRIORITY;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.IGNORE_QUEUE_CONSUMER_FILTERS;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.PULL_RECEIVER_BATCH_SIZE;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.DEFAULT_PULL_CREDIT_BATCH_SIZE;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.DISABLE_RECEIVER_DEMAND_TRACKING;
import static org.apache.activemq.artemis.protocol.amqp.connect.bridge.AMQPBridgeConstants.DISABLE_RECEIVER_PRIORITY;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;

import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.TransformerConfiguration;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBridgeBrokerConnectionElement;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBridgeQueuePolicyElement;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.tests.integration.amqp.AmqpClientTestSupport;
import org.apache.activemq.artemis.tests.integration.amqp.connect.AMQPFederationQueuePolicyTest.ApplicationPropertiesTransformer;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.LinkError;
import org.apache.qpid.protonj2.test.driver.ProtonTestServer;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the AMQP bridge from queue behavior.
 */
class AMQPBridgeFromQueueTest extends AmqpClientTestSupport {

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
   public void testBridgeCreatesQueueReceiverLinkForQueueMatchAnycast() throws Exception {
      doTestBridgeCreatesQueueReceiverLinkForQueueMatch(RoutingType.ANYCAST);
   }

   @Test
   @Timeout(20)
   public void testBridgeCreatesQueueReceiverLinkForQueueMatchMulticast() throws Exception {
      doTestBridgeCreatesQueueReceiverLinkForQueueMatch(RoutingType.MULTICAST);
   }

   private void doTestBridgeCreatesQueueReceiverLinkForQueueMatch(RoutingType routingType) throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(QueueConfiguration.of("test").setRoutingType(routingType)
                                                         .setAddress("test")
                                                         .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("test::test").also()
                            .withSource().withAddress("test").also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();
         peer.expectFlow().withLinkCredit(1000);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            if (RoutingType.MULTICAST.equals(routingType)) {
               // Requires FQQN to meet the test expectations
               session.createConsumer(session.createTopic("test::test"));
            } else {
               session.createConsumer(session.createQueue("test"));
            }

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectClose();
         peer.remoteClose().now();
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test
   @Timeout(20)
   public void testBridgeQueueReceiverCarriesConfiguredPolicyFilter() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.setFilter("color='red'");
         receiveFromQueue.addToIncludes("test", "test");

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(QueueConfiguration.of("test").setRoutingType(RoutingType.ANYCAST)
                                                         .setAddress("test")
                                                         .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("test::test").also()
                            .withSource().withAddress("test")
                                         .withJMSSelector("color='red'").also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();
         peer.expectFlow().withLinkCredit(1000);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }

   @Test
   @Timeout(20)
   public void testBridgeQueueReceiverCarriesConfiguredPolicyFilterOverQueueFilter() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.setFilter("color='red'");
         receiveFromQueue.addToIncludes("test", "test");

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(QueueConfiguration.of("test").setRoutingType(RoutingType.ANYCAST)
                                                         .setAddress("test")
                                                         .setFilterString("color='green'")
                                                         .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("test::test").also()
                            .withSource().withAddress("test")
                                         .withJMSSelector("color='red'").also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();
         peer.expectFlow().withLinkCredit(1000);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }

   @Test
   @Timeout(20)
   public void testBridgeQueueReceiverCarriesConfiguredQueueFilter() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(QueueConfiguration.of("test").setRoutingType(RoutingType.ANYCAST)
                                                         .setAddress("test")
                                                         .setFilterString("color='red'")
                                                         .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("test::test").also()
                            .withSource().withAddress("test")
                                         .withJMSSelector("color='red'").also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();
         peer.expectFlow().withLinkCredit(1000);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }

   @Test
   @Timeout(20)
   public void testBridgeQueueReceiverCarriesConsumerQueueFilter() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(QueueConfiguration.of("test").setRoutingType(RoutingType.ANYCAST)
                                                         .setAddress("test")
                                                         .setFilterString("color='red' OR color = 'blue'")
                                                         .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("test::test").also()
                            .withSource().withAddress("test")
                                         .withJMSSelector("color='red'").also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();
         peer.expectFlow().withLinkCredit(1000);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final Queue queue  = session.createQueue("test");

            session.createConsumer(queue, "color='red'"); // new receiver for this selector

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectAttach().ofReceiver()
                               .withTarget().withAddress("test::test").also()
                               .withSource().withAddress("test")
                                            .withJMSSelector("color='blue'").also()
                               .withName(allOf(containsString(getTestName()),
                                               containsString("test"),
                                               containsString("queue-receiver"),
                                               containsString("amqp-bridge"),
                                               containsString(server.getNodeID().toString())))
                               .respond();
            peer.expectFlow().withLinkCredit(1000);

            session.createConsumer(queue, "color='blue'"); // new receiver for this selector

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectAttach().ofReceiver()
                               .withTarget().withAddress("test::test").also()
                               .withSource().withAddress("test")
                                            .withJMSSelector("color='red' OR color = 'blue'").also()
                               .withName(allOf(containsString(getTestName()),
                                               containsString("test"),
                                               containsString("queue-receiver"),
                                               containsString("amqp-bridge"),
                                               containsString(server.getNodeID().toString())))
                               .respond();
            peer.expectFlow().withLinkCredit(1000);

            session.createConsumer(queue); // No selector so the queue filter should be used

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // Should not see more attaches as these match previous variations
            session.createConsumer(queue, "color='blue'");
            session.createConsumer(queue, "color='red'");
            session.createConsumer(queue);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }

   @Test
   @Timeout(20)
   public void testBridgeQueueReceiverCanIgnoreConsumerQueueFilter() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");
         receiveFromQueue.addProperty(IGNORE_QUEUE_CONSUMER_FILTERS, "true");

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(QueueConfiguration.of("test").setRoutingType(RoutingType.ANYCAST)
                                                         .setAddress("test")
                                                         .setFilterString("color='red' OR color = 'blue'")
                                                         .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("test::test").also()
                            .withSource().withAddress("test")
                                         .withJMSSelector("color='red' OR color = 'blue'").also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();
         peer.expectFlow().withLinkCredit(1000);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final Queue queue  = session.createQueue("test");

            session.createConsumer(queue, "color='red'"); // Consumer filter should be ignored

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            session.createConsumer(queue, "color='blue'"); // Consumer filter should be ignored

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }

   @Test
   @Timeout(20)
   public void testBridgeQueueReceiverCanIgnoreAllFilters() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");
         receiveFromQueue.addProperty(IGNORE_QUEUE_CONSUMER_FILTERS, "true");
         receiveFromQueue.addProperty(IGNORE_QUEUE_FILTERS, "true");

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(QueueConfiguration.of("test").setRoutingType(RoutingType.ANYCAST)
                                                         .setAddress("test")
                                                         .setFilterString("color='red' OR color = 'blue'")
                                                         .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("test::test").also()
                            .withSource().withAddress("test")
                                         .withFilter(nullValue()).also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();
         peer.expectFlow().withLinkCredit(1000);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final Queue queue  = session.createQueue("test");

            session.createConsumer(queue, "color='red'"); // Consumer filter should be ignored

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            session.createConsumer(queue, "color='blue'"); // Consumer filter should be ignored

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }

   @Test
   @Timeout(20)
   public void testBridgeCreatesQueueReceiverLinkForQueueMatchUsingPolicyCredit() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");
         receiveFromQueue.addProperty(RECEIVER_CREDITS, "30");
         receiveFromQueue.addProperty(RECEIVER_CREDITS_LOW, "3");

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(QueueConfiguration.of("test").setRoutingType(RoutingType.ANYCAST)
                                                         .setAddress("test")
                                                         .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("test::test").also()
                            .withSource().withAddress("test")
                                         .withFilter(nullValue()).also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();
         peer.expectFlow().withLinkCredit(30);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }

   @Test
   @Timeout(20)
   public void testBridgeClosesQueueReceiverWhenDemandIsRemovedFromQueue() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(QueueConfiguration.of("test").setRoutingType(RoutingType.ANYCAST)
                                                         .setAddress("test")
                                                         .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("test::test").also()
                            .withSource().withAddress("test")
                                         .withFilter(nullValue()).also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();
         peer.expectFlow().withLinkCredit(1000).optional();

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test
   @Timeout(20)
   public void testBridgeHandlesQueueDeletedAndConsumerRecreates() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(QueueConfiguration.of("test").setRoutingType(RoutingType.ANYCAST)
                                                         .setAddress("test")
                                                         .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("test::test").also()
                            .withSource().withAddress("test")
                                         .withFilter(nullValue()).also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();
         peer.expectFlow().withLinkCredit(1000).optional();

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();

            server.destroyQueue(SimpleString.of("test"), null, false, true);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         }

         // Consumer recreates Queue and adds demand back and bridge should restart
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("test::test").also()
                            .withSource().withAddress("test")
                                         .withFilter(nullValue()).also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();
         peer.expectFlow().withLinkCredit(1000).optional();

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test"));

            connection.start();

            peer.expectDetach().respond();
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test
   @Timeout(20)
   public void testSecondQueueConsumerDoesNotGenerateAdditionalBridgeReceiver() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(QueueConfiguration.of("test").setRoutingType(RoutingType.ANYCAST)
                                                         .setAddress("test")
                                                         .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("test::test").also()
                            .withSource().withAddress("test")
                                         .withFilter(nullValue()).also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();
         peer.expectFlow().withLinkCredit(1000).optional();

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test"));
            session.createConsumer(session.createQueue("test"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test
   @Timeout(20)
   public void testLinkCreatedForEachDistinctQueueMatchInSameConfiguredPolicy() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("addr1", "test.1");
         receiveFromQueue.addToIncludes("addr2", "test.2");

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(QueueConfiguration.of("test.1").setRoutingType(RoutingType.ANYCAST)
                                                           .setAddress("addr1")
                                                           .setAutoCreated(false));
         server.createQueue(QueueConfiguration.of("test.2").setRoutingType(RoutingType.ANYCAST)
                                                           .setAddress("addr2")
                                                           .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("addr1::test.1").also()
                            .withSource().withAddress("test.1")
                                         .withFilter(nullValue()).also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test.1"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();
         peer.expectFlow().withLinkCredit(1000).optional();

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test.1"));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectAttach().ofReceiver()
                               .withTarget().withAddress("addr2::test.2").also()
                               .withSource().withAddress("test.2")
                                            .withFilter(nullValue()).also()
                               .withName(allOf(containsString(getTestName()),
                                               containsString("test.2"),
                                               containsString("queue-receiver"),
                                               containsString("amqp-bridge"),
                                               containsString(server.getNodeID().toString())))
                               .respond();
            peer.expectFlow().withLinkCredit(1000).optional();

            session.createConsumer(session.createQueue("test.2"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();
            peer.expectDetach().respond();
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test
   @Timeout(20)
   public void testBridgeReceiverCreatedWhenWildcardPolicyMatchesConsumerQueue() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("", "test.#");

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(QueueConfiguration.of("test.queue").setRoutingType(RoutingType.ANYCAST)
                                                               .setAddress("test")
                                                               .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("test::test.queue").also()
                            .withSource().withAddress("test.queue")
                                         .withFilter(nullValue()).also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test.queue"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();
         peer.expectFlow().withLinkCredit(1000);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test.queue"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test
   @Timeout(20)
   public void testRemoteCloseOfQueueReceiverRespondsToDetach() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("", "test.#");

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(QueueConfiguration.of("test.queue").setRoutingType(RoutingType.ANYCAST)
                                                               .setAddress("test")
                                                               .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("test::test.queue").also()
                            .withSource().withAddress("test.queue")
                                         .withFilter(nullValue()).also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test.queue"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();
         peer.expectFlow().withLinkCredit(1000);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test.queue"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach();
            peer.remoteDetach().withErrorCondition("amqp:resource-deleted", "the resource was deleted").afterDelay(10).now();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test
   @Timeout(20)
   public void testRejectedQueueReceiverAttachWhenLocalMatchingQueueNotFoundIsHandled() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("", "test.#");

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(QueueConfiguration.of("test.queue").setRoutingType(RoutingType.ANYCAST)
                                                               .setAddress("test")
                                                               .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("test::test.queue").also()
                            .withSource().withAddress("test.queue")
                                         .withFilter(nullValue()).also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test.queue"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();
         peer.expectFlow().withLinkCredit(1000);
         peer.remoteDetach().withErrorCondition("amqp:not-found", "the requested queue was not found").queue().afterDelay(10);
         peer.expectDetach();

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test.queue"));

            connection.start();

            // Broker normally treats any remote link closure on the broker connection as terminal
            // but shouldn't in this case as it indicates the requested bridged queue wasn't present
            // on the remote. New queue interest should result in a new attempt to bridge the queue
            // and this time we will let it succeed.

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectAttach().ofReceiver()
                               .withTarget().withAddress("test::test.queue").also()
                               .withSource().withAddress("test.queue")
                                            .withFilter(nullValue()).also()
                               .withName(allOf(containsString(getTestName()),
                                               containsString("test.queue"),
                                               containsString("queue-receiver"),
                                               containsString("amqp-bridge"),
                                               containsString(server.getNodeID().toString())))
                               .respond();
            peer.expectFlow().withLinkCredit(1000);

            session.createConsumer(session.createQueue("test.queue"));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test
   @Timeout(20)
   public void testRemoteCloseQueueReceiverWhenRemoteResourceIsDeletedIsHandled() throws Exception {
      doTestRemoteCloseQueueReceiverForExpectedConditionsIsHandled("amqp:resource-deleted");
   }

   @Test
   @Timeout(20)
   public void testRemoteCloseQueueReceiverWhenRemoteReceiverIsForcedToDetachIsHandled() throws Exception {
      doTestRemoteCloseQueueReceiverForExpectedConditionsIsHandled("amqp:link:detach-forced");
   }

   private void doTestRemoteCloseQueueReceiverForExpectedConditionsIsHandled(String condition) throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("", "test.#");

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(QueueConfiguration.of("test.queue").setRoutingType(RoutingType.ANYCAST)
                                                               .setAddress("test")
                                                               .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("test::test.queue").also()
                            .withSource().withAddress("test.queue")
                                         .withFilter(nullValue()).also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test.queue"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();
         peer.expectFlow().withLinkCredit(1000);
         peer.remoteDetach().withErrorCondition(condition, "error message from remote....").queue().afterDelay(20);
         peer.expectDetach();

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test.queue"));

            connection.start();

            // Broker normally treats any remote link closure on the broker connection as terminal
            // but shouldn't in this case as it indicates the requested bridge queue receiver was
            // forced closed. New queue interest should result in a new attempt to bridge the queue

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectAttach().ofReceiver()
                               .withTarget().withAddress("test::test.queue").also()
                               .withSource().withAddress("test.queue")
                                            .withFilter(nullValue()).also()
                               .withName(allOf(containsString(getTestName()),
                                               containsString("test.queue"),
                                               containsString("queue-receiver"),
                                               containsString("amqp-bridge"),
                                               containsString(server.getNodeID().toString())))
                               .respond();
            peer.expectFlow().withLinkCredit(1000);

            session.createConsumer(session.createQueue("test.queue"));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test
   @Timeout(20)
   public void testUnhandledRemoteReceiverCloseConditionCausesConnectionRebuild() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("", "test.#");

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(1); // One reconnect to meet test expectations and use a
         amqpConnection.setRetryInterval(100);   // Short reconnect interval.
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(QueueConfiguration.of("test.queue").setRoutingType(RoutingType.ANYCAST)
                                                               .setAddress("test")
                                                               .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("test::test.queue").also()
                            .withSource().withAddress("test.queue")
                                         .withFilter(nullValue()).also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test.queue"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();
         peer.expectFlow().withLinkCredit(1000);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test.queue"));

            connection.start();

            // Broker treats some remote link closures on the broker connection as terminal
            // in this case we signal some internal error which should cause rebuild of the
            // broker connection.
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            peer.expectDetach().optional(); // Broker is not consistent on sending the detach
            peer.expectClose().optional();
            peer.expectConnectionToDrop();
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver()
                               .withTarget().withAddress("test::test.queue").also()
                               .withSource().withAddress("test.queue")
                                            .withFilter(nullValue()).also()
                               .withName(allOf(containsString(getTestName()),
                                               containsString("test.queue"),
                                               containsString("queue-receiver"),
                                               containsString("amqp-bridge"),
                                               containsString(server.getNodeID().toString())))
                               .respond();
            peer.expectFlow().withLinkCredit(1000);

            // Trigger the error that should cause the broker to drop and reconnect
            peer.remoteDetach().withErrorCondition("amqp:internal-error", "the resource suffered an internal error").afterDelay(15).now();

            peer.waitForScriptToComplete(50, TimeUnit.SECONDS);
            peer.expectDetach(); // demand will be gone and receiver link should close.
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test
   @Timeout(20)
   public void testInboundMessageRoutedToReceiverOnLocalQueue() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("", "test.#");

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(QueueConfiguration.of("test.queue").setRoutingType(RoutingType.ANYCAST)
                                                               .setAddress("test")
                                                               .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("test::test.queue").also()
                            .withSource().withAddress("test.queue")
                                         .withFilter(nullValue()).also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test.queue"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();
         peer.expectFlow().withLinkCredit(1000);
         peer.remoteTransfer().withBody().withString("test-message")
                              .also()
                              .withDeliveryId(0)
                              .queue();
         peer.expectDisposition().withSettled(true).withState().accepted();

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer consumer = session.createConsumer(session.createQueue("test.queue"));

            connection.start();

            final Message message = consumer.receive(20_0000);
            assertNotNull(message);
            assertTrue(message instanceof TextMessage);
            assertEquals("test-message", ((TextMessage) message).getText());

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach(); // demand will be gone and receiver link should close.
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test
   @Timeout(20)
   public void testLinkCreatedForEachDistinctQueueMatchInSameConfiguredPolicyWithSameAddressMatch() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("addr", "test.1");
         receiveFromQueue.addToIncludes("addr", "test.2");
         receiveFromQueue.addToIncludes("addr", "test.3");

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(QueueConfiguration.of("test.1").setRoutingType(RoutingType.ANYCAST)
                                                           .setAddress("addr")
                                                           .setAutoCreated(false));
         server.createQueue(QueueConfiguration.of("test.2").setRoutingType(RoutingType.ANYCAST)
                                                           .setAddress("addr")
                                                           .setAutoCreated(false));
         server.createQueue(QueueConfiguration.of("test.3").setRoutingType(RoutingType.ANYCAST)
                                                           .setAddress("addr")
                                                           .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("addr::test.1").also()
                            .withSource().withAddress("test.1")
                                         .withFilter(nullValue()).also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test.1"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();
         peer.expectFlow().withLinkCredit(1000).optional();

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test.1"));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectAttach().ofReceiver()
                               .withTarget().withAddress("addr::test.2").also()
                               .withSource().withAddress("test.2")
                                            .withFilter(nullValue()).also()
                               .withName(allOf(containsString(getTestName()),
                                               containsString("test.2"),
                                               containsString("queue-receiver"),
                                               containsString("amqp-bridge"),
                                               containsString(server.getNodeID().toString())))
                               .respond();
            peer.expectFlow().withLinkCredit(1000).optional();

            session.createConsumer(session.createQueue("test.2"));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectAttach().ofReceiver()
                               .withTarget().withAddress("addr::test.3").also()
                               .withSource().withAddress("test.3")
                                            .withFilter(nullValue()).also()
                               .withName(allOf(containsString(getTestName()),
                                               containsString("test.3"),
                                               containsString("queue-receiver"),
                                               containsString("amqp-bridge"),
                                               containsString(server.getNodeID().toString())))
                               .respond();
            peer.expectFlow().withLinkCredit(1000).optional();

            session.createConsumer(session.createQueue("test.3"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();
            peer.expectDetach().respond();
            peer.expectDetach().respond();
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test
   @Timeout(20)
   public void testTransformInboundFederatedMessageBeforeDispatch() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final Map<String, String> newApplicationProperties = new HashMap<>();
         newApplicationProperties.put("appProperty1", "one");
         newApplicationProperties.put("appProperty2", "two");

         final TransformerConfiguration transformerConfiguration = new TransformerConfiguration();
         transformerConfiguration.setClassName(ApplicationPropertiesTransformer.class.getName());
         transformerConfiguration.setProperties(newApplicationProperties);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.setTransformerConfiguration(transformerConfiguration);
         receiveFromQueue.addToIncludes("test", "test");

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(QueueConfiguration.of("test").setRoutingType(RoutingType.ANYCAST)
                                                         .setAddress("test")
                                                         .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("test::test").also()
                            .withSource().withAddress("test")
                                         .withFilter(nullValue()).also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();
         peer.expectFlow().withLinkCredit(1000);
         peer.remoteTransfer().withBody().withString("test-message")
                              .also()
                              .withDeliveryId(0)
                              .queue();
         peer.expectDisposition().withSettled(true).withState().accepted();

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer consumer = session.createConsumer(session.createQueue("test"));

            connection.start();

            final Message message = consumer.receive(5_000);
            assertNotNull(message);
            assertTrue(message instanceof TextMessage);
            assertEquals("test-message", ((TextMessage) message).getText());
            assertEquals("one", message.getStringProperty("appProperty1"));
            assertEquals("two", message.getStringProperty("appProperty2"));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach(); // demand will be gone and receiver link should close.
         }

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test
   @Timeout(20)
   public void testPullQueueConsumerGrantsDefaultCreditOnEmptyQueue() throws Exception {
      doTestPullConsumerGrantsConfiguredCreditOnEmptyQueue(0, false, 0, false, DEFAULT_PULL_CREDIT_BATCH_SIZE);
   }

   @Test
   @Timeout(20)
   public void testPullQueueConsumerGrantsReceiverConfiguredCreditOnEmptyQueue() throws Exception {
      doTestPullConsumerGrantsConfiguredCreditOnEmptyQueue(0, false, 10, true, 10);
   }

   @Test
   @Timeout(20)
   public void testPullQueueConsumerGrantsFederationConfiguredCreditOnEmptyQueue() throws Exception {
      doTestPullConsumerGrantsConfiguredCreditOnEmptyQueue(20, true, 0, false, 20);
   }

   @Test
   @Timeout(20)
   public void testPullQueueConsumerGrantsReceiverConfiguredCreditOverFederationConfiguredOnEmptyQueue() throws Exception {
      doTestPullConsumerGrantsConfiguredCreditOnEmptyQueue(20, true, 10, true, 10);
   }

   private void doTestPullConsumerGrantsConfiguredCreditOnEmptyQueue(int globalBatch, boolean setGlobal,
                                                                     int receiverBatch, boolean setReceiver,
                                                                     int expected) throws Exception {

      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");
         receiveFromQueue.addProperty(RECEIVER_CREDITS, 0);
         if (setReceiver) {
            receiveFromQueue.addProperty(PULL_RECEIVER_BATCH_SIZE, receiverBatch);
         }

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);
         if (setGlobal) {
            element.addProperty(PULL_RECEIVER_BATCH_SIZE, globalBatch);
         }

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(QueueConfiguration.of("test").setRoutingType(RoutingType.ANYCAST)
                                                         .setAddress("test")
                                                         .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("test::test").also()
                            .withSource().withAddress("test")
                                         .withFilter(nullValue()).also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();
         peer.expectFlow().withLinkCredit(expected);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            peer.close();
         }
      }
   }

   @Test
   @Timeout(30)
   public void testPullQueueConsumerGrantsCreditOnlyWhenPendingMessageIsConsumed() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(
               getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort() + "?amqpCredits=0");
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("test::test").also()
                            .withSource().withAddress("test")
                                         .withFilter(nullValue()).also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();

         // Must be pull consumer to ensure we can check that demand on remote doesn't
         // offer credit until the pending message count on the Queue is zeroed
         final ConnectionFactory factory = CFUtil.createConnectionFactory(
            "AMQP", "tcp://localhost:" + AMQP_PORT + "?jms.prefetchPolicy.all=0");

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final Queue queue = session.createQueue("test");
            final MessageProducer producer = session.createProducer(queue);

            // Add to backlog
            producer.send(session.createMessage());

            // Now create demand on the queue
            final MessageConsumer consumer = session.createConsumer(queue);

            connection.start();
            producer.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectFlow().withLinkCredit(DEFAULT_PULL_CREDIT_BATCH_SIZE);

            // Remove the backlog and credit should be offered to the remote
            assertNotNull(consumer.receiveNoWait());

            peer.waitForScriptToComplete(20, TimeUnit.SECONDS);

            peer.close();
         }
      }
   }

   @Test
   @Timeout(30)
   public void testPullQueueConsumerBatchCreditTopUpAfterEachBacklogDrain() throws Exception {
      doTestPullConsumerCreditTopUpAfterEachBacklogDrain(0, false, 0, false, DEFAULT_PULL_CREDIT_BATCH_SIZE);
   }

   @Test
   @Timeout(30)
   public void testPullQueueConsumerBatchCreditTopUpAfterEachBacklogDrainFederationConfigured() throws Exception {
      doTestPullConsumerCreditTopUpAfterEachBacklogDrain(10, true, 0, false, 10);
   }

   @Test
   @Timeout(30)
   public void testPullQueueConsumerBatchCreditTopUpAfterEachBacklogDrainPolicyConfigured() throws Exception {
      doTestPullConsumerCreditTopUpAfterEachBacklogDrain(0, false, 20, true, 20);
   }

   @Test
   @Timeout(30)
   public void testPullQueueConsumerBatchCreditTopUpAfterEachBacklogDrainBothConfigured() throws Exception {
      doTestPullConsumerCreditTopUpAfterEachBacklogDrain(100, true, 20, true, 20);
   }

   private void doTestPullConsumerCreditTopUpAfterEachBacklogDrain(int globalBatch, boolean setGlobal,
                                                                   int receiverBatch, boolean setReceiver,
                                                                   int expected) throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final SimpleString queueName = SimpleString.of("test");

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");
         if (setReceiver) {
            receiveFromQueue.addProperty(PULL_RECEIVER_BATCH_SIZE, receiverBatch);
         }

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);
         if (setGlobal) {
            element.addProperty(PULL_RECEIVER_BATCH_SIZE, globalBatch);
         }

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(
               getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort() + "?amqpCredits=0");
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("test::test").also()
                            .withSource().withAddress("test")
                                         .withFilter(nullValue()).also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();

         // Must be pull consumer to ensure we can check that demand on remote doesn't
         // offer credit until the pending message count on the Queue is zeroed
         final ConnectionFactory factory = CFUtil.createConnectionFactory(
            "AMQP", "tcp://localhost:" + AMQP_PORT + "?jms.prefetchPolicy.all=0");

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final Queue queue = session.createQueue("test");
            final MessageProducer producer = session.createProducer(queue);

            // Add to backlog
            producer.send(session.createMessage());

            // Now create demand on the queue
            final MessageConsumer consumer = session.createConsumer(queue);

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectFlow().withLinkCredit(expected);

            // Remove the backlog and credit should be offered to the remote
            assertNotNull(consumer.receiveNoWait());

            peer.waitForScriptToComplete(20, TimeUnit.SECONDS);

            // Consume all the credit that was presented in the batch
            for (int i = 0; i < expected; ++i) {
               peer.expectDisposition().withState().accepted();
               peer.remoteTransfer().withBody().withString("test-message")
                                    .also()
                                    .withDeliveryId(i)
                                    .now();
            }

            Wait.assertTrue(() -> server.queueQuery(queueName).getMessageCount() == expected, 10_000);

            // Consume all the newly received message from the remote except one
            // which should leave the queue with a pending message so no credit
            // should be offered.
            for (int i = 0; i < expected - 1; ++i) {
               assertNotNull(consumer.receiveNoWait());
            }

            // We should not get a new batch yet as there is still one pending
            // message on the local queue we have not consumed.
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectFlow().withLinkCredit(expected);

            // Remove the backlog and credit should be offered to the remote again
            assertNotNull(consumer.receiveNoWait());

            peer.waitForScriptToComplete(20, TimeUnit.SECONDS);
            peer.expectDetach().respond();

            consumer.close(); // Remove local demand and federation consumer is torn down.

            peer.waitForScriptToComplete(20, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }

   @Test
   @Timeout(20)
   public void testBridgeCreatesQueueReceiverLinkForQueueAfterBrokerConnectionStarted() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.setAutostart(false);
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(QueueConfiguration.of("test").setRoutingType(RoutingType.ANYCAST)
                                                         .setAddress("test")
                                                         .setAutoCreated(false));

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         final Connection connection = factory.createConnection();
         final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
         final Queue queue = session.createQueue("test");

         session.createConsumer(queue);
         session.createConsumer(queue);
         session.createConsumer(queue);

         connection.start();

         // Should be no interactions yet, check to be sure
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         server.getBrokerConnections().forEach(c -> {
            try {
               c.start();
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         });

         // Add another demand consumer immediately after start to add more events
         // on the federation plugin for broker events.
         session.createConsumer(queue);

         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("test::test").also()
                            .withSource().withAddress("test")
                                         .withFilter(nullValue()).also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();
         peer.expectFlow().withLinkCredit(1000);
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectDetach().respond();

         // Add more demand after the broker connection starts
         session.createConsumer(queue);
         session.createConsumer(queue);

         // This should remove all demand on the queue and federation should stop
         connection.close();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test
   @Timeout(20)
   public void testBridgeQueueDemandTrackedWhenRemoteRejectsInitialAttempts() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.createQueue(QueueConfiguration.of("test").setRoutingType(RoutingType.ANYCAST)
                                                         .setAddress("test")
                                                         .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);
         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final Queue queue = session.createQueue("test");

            connection.start();

            // First consumer we reject the bridge attempt
            peer.expectAttach().ofReceiver()
                               .withTarget().withAddress("test::test").also()
                               .withSource().withAddress("test").also()
                               .respond()
                               .withNullSource();
            peer.expectFlow().withLinkCredit(1000);
            peer.remoteDetach().withErrorCondition("amqp:not-found", "the requested queue was not found").queue().afterDelay(10);
            peer.expectDetach();

            final MessageConsumer consumer1 = session.createConsumer(queue);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // Second consumer we reject the bridge attempt
            peer.expectAttach().ofReceiver()
                               .withTarget().withAddress("test::test").also()
                               .withSource().withAddress("test").also()
                               .respond()
                               .withNullSource();
            peer.expectFlow().withLinkCredit(1000);
            peer.remoteDetach().withErrorCondition("amqp:not-found", "the requested queue was not found").queue().afterDelay(10);
            peer.expectDetach();

            final MessageConsumer consumer2 = session.createConsumer(queue);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // Third consumer we accept the federation attempt
            peer.expectAttach().ofReceiver()
                               .withTarget().withAddress("test::test").also()
                               .withSource().withAddress("test").also()
                               .respond();
            peer.expectFlow().withLinkCredit(1000);

            final MessageConsumer consumer3 = session.createConsumer(queue);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // Demand should remain
            consumer3.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // Demand should remain
            consumer2.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();

            // Demand should be gone now
            consumer1.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }

   @Test
   @Timeout(20)
   public void testBridgeQueueReceiverCarriesConfiguredPrioirty() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");
         receiveFromQueue.setPriority(10);

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.addAddressInfo(new AddressInfo(SimpleString.of("test"), RoutingType.ANYCAST));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("test::test").also()
                            .withSource().withAddress("test").also()
                            .withProperty(BRIDGE_RECEIVER_PRIORITY.toString(), 10)
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();
         peer.expectFlow().withLinkCredit(1000);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }

   @Test
   @Timeout(20)
   public void testBridgeQueueReceiverOmitsConfiguredPrioirtyWhenPrioritiesDisabled() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");
         receiveFromQueue.setPriority(10); // Should be ignored regardless of being manually set.
         receiveFromQueue.addProperty(DISABLE_RECEIVER_PRIORITY, "true");

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.addAddressInfo(new AddressInfo(SimpleString.of("test"), RoutingType.ANYCAST));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("test::test").also()
                            .withSource().withAddress("test").also()
                            .withProperties(not(Matchers.hasEntry(BRIDGE_RECEIVER_PRIORITY.toString(), 10)))
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();
         peer.expectFlow().withLinkCredit(1000);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }

   @Test
   @Timeout(20)
   public void testBridgeQueueReceiverAppliesConfiguredRemoteAddressPrefix() throws Exception {
      doTestBridgeReceiverAppliesConfiguredRemoteAddressCustomizations("topic://", null, null);
   }

   @Test
   @Timeout(20)
   public void testBridgeQueueReceiverAppliesConfiguredRemoteAddressSuffix() throws Exception {
      doTestBridgeReceiverAppliesConfiguredRemoteAddressCustomizations(null, null, "?consumer-priority=1");
   }

   @Test
   @Timeout(20)
   public void testBridgeQueueReceiverAppliesConfiguredRemoteAddress() throws Exception {
      doTestBridgeReceiverAppliesConfiguredRemoteAddressCustomizations(null, "alternate", null);
   }

   @Test
   @Timeout(20)
   public void testBridgeQueueReceiverAppliesConfiguredRemoteAddressCustomizations() throws Exception {
      doTestBridgeReceiverAppliesConfiguredRemoteAddressCustomizations("topic://", "alternate", "?consumer-priority=1");
   }

   private void doTestBridgeReceiverAppliesConfiguredRemoteAddressCustomizations(String prefix, String address, String suffix) throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("address-policy");
         receiveFromQueue.addToIncludes("test", "test");
         receiveFromQueue.setRemoteAddress(address);
         receiveFromQueue.setRemoteAddressPrefix(prefix);
         receiveFromQueue.setRemoteAddressSuffix(suffix);

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.addAddressInfo(new AddressInfo(SimpleString.of("test"), RoutingType.ANYCAST));

         final String expectedSourceAddress = (prefix != null ? prefix : "") +
                                              (address != null ? address : "test") +
                                              (suffix != null ? suffix : "");

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("test::test").also()
                            .withSource().withAddress(expectedSourceAddress).also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();
         peer.expectFlow().withLinkCredit(1000);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }

   @Test
   @Timeout(20)
   public void testBridgeQueueReceiverAddsRemoteTerminusCapabilitiesConfigured() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");
         receiveFromQueue.setRemoteTerminusCapabilities(new String[] {"topic", "another"});

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.addAddressInfo(new AddressInfo(SimpleString.of("test"), RoutingType.ANYCAST));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("test::test").also()
                            .withSource().withAddress("test")
                                         .withCapabilities("topic", "another")
                                         .also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();
         peer.expectFlow().withLinkCredit(1000);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }

   @Test
   @Timeout(20)
   public void testBridgeAddressReceiverSetsSenderPresettledWhenConfigured() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Connect test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");
         receiveFromQueue.addProperty(PRESETTLE_SEND_MODE, "true");

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.addAddressInfo(new AddressInfo(SimpleString.of("test"), RoutingType.ANYCAST));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withSenderSettleModeSettled()
                            .withTarget().withAddress("test::test").also()
                            .withSource().withAddress("test").also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();
         peer.expectFlow().withLinkCredit(1000);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }

   @Test
   @Timeout(20)
   public void testBridgeStartedTriggersRemoteDemandWithExistingQueuesWithoutConsumersWhenTrackingDisabled() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");
         receiveFromQueue.addProperty(DISABLE_RECEIVER_DEMAND_TRACKING, "true");

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.setAutostart(false);
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();

         server.createQueue(QueueConfiguration.of("test").setRoutingType(RoutingType.ANYCAST)
                                                         .setAddress("test")
                                                         .setAutoCreated(false));

         // Other non-matching queues that also get scanned on start
         server.createQueue(QueueConfiguration.of("other").setRoutingType(RoutingType.ANYCAST)
                                                          .setAddress("other")
                                                          .setAutoCreated(false));
         server.createQueue(QueueConfiguration.of("another").setRoutingType(RoutingType.ANYCAST)
                                                            .setAddress("another")
                                                            .setAutoCreated(false));

         // Should be no interactions at this point, check to make sure.
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("test::test").also()
                            .withSource().withAddress("test").also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();
         peer.expectFlow().withLinkCredit(1000);

         // Starting the broker connection should trigger bridge of address with demand.
         server.getBrokerConnections().forEach(c -> {
            try {
               c.start();
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         });

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectDetach().respond();

         // This should trigger the bridge consumer to be shutdown as the target Queue is now gone.
         logger.info("Removing Queues to eliminate demand");
         server.destroyQueue(SimpleString.of("test"), null, true);
         Wait.assertFalse(() -> server.queueQuery(SimpleString.of("test")).isExists());

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectClose();
         peer.remoteClose().now();
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test
   @Timeout(20)
   public void testBridgeTriggersRemoteDemandAfterQueueCreatedWhenTrackingDisabled() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");
         receiveFromQueue.addProperty(DISABLE_RECEIVER_DEMAND_TRACKING, "true");

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();

         // Other non-matching queues that also get scanned on start
         server.createQueue(QueueConfiguration.of("other").setRoutingType(RoutingType.ANYCAST)
                                                          .setAddress("other")
                                                          .setAutoCreated(false));
         server.createQueue(QueueConfiguration.of("another").setRoutingType(RoutingType.ANYCAST)
                                                            .setAddress("another")
                                                            .setAutoCreated(false));

         // Should be no interactions at this point, check to make sure.
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("test::test").also()
                            .withSource().withAddress("test").also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();
         peer.expectFlow().withLinkCredit(1000);

         server.createQueue(QueueConfiguration.of("test").setRoutingType(RoutingType.ANYCAST)
                                                         .setAddress("test")
                                                         .setAutoCreated(false));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectDetach().respond();

         // This should trigger the bridge consumer to be shutdown as the target Queue is now gone.
         logger.info("Removing Queues to eliminate demand");
         server.destroyQueue(SimpleString.of("test"), null, true);
         Wait.assertFalse(() -> server.queueQuery(SimpleString.of("test")).isExists());

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectClose();
         peer.remoteClose().now();
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();
      }
   }

   @Test
   @Timeout(20)
   public void testBridgeRetriesReceiverAttachOnNewDemandAfterFirstReceiverForceDetached() throws Exception {
      doTestBridgeRetriesReceiverAttachOnNewDemandAfterFirstReceiverDetached(LinkError.DETACH_FORCED);
   }

   @Test
   @Timeout(20)
   public void testBridgeRetriesReceiverAttachOnNewDemandAfterFirstReceiverResourceDeleted() throws Exception {
      doTestBridgeRetriesReceiverAttachOnNewDemandAfterFirstReceiverDetached(AmqpError.RESOURCE_DELETED);
   }

   private void doTestBridgeRetriesReceiverAttachOnNewDemandAfterFirstReceiverDetached(Symbol condition) throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.addAddressInfo(new AddressInfo(SimpleString.of("test"), RoutingType.ANYCAST));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("test::test").also()
                            .withSource().withAddress("test").also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond();
         peer.expectFlow().withLinkCredit(1000);

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach();
            peer.remoteDetach().withErrorCondition(condition.toString(), "Forced Detach").now();

            // Retry after another consumer added.
            peer.expectAttach().ofReceiver()
                               .withTarget().withAddress("test::test").also()
                               .withSource().withAddress("test").also()
                               .withName(allOf(containsString(getTestName()),
                                               containsString("test"),
                                               containsString("queue-receiver"),
                                               containsString("amqp-bridge"),
                                               containsString(server.getNodeID().toString())))
                               .respond();
            peer.expectFlow().withLinkCredit(1000);

            session.createConsumer(session.createQueue("test")); // New demand.

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }

   @Test
   @Timeout(20)
   public void testBridgeRetriesReceiverAttachOnNewDemandAfterFirstReceiverResourceNotFound() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.addAddressInfo(new AddressInfo(SimpleString.of("test"), RoutingType.ANYCAST));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("test::test").also()
                            .withSource().withAddress("test").also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond()
                            .withNullSource();
         peer.remoteDetach().withClosed(true)
                            .withErrorCondition(AmqpError.NOT_FOUND.toString(), "Resource Not Found").queue();
         peer.expectFlow().optional();
         peer.expectDetach();

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // Retry after another consumer added.
            peer.expectAttach().ofReceiver()
                               .withTarget().withAddress("test::test").also()
                               .withSource().withAddress("test").also()
                               .withName(allOf(containsString(getTestName()),
                                               containsString("test"),
                                               containsString("queue-receiver"),
                                               containsString("amqp-bridge"),
                                               containsString(server.getNodeID().toString())))
                               .respond();
            peer.expectFlow().withLinkCredit(1000);

            session.createConsumer(session.createQueue("test")); // New demand.

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }

   @Test
   @Timeout(20)
   public void testBridgeRecoversLinkAfterFirstReceiverFailsWithResourceNotFound() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");
         receiveFromQueue.addProperty(LINK_RECOVERY_INITIAL_DELAY, 10); // 10 millisecond initial recovery delay
         receiveFromQueue.addProperty(LINK_RECOVERY_DELAY, 10);         // 10 millisecond continued recovery delay

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.addAddressInfo(new AddressInfo(SimpleString.of("test"), RoutingType.ANYCAST));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("test::test").also()
                            .withSource().withAddress("test").also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond()
                            .withNullSource();
         peer.remoteDetach().withClosed(true)
                            .withErrorCondition(AmqpError.NOT_FOUND.toString(), "Resource Not Found").queue();
         peer.expectFlow().optional();
         peer.expectDetach();

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // Retry after delay.
            peer.expectAttach().ofReceiver()
                               .withTarget().withAddress("test::test").also()
                               .withSource().withAddress("test").also()
                               .withName(allOf(containsString(getTestName()),
                                               containsString("test"),
                                               containsString("queue-receiver"),
                                               containsString("amqp-bridge"),
                                               containsString(server.getNodeID().toString())))
                               .respond();
            peer.expectFlow().withLinkCredit(1000);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }

   @Test
   @Timeout(20)
   public void testBridgeAttemptsLimitedRecoveryLinkAfterFirstReceiverFailsWithResourceNotFound() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");
         receiveFromQueue.addToIncludes("another", "another");
         receiveFromQueue.addProperty(LINK_RECOVERY_INITIAL_DELAY, 10); // 10 millisecond initial recovery delay
         receiveFromQueue.addProperty(LINK_RECOVERY_DELAY, 10);         // 10 millisecond continued recovery delay
         receiveFromQueue.addProperty(MAX_LINK_RECOVERY_ATTEMPTS, 2);   // 2 attempts then stop trying

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.addAddressInfo(new AddressInfo(SimpleString.of("test"), RoutingType.ANYCAST));
         server.addAddressInfo(new AddressInfo(SimpleString.of("another"), RoutingType.ANYCAST));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("test::test").also()
                            .withSource().withAddress("test").also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond()
                            .withNullSource();
         peer.remoteDetach().withClosed(true)
                            .withErrorCondition(AmqpError.NOT_FOUND.toString(), "Resource Not Found").queue();
         peer.expectFlow().optional();
         peer.expectDetach();

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // Attempt #1
            peer.expectAttach().ofReceiver()
                               .withTarget().withAddress("test::test").also()
                               .withSource().withAddress("test").also()
                               .respond()
                               .withNullSource();
            peer.remoteDetach().withClosed(true)
                               .withErrorCondition(AmqpError.NOT_FOUND.toString(), "Resource Not Found").queue();
            peer.expectFlow().optional();
            peer.expectDetach();

            // Attempt #2
            peer.expectAttach().ofReceiver()
                               .withTarget().withAddress("test::test").also()
                               .withSource().withAddress("test").also()
                               .respond()
                               .withNullSource();
            peer.remoteDetach().withClosed(true)
                               .withErrorCondition(AmqpError.NOT_FOUND.toString(), "Resource Not Found").queue();
            peer.expectFlow().optional();
            peer.expectDetach();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // Retry after new consumer added.
            peer.expectAttach().ofReceiver()
                               .withTarget().withAddress("another::another").also()
                               .withSource().withAddress("another").also()
                               .withName(allOf(containsString(getTestName()),
                                               containsString("another"),
                                               containsString("queue-receiver"),
                                               containsString("amqp-bridge"),
                                               containsString(server.getNodeID().toString())))
                               .respond();
            peer.expectFlow().withLinkCredit(1000);

            // Trigger demand on the alternate to which will start an attach on that
            session.createConsumer(session.createQueue("another"));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // Retry after new consumer added.
            peer.expectAttach().ofReceiver()
                               .withTarget().withAddress("test::test").also()
                               .withSource().withAddress("test").also()
                               .withName(allOf(containsString(getTestName()),
                                               containsString("test"),
                                               containsString("queue-receiver"),
                                               containsString("amqp-bridge"),
                                               containsString(server.getNodeID().toString())))
                               .respond();
            peer.expectFlow().withLinkCredit(1000);

            // Now add new demand and trigger another attempt to create the bridge receiver
            session.createConsumer(session.createQueue("test"));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }

   @Test
   @Timeout(20)
   public void testBridgeDoesNotAttemptLimitedRecoveryAfterFirstReceiverFailsWithResourceNotFound() throws Exception {
      try (ProtonTestServer peer = new ProtonTestServer()) {
         peer.expectSASLAnonymousConnect();
         peer.expectOpen().respond();
         peer.expectBegin().respond();
         peer.start();

         final URI remoteURI = peer.getServerURI();
         logger.info("Test started, peer listening on: {}", remoteURI);

         final AMQPBridgeQueuePolicyElement receiveFromQueue = new AMQPBridgeQueuePolicyElement();
         receiveFromQueue.setName("queue-policy");
         receiveFromQueue.addToIncludes("test", "test");
         receiveFromQueue.addToIncludes("another", "another");
         receiveFromQueue.addProperty(LINK_RECOVERY_INITIAL_DELAY, 1);  // 1 millisecond initial recovery delay
         receiveFromQueue.addProperty(LINK_RECOVERY_DELAY, 10);         // 10 millisecond continued recovery delay
         receiveFromQueue.addProperty(MAX_LINK_RECOVERY_ATTEMPTS, 0);   // No attempts

         final AMQPBridgeBrokerConnectionElement element = new AMQPBridgeBrokerConnectionElement();
         element.setName(getTestName());
         element.addBridgeFromQueuePolicy(receiveFromQueue);

         final AMQPBrokerConnectConfiguration amqpConnection =
            new AMQPBrokerConnectConfiguration(getTestName(), "tcp://" + remoteURI.getHost() + ":" + remoteURI.getPort());
         amqpConnection.setReconnectAttempts(0);// No reconnects
         amqpConnection.addElement(element);

         server.getConfiguration().addAMQPConnection(amqpConnection);
         server.start();
         server.addAddressInfo(new AddressInfo(SimpleString.of("test"), RoutingType.ANYCAST));
         server.addAddressInfo(new AddressInfo(SimpleString.of("another"), RoutingType.ANYCAST));

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofReceiver()
                            .withTarget().withAddress("test::test").also()
                            .withSource().withAddress("test").also()
                            .withName(allOf(containsString(getTestName()),
                                            containsString("test"),
                                            containsString("queue-receiver"),
                                            containsString("amqp-bridge"),
                                            containsString(server.getNodeID().toString())))
                            .respond()
                            .withNullSource();
         peer.remoteDetach().withClosed(true)
                            .withErrorCondition(AmqpError.NOT_FOUND.toString(), "Resource Not Found").queue();
         peer.expectFlow().optional();
         peer.expectDetach();

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue("test"));

            connection.start();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // Try after new consumer added to alternate queue.
            peer.expectAttach().ofReceiver()
                               .withTarget().withAddress("another::another").also()
                               .withSource().withAddress("another").also()
                               .withName(allOf(containsString(getTestName()),
                                               containsString("another"),
                                               containsString("queue-receiver"),
                                               containsString("amqp-bridge"),
                                               containsString(server.getNodeID().toString())))
                               .respond();
            peer.expectFlow().withLinkCredit(1000);

            // Add demand on alternate Queue which triggers a receiver attach.
            session.createConsumer(session.createQueue("another"));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // Retry after new consumer added.
            peer.expectAttach().ofReceiver()
                               .withTarget().withAddress("test::test").also()
                               .withSource().withAddress("test").also()
                               .withName(allOf(containsString(getTestName()),
                                               containsString("test"),
                                               containsString("queue-receiver"),
                                               containsString("amqp-bridge"),
                                               containsString(server.getNodeID().toString())))
                               .respond();
            peer.expectFlow().withLinkCredit(1000);

            // Now add new demand on Queue and it should trigger another attach attempt
            session.createConsumer(session.createQueue("test"));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();
         }
      }
   }
}
