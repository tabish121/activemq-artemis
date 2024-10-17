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
package org.apache.activemq.artemis.tests.integration.plugin;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQIOErrorException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerPlugin;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Random;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BeforeSendPluginTest extends JMSTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static final int BROKER_PLUGIN_MESSAGE_SIZE_LIMIT = 10 * 1024;

   private static final int MIN_LARGE_MESSAGE_SIZE = 16384;

   private static final int LARGE_MESSAGE_SIZE = MIN_LARGE_MESSAGE_SIZE + 1000;
   private static final int NON_LARGE_MESSAGE_SIZE = MIN_LARGE_MESSAGE_SIZE - 1000;

   @Override
   protected Configuration createDefaultConfig(boolean netty) throws Exception {
      Configuration config = super.createDefaultConfig(netty);
      config.getAcceptorConfigurations().clear();

      HashMap<String, Object> params = new HashMap<>();
      params.put(TransportConstants.PORT_PROP_NAME, String.valueOf(61616));
      params.put(TransportConstants.PROTOCOLS_PROP_NAME, "CORE,AMQP");
      params.put("amqpMinLargeMessageSize", MIN_LARGE_MESSAGE_SIZE);

      TransportConfiguration transportConfiguration = new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params, "netty-acceptor", new HashMap<>());

      config.addAcceptorConfiguration(transportConfiguration);

      config.registerBrokerPlugin(new BeforeSendPlugin(BROKER_PLUGIN_MESSAGE_SIZE_LIMIT));

      return config;
   }

   @Override
   @BeforeEach
   public void setUp() throws Exception {
      super.setUp();
   }

   @Override
   public boolean usePersistence() {
      return true;
   }

   @Test
   public void testCORESend() throws Exception {
      ConnectionFactory connectionFactory = CFUtil.createConnectionFactory("CORE", "tcp://localhost:61616");
      internalTestSendReceive(connectionFactory, NON_LARGE_MESSAGE_SIZE, false);
   }

   @Test
   public void testCORESendLarge() throws Exception {
      ConnectionFactory connectionFactory = CFUtil.createConnectionFactory("CORE", "tcp://localhost:61616");
      internalTestSendReceive(connectionFactory, LARGE_MESSAGE_SIZE, false);
   }

   @Test
   public void testCORESendTransactional() throws Exception {
      ConnectionFactory connectionFactory = CFUtil.createConnectionFactory("CORE", "tcp://localhost:61616");
      internalTestSendReceive(connectionFactory, NON_LARGE_MESSAGE_SIZE, true);
   }

   @Test
   public void testCORESendLargeTransactional() throws Exception {
      ConnectionFactory connectionFactory = CFUtil.createConnectionFactory("CORE", "tcp://localhost:61616");
      internalTestSendReceive(connectionFactory, LARGE_MESSAGE_SIZE, true);
   }

   @Test
   public void testAMQPSend() throws Exception {
      ConnectionFactory connectionFactory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:61616");
      internalTestSendReceive(connectionFactory, NON_LARGE_MESSAGE_SIZE, false);
   }

   @Test
   public void testAMQPSendLarge() throws Exception {
      ConnectionFactory connectionFactory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:61616");
      internalTestSendReceive(connectionFactory, LARGE_MESSAGE_SIZE, false);
   }

   @Test
   public void testAMQPSendTransactional() throws Exception {
      ConnectionFactory connectionFactory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:61616");
      internalTestSendReceive(connectionFactory, NON_LARGE_MESSAGE_SIZE, true);
   }

   @Test
   public void testAMQPSendLargeTransactional() throws Exception {
      ConnectionFactory connectionFactory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:61616");
      internalTestSendReceive(connectionFactory, LARGE_MESSAGE_SIZE, true);
   }

   private void internalTestSendReceive(ConnectionFactory connectionFactory, int messageSize, boolean transacted) throws Exception {
      server.createQueue(QueueConfiguration.of(getTestMethodName()).setAddress(getTestMethodName()).setRoutingType(RoutingType.ANYCAST));

      try (Connection connection = connectionFactory.createConnection()) {
         connection.start();

         final Session session;

         if (transacted) {
            session = connection.createSession(true, Session.SESSION_TRANSACTED);
         } else {
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         }

         final Queue queue = session.createQueue(getTestMethodName());
         final MessageProducer producer = session.createProducer(queue);
         final MessageConsumer consumer = session.createConsumer(queue);

         final byte[] payload = new byte[messageSize];
         final Random random = new Random(System.currentTimeMillis());

         random.nextBytes(payload);

         boolean hadExceptionOnSend = false;

         final TextMessage sentMessage = session.createTextMessage(new String(payload));

         try {
            producer.send(sentMessage);
         } catch (Exception e) {
            logger.debug("Caught exception on producer send: ", e);
            hadExceptionOnSend = true;
         }

         if (!transacted) {
            assertTrue(hadExceptionOnSend);
         }

         final TextMessage messageReceived = (TextMessage) consumer.receive(1000);

         assertNull(messageReceived);

         if (transacted) {
            boolean hadExceptionOnCommit = false;
            try {
               session.commit();
            } catch (Exception e) {
               logger.debug("Caught exception on commit transaction: ", e);
               hadExceptionOnCommit = true;
            }

            assertTrue(hadExceptionOnCommit);
         }

         connection.close();
      }

      validateNoFilesOnLargeDir();
   }

   private class BeforeSendPlugin implements ActiveMQServerPlugin {

      private final long maxMessageSize;

      BeforeSendPlugin(long maxMessageSize) {
         this.maxMessageSize = maxMessageSize;
      }

      @Override
      public void beforeSend(ServerSession session, Transaction tx, Message message, boolean direct, boolean noAutoCreateQueue) throws ActiveMQException {
         final long messageSize = message.getWholeMessageSize();

         if (messageSize > maxMessageSize) {
            throw new ActiveMQIOErrorException("Message of size " + messageSize + " exceeded size limit of " + maxMessageSize);
         }
      }
   }
}
