/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.retention;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
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
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.tests.util.RandomUtil;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.jgroups.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ReplayTest extends ActiveMQTestBase {

   protected ActiveMQServer server;

   private String queueName1;
   private String queueName2;

   @BeforeEach
   @Override
   public void setUp() throws Exception {
      super.setUp();

      queueName1 = getName() + "-1";
      queueName2 = getName() + "-2";

      server = addServer(createServer(true, true));
      server.getConfiguration().setJournalRetentionDirectory(getJournalDir() + "retention");
      server.getConfiguration().setJournalFileSize(100 * 1024);

      server.start();

      server.addAddressInfo(new AddressInfo(queueName1).addRoutingType(RoutingType.ANYCAST));
      server.createQueue(QueueConfiguration.of(queueName1).setAddress(queueName1).setRoutingType(RoutingType.ANYCAST));

      server.addAddressInfo(new AddressInfo(queueName2).addRoutingType(RoutingType.ANYCAST));
      server.createQueue(QueueConfiguration.of(queueName2).setAddress(queueName2).setRoutingType(RoutingType.ANYCAST));
   }

   @Test
   public void testReplayAMQP() throws Exception {
      testReplay("AMQP", 10, false);
   }

   @Test
   public void testReplayCore() throws Exception {
      testReplay("CORE", 10, false);
   }

   @Test
   public void testReplayLargeAMQP() throws Exception {
      testReplay("AMQP", 500 * 1024, false);
   }

   @Test
   public void testReplayLargeCore() throws Exception {
      testReplay("CORE", 500 * 1024, false);
   }

   @Test
   public void testReplayCorePaging() throws Exception {
      testReplay("CORE", 10, true);
   }

   @Test
   public void testReplayLargeCorePaging() throws Exception {
      testReplay("CORE", 500 * 1024, true);
   }

   protected void testReplay(String protocol, int size, boolean paging) throws Exception {
      StringBuffer buffer = new StringBuffer();
      buffer.append(RandomUtil.randomString());
      for (int i = 0; i < size; i++) {
         buffer.append("*");
      }

      if (paging) {
         org.apache.activemq.artemis.core.server.Queue serverQueue = server.locateQueue(queueName1);
         serverQueue.getPagingStore().startPaging();
      }

      ConnectionFactory cf = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");

      try (Connection connection = cf.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Queue queue = session.createQueue(queueName1);

         MessageProducer producer = session.createProducer(null);

         producer.send(queue, session.createTextMessage(buffer.toString()));

         connection.start();

         MessageConsumer consumer = session.createConsumer(queue);

         assertNotNull(consumer.receive(5000));

         assertNull(consumer.receiveNoWait());

         server.replay(null, null, queueName1, queueName2, null);

         Queue t2 = session.createQueue(queueName2);

         MessageConsumer consumert2 = session.createConsumer(t2);

         TextMessage receivedMessage = (TextMessage) consumert2.receive(5000);

         assertNotNull(receivedMessage);

         assertEquals(buffer.toString(), receivedMessage.getText());

         assertNull(consumert2.receiveNoWait());

         server.replay(null, null, queueName2, queueName1, null);

         receivedMessage = (TextMessage) consumer.receive(5000);

         assertNotNull(receivedMessage);

         assertNull(consumer.receiveNoWait());

         // invalid filter.. nothing should be re played
         server.replay(null, null, queueName1, queueName1, "foo='foo'");

         assertNull(consumer.receiveNoWait());
      }
   }

   @Test
   public void testSelectPriorityFromRetentionAMQP() throws Exception {
      doTestSelectPriorityFromRetention("AMQP", 10, false);
   }

   @Test
   public void testSelectPriorityFromRetentionAMQPAndPaging() throws Exception {
      doTestSelectPriorityFromRetention("AMQP", 10, true);
   }

   @Test
   public void testSelectPriorityFromRetentionCore() throws Exception {
      doTestSelectPriorityFromRetention("Core", 10, false);
   }

   @Test
   public void testSelectPriorityFromRetentionCoreAndPaging() throws Exception {
      doTestSelectPriorityFromRetention("Core", 10, true);
   }

   protected void doTestSelectPriorityFromRetention(String protocol, int size, boolean paging) throws Exception {
      final String payload = UUID.randomUUID().toString() + "*".repeat(size);

      if (paging) {
         org.apache.activemq.artemis.core.server.Queue serverQueue = server.locateQueue(queueName1);
         serverQueue.getPagingStore().startPaging();
      }

      final ConnectionFactory cf = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");

      final org.apache.activemq.artemis.core.server.Queue queue1View = server.locateQueue(queueName1);
      final org.apache.activemq.artemis.core.server.Queue queue2View = server.locateQueue(queueName2);

      final String message1Prefix = "message-1:";
      final String message2Prefix = "message-2:";

      try (Connection connection = cf.createConnection()) {
         final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         final Queue queue = session.createQueue(queueName1);
         final MessageProducer producer = session.createProducer(queue);
         final MessageConsumer consumer = session.createConsumer(queue);

         final TextMessage message1 = session.createTextMessage(message1Prefix + payload);
         final TextMessage message2 = session.createTextMessage(message2Prefix + payload);

         producer.setPriority(0);
         producer.send(message1);
         producer.setPriority(9);
         producer.send(message2);

         connection.start();

         final Message received1 = consumer.receive(5_000);
         final Message received2 = consumer.receive(5_000);

         assertNotNull(received1);
         assertNotNull(received2);
      }

      assertEquals(0, queue1View.getMessageCount());
      assertEquals(0, queue2View.getMessageCount());

      server.replay(null, null, queueName1, queueName2, "AMQPriority=9");

      assertEquals(0, queue1View.getMessageCount());
      Wait.assertEquals(1L, () -> queue2View.getMessageCount(), 2_000, 100);

      try (Connection connection = cf.createConnection()) {
         final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         final Queue queue = session.createQueue(queueName2);
         final MessageConsumer consumer = session.createConsumer(queue);

         connection.start();

         final TextMessage received = (TextMessage) consumer.receive(5_000);

         assertNotNull(received);
         assertTrue(((TextMessage) received).getText().startsWith(message2Prefix));
      }
   }

   @Test
   public void testSelectGroupIDFromRetentionAMQP() throws Exception {
      doTestSelectGroupIDFromRetention("AMQP", 10, false);
   }

   @Test
   public void testSelectGroupIDFromRetentionAMQPAndPaging() throws Exception {
      doTestSelectGroupIDFromRetention("AMQP", 10, true);
   }

   @Test
   public void testSelectGroupIDFromRetentionCore() throws Exception {
      doTestSelectGroupIDFromRetention("Core", 10, false);
   }

   @Test
   public void testSelectGroupIDFromRetentionCoreAndPaging() throws Exception {
      doTestSelectGroupIDFromRetention("Core", 10, true);
   }

   protected void doTestSelectGroupIDFromRetention(String protocol, int size, boolean paging) throws Exception {
      final String payload = UUID.randomUUID().toString() + "*".repeat(size);

      if (paging) {
         org.apache.activemq.artemis.core.server.Queue serverQueue = server.locateQueue(queueName1);
         serverQueue.getPagingStore().startPaging();
      }

      final ConnectionFactory cf = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");

      final org.apache.activemq.artemis.core.server.Queue queue1View = server.locateQueue(queueName1);
      final org.apache.activemq.artemis.core.server.Queue queue2View = server.locateQueue(queueName2);

      final String message1Prefix = "message-1:";
      final String message2Prefix = "message-2:";

      try (Connection connection = cf.createConnection()) {
         final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         final Queue queue = session.createQueue(queueName1);
         final MessageProducer producer = session.createProducer(queue);
         final MessageConsumer consumer = session.createConsumer(queue);

         final TextMessage message1 = session.createTextMessage(message1Prefix + payload);
         final TextMessage message2 = session.createTextMessage(message2Prefix + payload);

         message1.setStringProperty("JMSXGroupID", "one");
         message2.setStringProperty("JMSXGroupID", "two");

         producer.send(message1);
         producer.send(message2);

         connection.start();

         final Message received1 = consumer.receive(5_000);
         final Message received2 = consumer.receive(5_000);

         assertNotNull(received1);
         assertNotNull(received2);
      }

      assertEquals(0, queue1View.getMessageCount());
      assertEquals(0, queue2View.getMessageCount());

      server.replay(null, null, queueName1, queueName2, "AMQGroupID='two'");

      assertEquals(0, queue1View.getMessageCount());
      Wait.assertEquals(1L, () -> queue2View.getMessageCount(), 2_000, 100);

      try (Connection connection = cf.createConnection()) {
         final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         final Queue queue = session.createQueue(queueName2);
         final MessageConsumer consumer = session.createConsumer(queue);

         connection.start();

         final TextMessage received = (TextMessage) consumer.receive(5_000);

         assertNotNull(received);
         assertTrue(((TextMessage) received).getText().startsWith(message2Prefix));
      }
   }

   @Test
   public void testSelectCorrelationIDFromRetentionAMQP() throws Exception {
      doTestSelectCorrelationIDFromRetention("AMQP", 10, false);
   }

   @Test
   public void testSelectCorrelationIDFromRetentionAMQPAndPaging() throws Exception {
      doTestSelectCorrelationIDFromRetention("AMQP", 10, true);
   }

   @Test
   public void testSelectCorrelationIDFromRetentionCore() throws Exception {
      doTestSelectCorrelationIDFromRetention("Core", 10, false);
   }

   @Test
   public void testSelectCorrelationIDFromRetentionCoreAndPaging() throws Exception {
      doTestSelectCorrelationIDFromRetention("Core", 10, true);
   }

   protected void doTestSelectCorrelationIDFromRetention(String protocol, int size, boolean paging) throws Exception {
      final String payload = UUID.randomUUID().toString() + "*".repeat(size);

      if (paging) {
         org.apache.activemq.artemis.core.server.Queue serverQueue = server.locateQueue(queueName1);
         serverQueue.getPagingStore().startPaging();
      }

      final ConnectionFactory cf = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");

      final org.apache.activemq.artemis.core.server.Queue queue1View = server.locateQueue(queueName1);
      final org.apache.activemq.artemis.core.server.Queue queue2View = server.locateQueue(queueName2);

      final String message1Prefix = "message-1:";
      final String message2Prefix = "message-2:";

      try (Connection connection = cf.createConnection()) {
         final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         final Queue queue = session.createQueue(queueName1);
         final MessageProducer producer = session.createProducer(queue);
         final MessageConsumer consumer = session.createConsumer(queue);

         final TextMessage message1 = session.createTextMessage(message1Prefix + payload);
         final TextMessage message2 = session.createTextMessage(message2Prefix + payload);

         message1.setJMSCorrelationID(message1Prefix);
         message2.setJMSCorrelationID(message2Prefix);

         producer.send(message1);
         producer.send(message2);

         connection.start();

         final Message received1 = consumer.receive(5_000);
         final Message received2 = consumer.receive(5_000);

         assertNotNull(received1);
         assertNotNull(received2);
      }

      assertEquals(0, queue1View.getMessageCount());
      assertEquals(0, queue2View.getMessageCount());

      server.replay(null, null, queueName1, queueName2, "JMSCorrelationID LIKE '" + message2Prefix + "'");

      assertEquals(0, queue1View.getMessageCount());
      Wait.assertEquals(1L, () -> queue2View.getMessageCount(), 2_000, 100);

      try (Connection connection = cf.createConnection()) {
         final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         final Queue queue = session.createQueue(queueName2);
         final MessageConsumer consumer = session.createConsumer(queue);

         connection.start();

         final TextMessage received = (TextMessage) consumer.receive(5_000);

         assertNotNull(received);
         assertTrue(((TextMessage) received).getText().startsWith(message2Prefix));
      }
   }

   @Test
   public void testSelectIntPropertyFromRetentionAMQP() throws Exception {
      doTestSelectIntPropertyFromRetention("AMQP", 10, false);
   }

   @Test
   public void testSelectIntPropertyFromRetentionAMQPAndPaging() throws Exception {
      doTestSelectIntPropertyFromRetention("AMQP", 10, true);
   }

   @Test
   public void testSelectIntPropertyFromRetentionCore() throws Exception {
      doTestSelectIntPropertyFromRetention("Core", 10, false);
   }

   @Test
   public void testSelectIntPropertyFromRetentionCoreAndPaging() throws Exception {
      doTestSelectIntPropertyFromRetention("Core", 10, true);
   }

   protected void doTestSelectIntPropertyFromRetention(String protocol, int size, boolean paging) throws Exception {
      final String payload = UUID.randomUUID().toString() + "*".repeat(size);

      if (paging) {
         org.apache.activemq.artemis.core.server.Queue serverQueue = server.locateQueue(queueName1);
         serverQueue.getPagingStore().startPaging();
      }

      final ConnectionFactory cf = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");

      final org.apache.activemq.artemis.core.server.Queue queue1View = server.locateQueue(queueName1);
      final org.apache.activemq.artemis.core.server.Queue queue2View = server.locateQueue(queueName2);

      final String message1Prefix = "message-1:";
      final String message2Prefix = "message-2:";

      try (Connection connection = cf.createConnection()) {
         final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         final Queue queue = session.createQueue(queueName1);
         final MessageProducer producer = session.createProducer(queue);
         final MessageConsumer consumer = session.createConsumer(queue);

         final TextMessage message1 = session.createTextMessage(message1Prefix + payload);
         final TextMessage message2 = session.createTextMessage(message2Prefix + payload);

         message1.setIntProperty("property", 42);
         message2.setIntProperty("property", 24);

         producer.send(message1);
         producer.send(message2);

         connection.start();

         final Message received1 = consumer.receive(5_000);
         final Message received2 = consumer.receive(5_000);

         assertNotNull(received1);
         assertNotNull(received2);
      }

      assertEquals(0, queue1View.getMessageCount());
      assertEquals(0, queue2View.getMessageCount());

      server.replay(null, null, queueName1, queueName2, "property = 42");

      assertEquals(0, queue1View.getMessageCount());
      Wait.assertEquals(1L, () -> queue2View.getMessageCount(), 2_000, 100);

      try (Connection connection = cf.createConnection()) {
         final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         final Queue queue = session.createQueue(queueName2);
         final MessageConsumer consumer = session.createConsumer(queue);

         connection.start();

         final TextMessage received = (TextMessage) consumer.receive(5_000);

         assertNotNull(received);
         assertTrue(((TextMessage) received).getText().startsWith(message1Prefix));
      }
   }

   @Test
   public void testSelectMessageIDFromRetentionAMQP() throws Exception {
      doTestSelectMessageIDFromRetention("AMQP", 10, false);
   }

   @Test
   public void testSelectMessageIDFromRetentionAMQPAndPaging() throws Exception {
      doTestSelectMessageIDFromRetention("AMQP", 10, true);
   }

   @Test
   public void testSelectMessageIDFromRetentionCore() throws Exception {
      doTestSelectMessageIDFromRetention("Core", 10, false);
   }

   @Test
   public void testSelectMessageIDFromRetentionCoreAndPaging() throws Exception {
      doTestSelectMessageIDFromRetention("Core", 10, true);
   }

   protected void doTestSelectMessageIDFromRetention(String protocol, int size, boolean paging) throws Exception {
      final String payload = UUID.randomUUID().toString() + "*".repeat(size);

      if (paging) {
         org.apache.activemq.artemis.core.server.Queue serverQueue = server.locateQueue(queueName1);
         serverQueue.getPagingStore().startPaging();
      }

      final ConnectionFactory cf = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");

      final org.apache.activemq.artemis.core.server.Queue queue1View = server.locateQueue(queueName1);
      final org.apache.activemq.artemis.core.server.Queue queue2View = server.locateQueue(queueName2);

      final String message1Prefix = "message-1:";
      final String message2Prefix = "message-2:";

      String message2MessageID = null;

      try (Connection connection = cf.createConnection()) {
         final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         final Queue queue = session.createQueue(queueName1);
         final MessageProducer producer = session.createProducer(queue);
         final MessageConsumer consumer = session.createConsumer(queue);

         final TextMessage message1 = session.createTextMessage(message1Prefix + payload);
         final TextMessage message2 = session.createTextMessage(message2Prefix + payload);

         producer.send(message1);
         producer.send(message2);

         connection.start();

         final Message received1 = consumer.receive(5_000);
         final Message received2 = consumer.receive(5_000);

         assertNotNull(received1);
         assertNotNull(received2);

         message2MessageID = received2.getJMSMessageID();
      }

      assertEquals(0, queue1View.getMessageCount());
      assertEquals(0, queue2View.getMessageCount());

      server.replay(null, null, queueName1, queueName2, "AMQUserID='" + message2MessageID + "'");

      assertEquals(0, queue1View.getMessageCount());
      Wait.assertEquals(1L, () -> queue2View.getMessageCount(), 2_000, 100);

      try (Connection connection = cf.createConnection()) {
         final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         final Queue queue = session.createQueue(queueName2);
         final MessageConsumer consumer = session.createConsumer(queue);

         connection.start();

         final TextMessage received = (TextMessage) consumer.receive(5_000);

         assertNotNull(received);
         assertTrue(((TextMessage) received).getText().startsWith(message2Prefix));
      }
   }

   @Test
   public void testSelectTimestampFromRetentionAMQP() throws Exception {
      doTestSelectTimestampFromRetention("AMQP", 10, false);
   }

   @Test
   public void testSelectTimestampFromRetentionAMQPAndPaging() throws Exception {
      doTestSelectTimestampFromRetention("AMQP", 10, true);
   }

   @Test
   public void testSelectTimestampFromRetentionCore() throws Exception {
      doTestSelectTimestampFromRetention("Core", 10, false);
   }

   @Test
   public void testSelectTimestampFromRetentionCoreAndPaging() throws Exception {
      doTestSelectTimestampFromRetention("Core", 10, true);
   }

   protected void doTestSelectTimestampFromRetention(String protocol, int size, boolean paging) throws Exception {
      final String payload = UUID.randomUUID().toString() + "*".repeat(size);

      if (paging) {
         org.apache.activemq.artemis.core.server.Queue serverQueue = server.locateQueue(queueName1);
         serverQueue.getPagingStore().startPaging();
      }

      final ConnectionFactory cf = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");

      final org.apache.activemq.artemis.core.server.Queue queue1View = server.locateQueue(queueName1);
      final org.apache.activemq.artemis.core.server.Queue queue2View = server.locateQueue(queueName2);

      final String message1Prefix = "message-1:";
      final String message2Prefix = "message-2:";
      final String message3Prefix = "message-3:";

      long beforeProductionTime = Long.MAX_VALUE;
      long afterProductionTime = Long.MAX_VALUE;

      try (Connection connection = cf.createConnection()) {
         final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         final Queue queue = session.createQueue(queueName1);
         final MessageProducer producer = session.createProducer(queue);
         final MessageConsumer consumer = session.createConsumer(queue);

         final TextMessage message1 = session.createTextMessage(message1Prefix + payload);
         final TextMessage message2 = session.createTextMessage(message2Prefix + payload);
         final TextMessage message3 = session.createTextMessage(message3Prefix + payload);

         producer.send(message1);
         Thread.sleep(10);
         beforeProductionTime = System.currentTimeMillis() - 5;
         producer.send(message2);
         afterProductionTime = System.currentTimeMillis() + 1;
         Thread.sleep(10);
         producer.send(message3);

         connection.start();

         final Message received1 = consumer.receive(5_000);
         final Message received2 = consumer.receive(5_000);
         final Message received3 = consumer.receive(5_000);

         assertNotNull(received1);
         assertNotNull(received2);
         assertNotNull(received3);
      }

      assertEquals(0, queue1View.getMessageCount());
      assertEquals(0, queue2View.getMessageCount());

      server.replay(null, null, queueName1, queueName2,
         "AMQTimestamp > " + beforeProductionTime + " AND AMQTimestamp < " + afterProductionTime);

      assertEquals(0, queue1View.getMessageCount());
      Wait.assertEquals(1L, () -> queue2View.getMessageCount(), 2_000, 100);

      try (Connection connection = cf.createConnection()) {
         final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         final Queue queue = session.createQueue(queueName2);
         final MessageConsumer consumer = session.createConsumer(queue);

         connection.start();

         final TextMessage received = (TextMessage) consumer.receive(5_000);

         assertNotNull(received);
         assertTrue(((TextMessage) received).getText().startsWith(message2Prefix));
      }
   }

   @Test
   public void testSelectJMSTypeFromRetentionAMQP() throws Exception {
      doTestSelectJMSTypeFromRetention("AMQP", 10, false);
   }

   @Test
   public void testSelectJMSTypeFromRetentionAMQPAndPaging() throws Exception {
      doTestSelectJMSTypeFromRetention("AMQP", 10, true);
   }

   @Test
   public void testSelectJMSTypeFromRetentionCore() throws Exception {
      doTestSelectJMSTypeFromRetention("Core", 10, false);
   }

   @Test
   public void testSelectJMSTypeFromRetentionCoreAndPaging() throws Exception {
      doTestSelectJMSTypeFromRetention("Core", 10, true);
   }

   protected void doTestSelectJMSTypeFromRetention(String protocol, int size, boolean paging) throws Exception {
      final String payload = UUID.randomUUID().toString() + "*".repeat(size);

      if (paging) {
         org.apache.activemq.artemis.core.server.Queue serverQueue = server.locateQueue(queueName1);
         serverQueue.getPagingStore().startPaging();
      }

      final ConnectionFactory cf = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");

      final org.apache.activemq.artemis.core.server.Queue queue1View = server.locateQueue(queueName1);
      final org.apache.activemq.artemis.core.server.Queue queue2View = server.locateQueue(queueName2);

      final String message1Prefix = "message-1:";
      final String message2Prefix = "message-2:";

      try (Connection connection = cf.createConnection()) {
         final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         final Queue queue = session.createQueue(queueName1);
         final MessageProducer producer = session.createProducer(queue);
         final MessageConsumer consumer = session.createConsumer(queue);

         final TextMessage message1 = session.createTextMessage(message1Prefix + payload);
         final TextMessage message2 = session.createTextMessage(message2Prefix + payload);

         message1.setJMSType("typeA");
         message2.setJMSType("typeB");

         producer.send(message1);
         producer.send(message2);

         connection.start();

         final Message received1 = consumer.receive(5_000);
         final Message received2 = consumer.receive(5_000);

         assertNotNull(received1);
         assertNotNull(received2);
      }

      assertEquals(0, queue1View.getMessageCount());
      assertEquals(0, queue2View.getMessageCount());

      server.replay(null, null, queueName1, queueName2, "JMSType='typeB'");

      assertEquals(0, queue1View.getMessageCount());
      Wait.assertEquals(1L, () -> queue2View.getMessageCount(), 2_000, 100);

      try (Connection connection = cf.createConnection()) {
         final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         final Queue queue = session.createQueue(queueName2);
         final MessageConsumer consumer = session.createConsumer(queue);

         connection.start();

         final TextMessage received = (TextMessage) consumer.receive(5_000);

         assertNotNull(received);
         assertTrue(((TextMessage) received).getText().startsWith(message2Prefix));
      }
   }

   @Test
   public void testSelectMessageAnnotationFromRetentionAMQP() throws Exception {
      doTestSelectMessageAnnotationFromRetention("AMQP", 10, false);
   }

   @Test
   public void testSelectMessageAnnotationFromRetentionAMQPAndPaging() throws Exception {
      doTestSelectMessageAnnotationFromRetention("AMQP", 10, true);
   }

   protected void doTestSelectMessageAnnotationFromRetention(String protocol, int size, boolean paging) throws Exception {
      final String payload = UUID.randomUUID().toString() + "*".repeat(size);

      if (paging) {
         org.apache.activemq.artemis.core.server.Queue serverQueue = server.locateQueue(queueName1);
         serverQueue.getPagingStore().startPaging();
      }

      final ConnectionFactory cf = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");

      final org.apache.activemq.artemis.core.server.Queue queue1View = server.locateQueue(queueName1);
      final org.apache.activemq.artemis.core.server.Queue queue2View = server.locateQueue(queueName2);

      final String message1Prefix = "message-1:";
      final String message2Prefix = "message-2:";

      {
         final AmqpClient client = new AmqpClient(new URI("tcp://localhost:61616"), null, null);
         final AmqpConnection connection = client.connect();

         try {
            final AmqpSession session = connection.createSession();
            final AmqpSender sender = session.createSender(queueName1);

            final AmqpMessage message1 = new AmqpMessage();
            final AmqpMessage message2 = new AmqpMessage();

            message1.setDurable(true);
            message1.setMessageId("msg" + 1);
            message1.setText(message1Prefix + payload);
            message2.setDurable(true);
            message2.setMessageId("msg" + 2);
            message2.setText(message2Prefix + payload);

            message2.setMessageAnnotation("x-opt-serialNo", 1);

            sender.send(message1);
            sender.send(message2);

         } finally {
            connection.close();
         }
      }

      try (Connection connection = cf.createConnection()) {
         final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         final Queue queue = session.createQueue(queueName1);
         final MessageConsumer consumer = session.createConsumer(queue);

         connection.start();

         final Message received1 = consumer.receive(5_000);
         final Message received2 = consumer.receive(5_000);

         assertNotNull(received1);
         assertNotNull(received2);
      }

      assertEquals(0, queue1View.getMessageCount());
      assertEquals(0, queue2View.getMessageCount());

      server.replay(null, null, queueName1, queueName2, "\"m.x-opt-serialNo\"=1");

      assertEquals(0, queue1View.getMessageCount());
      Wait.assertEquals(1L, () -> queue2View.getMessageCount(), 2_000, 100);

      try (Connection connection = cf.createConnection()) {
         final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         final Queue queue = session.createQueue(queueName2);
         final MessageConsumer consumer = session.createConsumer(queue);

         connection.start();

         final TextMessage received = (TextMessage) consumer.receive(5_000);

         assertNotNull(received);
         assertTrue(((TextMessage) received).getText().startsWith(message2Prefix));
      }
   }
}
