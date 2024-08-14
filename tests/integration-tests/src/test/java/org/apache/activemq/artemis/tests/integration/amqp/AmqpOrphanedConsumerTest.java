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
package org.apache.activemq.artemis.tests.integration.amqp;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.invoke.MethodHandles;

import org.apache.activemq.artemis.api.core.JsonUtil;
import org.apache.activemq.artemis.json.JsonValue;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that interrupted AMQP connections with consumers don't leave consumers orphaned if the
 * connection drops unexpectedly.
 */
@Timeout(20)
class AmqpOrphanedConsumerTest extends AmqpClientTestSupport {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   @Test
   public void testSingleReceiverNotOrphanedOnDrop() throws Exception {
      final AmqpClient client = createAmqpClient();
      final AmqpConnection connection = addConnection(client.connect());
      final AmqpSession session = connection.createSession();

      session.createReceiver(getQueueName());

      assertNotNull(getProxyToQueue(getQueueName()));

      Wait.assertTrue(() -> server.locateQueue(getQueueName()).getConsumerCount() == 1L, 5000, 100);

      server.getActiveMQServerControl().closeConsumerConnectionsForAddress(getQueueName());

      Wait.assertFalse(() -> {
         final String localConsumersJSON = server.getActiveMQServerControl().listAllConsumersAsJSON();

         for (JsonValue value : JsonUtil.readJsonArray(localConsumersJSON)) {
           if (value.asJsonObject().getString("status").equals("Orphaned")) {
              logger.info("Orphaned: {}", value);
              return true;
           }
         }

         return false;
      }, 5000, 100);
   }

   @Test
   public void testMultipleReceiversNotOrphanedOnDrop() throws Exception {
      final AmqpClient client = createAmqpClient();
      final AmqpConnection connection = addConnection(client.connect());
      final AmqpSession session = connection.createSession();

      final int RECEIVER_COUNT = 10;

      for (int i = 0; i < RECEIVER_COUNT; ++i) {
         session.createReceiver(getQueueName());
      }

      assertNotNull(getProxyToQueue(getQueueName()));

      Wait.assertTrue(() -> server.locateQueue(getQueueName()).getConsumerCount() == RECEIVER_COUNT, 5000, 100);

      server.getActiveMQServerControl().closeConsumerConnectionsForAddress(getQueueName());

      Wait.assertFalse(() -> {
         final String localConsumersJSON = server.getActiveMQServerControl().listAllConsumersAsJSON();

         for (JsonValue value : JsonUtil.readJsonArray(localConsumersJSON)) {
           if (value.asJsonObject().getString("status").equals("Orphaned")) {
              logger.info("Orphaned: {}", value);
              return true;
           }
         }

         return false;
      }, 10_000, 100);
   }
}
