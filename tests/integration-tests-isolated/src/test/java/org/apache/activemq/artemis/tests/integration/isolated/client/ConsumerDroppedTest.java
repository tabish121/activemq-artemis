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

package org.apache.activemq.artemis.tests.integration.isolated.client;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.apache.activemq.artemis.utils.ThreadDumpUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.qpid.protonj2.test.driver.ProtonTestClient;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDroppedTest extends ActiveMQTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   @Test(timeout = 20_000)
   public void testConsumerDroppedWithProtonTestClient() throws Exception {
      int NUMBER_OF_CONNECTIONS = 100;
      ActiveMQServer server = createServer(true, createDefaultConfig(true));
      server.start();
      Queue serverQueue = server.createQueue(new QueueConfiguration("test-queue").setRoutingType(RoutingType.ANYCAST).setAddress("test-queue").setAutoCreated(false));

      ExecutorService executorService = Executors.newFixedThreadPool(NUMBER_OF_CONNECTIONS);
      runAfter(executorService::shutdownNow);

      CountDownLatch done = new CountDownLatch(NUMBER_OF_CONNECTIONS);
      AtomicInteger errors = new AtomicInteger(0);

      for (int i = 0; i < NUMBER_OF_CONNECTIONS; i++) {
         executorService.execute(() -> {
            try (ProtonTestClient peer = new ProtonTestClient()) {
               peer.queueClientSaslAnonymousConnect();
               peer.remoteOpen().queue();
               peer.expectOpen();
               peer.remoteBegin().queue();
               peer.expectBegin();
               peer.remoteAttach().ofReceiver().withName(RandomUtil.randomString()).withSenderSettleModeUnsettled().withReceivervSettlesFirst().withTarget().also().withSource().withAddress("test-queue").withExpiryPolicyOnLinkDetach().withDurabilityOfNone().withCapabilities("queue").withOutcomes("amqp:accepted:list", "amqp:rejected:list").also().queue();
               peer.dropAfterLastHandler(1000); // This closes the netty connection after the attach is written
               peer.connect("localhost", 61616);

               // Waits for all the commands to fire and the drop action to be run.
               peer.waitForScriptToComplete();
               Thread.sleep(1000);
            } catch (Throwable e) {
               errors.incrementAndGet();
               logger.warn(e.getMessage(), e);
            } finally {
               done.countDown();
            }
         });
      }

      Assert.assertTrue(done.await(10, TimeUnit.SECONDS));

      Assert.assertEquals(0, errors.get());

      Wait.assertEquals(0, () -> serverQueue.getConsumers().size(), 5000, 100);
   }

   @Test
   public void testConsumerDroppedAMQP() throws Throwable {
      testConsumerDroppedWithRegularClient("AMQP");

   }

   @Test
   public void testConsumerDroppedCORE() throws Throwable {
      testConsumerDroppedWithRegularClient("CORE");
   }

   @Test
   public void testConsumerDroppedOpenWire() throws Throwable {
      testConsumerDroppedWithRegularClient("OPENWIRE");
   }

   public void testConsumerDroppedWithRegularClient(final String protocol) throws Throwable {
      int NUMBER_OF_CONNECTIONS = 25;
      ActiveMQServer server = createServer(true, createDefaultConfig(true));
      server.start();
      Queue serverQueue = server.createQueue(new QueueConfiguration("test-queue").setRoutingType(RoutingType.ANYCAST).setAddress("test-queue").setAutoCreated(false));

      ExecutorService executorService = Executors.newFixedThreadPool(NUMBER_OF_CONNECTIONS);
      runAfter(executorService::shutdownNow);

      CountDownLatch done = new CountDownLatch(NUMBER_OF_CONNECTIONS);

      ConnectionFactory factory = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");

      final AtomicBoolean running = new AtomicBoolean(true);

      runAfter(() -> running.set(false));

      CyclicBarrier flagStart = new CyclicBarrier(NUMBER_OF_CONNECTIONS + 1);
      flagStart.reset();

      for (int i = 0; i < NUMBER_OF_CONNECTIONS; i++) {
         final int t = i;
         executorService.execute(() -> {
            try {
               boolean alreadyStarted = false;
               AtomicBoolean ex = new AtomicBoolean(true);
               while (running.get()) {
                  try  {
                     // do not be tempted to use try (connection = factory.createConnection())
                     // this is because we don't need to close the connection after a network failure on this test.
                     Connection connection = factory.createConnection();

                     synchronized (ConsumerDroppedTest.this) {
                        runAfter(connection::close);
                     }
                     connection.setExceptionListener(new ExceptionListener() {
                        @Override
                        public void onException(JMSException exception) {
                           ex.set(true);
                        }
                     });
                     flagStart.await(60, TimeUnit.SECONDS);

                     connection.start();

                     Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
                     javax.jms.Queue jmsQueue = session.createQueue("test-queue");

                     while (running.get() && !ex.get()) {
                        if (!alreadyStarted) {
                           alreadyStarted = true;
                        }
                        System.out.println("Consumer");
                        MessageConsumer consumer = session.createConsumer(jmsQueue);
                        Thread.sleep(500);
                     }

                     if (!protocol.equals("CORE")) {
                        connection.close();
                     }
                  } catch (Exception e) {
                     logger.debug(e.getMessage(), e);
                  }
               }
            } finally {
               done.countDown();
            }
         });
      }

      Wait.assertEquals(NUMBER_OF_CONNECTIONS, server.getRemotingService()::getConnectionCount, 5000, 100);

      for (int i = 0; i < 10; i++) {
         try {
            flagStart.await(60, TimeUnit.SECONDS); // align all the clients at the same spot
         } catch (Throwable throwable) {
            logger.info(ThreadDumpUtil.threadDump("timed out flagstart"));
            throw throwable;
         }

         logger.info("*******************************************************************************************************************************\nloop kill {}" + "\n*******************************************************************************************************************************", i);
         server.getRemotingService().getConnections().forEach(r -> {
            r.fail(new ActiveMQException("it's a simulation"));
         });

      }

      running.set(false);
      try {
         flagStart.await(1, TimeUnit.SECONDS);
      } catch (Exception ignored) {
      }
      if (!done.await(10, TimeUnit.SECONDS)) {
         for (int i = 0; i < 10; i++) {
            System.out.println("Will fail");
            Thread.sleep(1000);
         }
         logger.warn(ThreadDumpUtil.threadDump("Still running"));
         Assert.fail("Threads are still running");
      }

      Wait.assertEquals(0, () -> serverQueue.getConsumers().size(), 5000, 100);

   }

}
