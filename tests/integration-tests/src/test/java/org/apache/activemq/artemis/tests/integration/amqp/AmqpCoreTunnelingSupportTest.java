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

import static org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport.CORE_MESSAGE_TUNNELING_SUPPORT;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.qpid.protonj2.test.driver.ProtonTestClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AmqpCoreTunnelingSupportTest extends AmqpClientTestSupport {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   @Override
   protected String getConfiguredProtocols() {
      return "AMQP,CORE";
   }

   @Override
   protected ActiveMQServer createServer() throws Exception {
      // Creates the broker used to make the outgoing connection. The port passed is for
      // that brokers acceptor. The test server connected to by the broker binds to a random port.
      return createServer(AMQP_PORT, true);
   }

   @Test
   @Timeout(20)
   public void testReceiverDesiresCoreTunneling() throws Exception {
      try (ProtonTestClient peer = new ProtonTestClient()) {
         peer.queueClientSaslAnonymousConnect();
         peer.connect("localhost", AMQP_PORT);
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         logger.info("Test started, client connected on: {}", AMQP_PORT);

         peer.expectOpen();
         peer.expectBegin();
         peer.expectAttach().withDesiredCapability(CORE_MESSAGE_TUNNELING_SUPPORT.toString());
         peer.remoteOpen().withContainerId("test-sender").now();
         peer.remoteBegin().now();
         peer.remoteAttach().ofReceiver()
                            .withOfferedCapabilities(CORE_MESSAGE_TUNNELING_SUPPORT.toString())
                            .withInitialDeliveryCount(0)
                            .withName("sending-peer")
                            .withSource().withAddress(getTestName())
                                         .withCapabilities("topic").also()
                            .withTarget().also()
                            .now();
         peer.remoteFlow().withLinkCredit(10).now();
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         peer.close();
      }
   }

   @Test
   @Timeout(20)
   public void testSenderDesiresCoreTunneling() throws Exception {
      try (ProtonTestClient peer = new ProtonTestClient()) {
         peer.queueClientSaslAnonymousConnect();
         peer.connect("localhost", AMQP_PORT);
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         logger.info("Test started, client connected on: {}", AMQP_PORT);

         peer.expectOpen();
         peer.expectBegin();
         peer.expectAttach().ofReceiver().withOfferedCapability(CORE_MESSAGE_TUNNELING_SUPPORT.toString());
         peer.remoteOpen().withContainerId("test-sender").now();
         peer.remoteBegin().now();
         peer.remoteAttach().ofSender()
                            .withDesiredCapabilities(CORE_MESSAGE_TUNNELING_SUPPORT.toString())
                            .withInitialDeliveryCount(0)
                            .withName("sending-peer")
                            .withTarget().withAddress(getTestName())
                                         .withCapabilities("topic").also()
                            .withSource().also()
                            .now();
         peer.remoteFlow().withLinkCredit(10).now();
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         peer.close();
      }
   }
}
