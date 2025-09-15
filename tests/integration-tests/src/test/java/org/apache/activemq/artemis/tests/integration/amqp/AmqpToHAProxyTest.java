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

import java.lang.invoke.MethodHandles;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.transport.netty.NettyHAProxyServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmqpToHAProxyTest extends AmqpClientTestSupport {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static final int STATIC_HAPROXY_SERVER_WITH_HEADER_V1_PORT = 51516;
   private static final int STATIC_HAPROXY_SERVER_WITH_HEADER_V2_PORT = 51517;
   private static final int STATIC_HAPROXY_SERVER_NO_HEADER_PORT = 51518;

   private static final boolean TRACE_BYTES = true;

   private NettyHAProxyServer proxyThatSendsHeaderV1;
   private NettyHAProxyServer proxyThatSendsHeaderV2;
   private NettyHAProxyServer proxyThatDoesNotSendHeader;

   @Override
   protected String getConfiguredProtocols() {
      return "AMQP,CORE,OPENWIRE";
   }

   @Override
   protected void configureAMQPAcceptorParameters(TransportConfiguration tc) {
      tc.getParams().put("proxyProtocolEnabled", "true");
   }

   @BeforeEach
   @Override
   public void setUp() throws Exception {
      super.setUp();

      proxyThatSendsHeaderV1 = new NettyHAProxyServer(AMQP_PORT);
      proxyThatSendsHeaderV2 = new NettyHAProxyServer(AMQP_PORT);
      proxyThatDoesNotSendHeader = new NettyHAProxyServer(AMQP_PORT);

      proxyThatSendsHeaderV1.setSendProxyHeader(true)
                            .setProxyHeaderVersion(1)
                            .setFrontEndPort(STATIC_HAPROXY_SERVER_WITH_HEADER_V1_PORT)
                            .setTraceBytes(TRACE_BYTES)
                            .start();
      proxyThatSendsHeaderV2.setSendProxyHeader(true)
                            .setProxyHeaderVersion(2)
                            .setFrontEndPort(STATIC_HAPROXY_SERVER_WITH_HEADER_V2_PORT)
                            .setTraceBytes(TRACE_BYTES)
                            .setProxyHeaderVersion(1).start();
      proxyThatDoesNotSendHeader.setSendProxyHeader(false)
                                .setFrontEndPort(STATIC_HAPROXY_SERVER_NO_HEADER_PORT)
                                .setTraceBytes(TRACE_BYTES)
                                .start();

      runAfter(proxyThatSendsHeaderV1::stop);
      runAfter(proxyThatSendsHeaderV2::stop);
      runAfter(proxyThatDoesNotSendHeader::stop);
   }

   @Override
   protected ActiveMQServer createServer() throws Exception {
      return createServer(AMQP_PORT, true);
   }

   @Test
   public void testConnectToProxyThatSendsHeaderV1() throws Exception {
      final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + STATIC_HAPROXY_SERVER_WITH_HEADER_V1_PORT);

      try (Connection connection = factory.createConnection()) {
         final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
         final MessageConsumer consumer = session.createConsumer(session.createTopic("test"));

         connection.start();

         consumer.receiveNoWait();
         consumer.close();
      } catch (Exception ex) {
         logger.debug("Test throw error:", ex);
         throw ex;
      }
   }

   @Test
   public void testConnectToProxyThatSendsHeaderV2() throws Exception {
      final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + STATIC_HAPROXY_SERVER_WITH_HEADER_V2_PORT);

      try (Connection connection = factory.createConnection()) {
         final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
         final MessageConsumer consumer = session.createConsumer(session.createTopic("test"));

         connection.start();

         consumer.receiveNoWait();
         consumer.close();
      } catch (Exception ex) {
         logger.debug("Test throw error:", ex);
         throw ex;
      }
   }

   @Test
   public void testConnectToProxyThatDoesNotSendsHeader() throws Exception {
      final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + STATIC_HAPROXY_SERVER_NO_HEADER_PORT);

      try (Connection connection = factory.createConnection()) {
         final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
         final MessageConsumer consumer = session.createConsumer(session.createTopic("test"));

         connection.start();

         consumer.receiveNoWait();
         consumer.close();
      } catch (Exception ex) {
         logger.debug("Test throw error:", ex);
         throw ex;
      }
   }
}
