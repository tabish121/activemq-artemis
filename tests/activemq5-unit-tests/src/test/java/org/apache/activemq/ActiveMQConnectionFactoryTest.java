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
package org.apache.activemq;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Session;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.activemq.artemiswrapper.ArtemisBrokerHelper;
import org.apache.activemq.broker.BrokerRegistry;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.transport.tcp.TcpTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActiveMQConnectionFactoryTest extends CombinationTestSupport {

   private static final Logger LOG = LoggerFactory.getLogger(ActiveMQConnectionFactoryTest.class);

   private ActiveMQConnection connection;
   private BrokerService broker;

   public void testUseURIToSetUseClientIDPrefixOnConnectionFactory() throws URISyntaxException, JMSException {
      ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost?jms.clientIDPrefix=Cheese");
      assertEquals("Cheese", cf.getClientIDPrefix());

      connection = (ActiveMQConnection) cf.createConnection();
      connection.start();

      String clientID = connection.getClientID();
      LOG.info("Got client ID: " + clientID);

      assertTrue("should start with Cheese! but was: " + clientID, clientID.startsWith("Cheese"));
   }

   @Override
   public void tearDown() throws Exception {
      // Try our best to close any previously opened connection.
      try {
         connection.close();
      } catch (Throwable ignore) {
      }
      // Try our best to stop any previously started broker.
      try {
         broker.stop();
      } catch (Throwable ignore) {
      }
      try {
         ArtemisBrokerHelper.stopArtemisBroker();
      } catch (Throwable ignore) {
      }
      TcpTransportFactory.clearService();
   }

   public void testUseURIToSetOptionsOnConnectionFactory() throws URISyntaxException, JMSException {
      ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost?jms.useAsyncSend=true");
      assertTrue(cf.isUseAsyncSend());
      // the broker url have been adjusted.
      assertEquals("vm://localhost", cf.getBrokerURL());

      cf = new ActiveMQConnectionFactory("vm://localhost?jms.useAsyncSend=false");
      assertFalse(cf.isUseAsyncSend());
      // the broker url have been adjusted.
      assertEquals("vm://localhost", cf.getBrokerURL());

      cf = new ActiveMQConnectionFactory("vm:(broker:()/localhost)?jms.useAsyncSend=true");
      assertTrue(cf.isUseAsyncSend());
      // the broker url have been adjusted.
      assertEquals("vm:(broker:()/localhost)", cf.getBrokerURL());

      cf = new ActiveMQConnectionFactory("vm://localhost?jms.auditDepth=5000");
      assertEquals(5000, cf.getAuditDepth());
   }

   public void testUseURIToConfigureRedeliveryPolicy() throws URISyntaxException, JMSException {
      ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost?jms.redeliveryPolicy.maximumRedeliveries=2");
      assertEquals("connection redeliveries", 2, cf.getRedeliveryPolicy().getMaximumRedeliveries());

      ActiveMQConnection connection = (ActiveMQConnection) cf.createConnection();
      assertEquals("connection redeliveries", 2, connection.getRedeliveryPolicy().getMaximumRedeliveries());

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      ActiveMQMessageConsumer consumer = (ActiveMQMessageConsumer) session.createConsumer(session.createQueue("FOO.BAR"));
      assertEquals("consumer redeliveries", 2, consumer.getRedeliveryPolicy().getMaximumRedeliveries());
      connection.close();
   }

   //we don't support in-vm connector (will we?)
   public void testCreateVMConnectionWithEmbeddedBroker() throws URISyntaxException, JMSException {
      ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://myBroker2?broker.persistent=false");
      // Make sure the broker is not created until the connection is
      // instantiated.
      assertNull(BrokerRegistry.getInstance().lookup("myBroker2"));
      connection = (ActiveMQConnection) cf.createConnection();
      // This should create the connection.
      assertNotNull(connection);
      // Verify the broker was created.
      assertNotNull(BrokerRegistry.getInstance().lookup("myBroker2"));

      connection.close();

      // Verify the broker was destroyed.
      //I comment out this because this is pure client behavior in
      //amq5. there shouldn't be any use-case like that with Artemis.
      //assertNull(BrokerRegistry.getInstance().lookup("myBroker2"));
   }

   public void testGetBrokerName() throws URISyntaxException, JMSException {
      System.out.println("------------------beging testing...............");
      ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
      connection = (ActiveMQConnection) cf.createConnection();
      connection.start();

      String brokerName = connection.getBrokerName();
      LOG.info("Got broker name: " + brokerName);

      assertNotNull("No broker name available!", brokerName);
   }

   public void testCreateTcpConnectionUsingAllocatedPort() throws Exception {
      assertCreateConnection("tcp://localhost:0?wireFormat.tcpNoDelayEnabled=true");
   }

   public void testCreateTcpConnectionUsingKnownPort() throws Exception {
      assertCreateConnection("tcp://localhost:61610?wireFormat.tcpNoDelayEnabled=true");
   }

   public void testCreateTcpConnectionUsingKnownLocalPort() throws Exception {
      broker = new BrokerService();
      broker.setPersistent(false);
      broker.addConnector("tcp://localhost:61610?wireFormat.tcpNoDelayEnabled=true");
      broker.start();

      // This should create the connection.
      ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("tcp://localhost:61610/localhost:51610");
      connection = (ActiveMQConnection) cf.createConnection();
      assertNotNull(connection);

      connection.close();

      broker.stop();
   }

   public void testConnectionFailsToConnectToVMBrokerThatIsNotRunning() throws Exception {
      ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost?create=false");
      try {
         factory.createConnection();
         fail("Expected connection failure.");
      } catch (JMSException e) {
      }
   }

   public void testFactorySerializable() throws Exception {
      String clientID = "TestClientID";
      ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory();
      cf.setClientID(clientID);
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      ObjectOutputStream objectsOut = new ObjectOutputStream(bytesOut);
      objectsOut.writeObject(cf);
      objectsOut.flush();
      byte[] data = bytesOut.toByteArray();
      ByteArrayInputStream bytesIn = new ByteArrayInputStream(data);
      ObjectInputStream objectsIn = new ObjectInputStream(bytesIn);
      cf = (ActiveMQConnectionFactory) objectsIn.readObject();
      assertEquals(cf.getClientID(), clientID);
   }

   public void testSetExceptionListener() throws Exception {
      ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
      connection = (ActiveMQConnection) cf.createConnection();
      assertNull(connection.getExceptionListener());

      ExceptionListener exListener = arg0 -> {};
      cf.setExceptionListener(exListener);
      connection.close();

      connection = (ActiveMQConnection) cf.createConnection();
      assertNotNull(connection.getExceptionListener());
      assertEquals(exListener, connection.getExceptionListener());
      connection.close();

      connection = (ActiveMQConnection) cf.createConnection();
      assertEquals(exListener, connection.getExceptionListener());

      assertEquals(exListener, cf.getExceptionListener());
      connection.close();

   }

   public void testSetClientInternalExceptionListener() throws Exception {
      ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
      connection = (ActiveMQConnection) cf.createConnection();
      assertNull(connection.getClientInternalExceptionListener());

      ClientInternalExceptionListener listener = exception -> {};
      connection.setClientInternalExceptionListener(listener);
      cf.setClientInternalExceptionListener(listener);
      connection.close();

      connection = (ActiveMQConnection) cf.createConnection();
      assertNotNull(connection.getClientInternalExceptionListener());
      assertEquals(listener, connection.getClientInternalExceptionListener());
      connection.close();

      connection = (ActiveMQConnection) cf.createConnection();
      assertEquals(listener, connection.getClientInternalExceptionListener());
      assertEquals(listener, cf.getClientInternalExceptionListener());
      connection.close();

   }

   protected void assertCreateConnection(String uri) throws Exception {
      // Start up a broker with a tcp connector.
      broker = new BrokerService();
      broker.setPersistent(false);
      broker.setUseJmx(false);
      TransportConnector connector = broker.addConnector(uri);
      broker.start();

      URI temp = new URI(uri);
      // URI connectURI = connector.getServer().getConnectURI();
      // TODO this sometimes fails when using the actual local host name
      URI currentURI = new URI(connector.getPublishableConnectString());

      // sometimes the actual host name doesn't work in this test case
      // e.g. on OS X so lets use the original details but just use the actual
      // port
      URI connectURI = new URI(temp.getScheme(), temp.getUserInfo(), temp.getHost(), currentURI.getPort(), temp.getPath(), temp.getQuery(), temp.getFragment());

      LOG.info("connection URI is: " + connectURI);

      // This should create the connection.
      ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(connectURI);
      connection = (ActiveMQConnection) cf.createConnection();
      assertNotNull(connection);
   }

}
