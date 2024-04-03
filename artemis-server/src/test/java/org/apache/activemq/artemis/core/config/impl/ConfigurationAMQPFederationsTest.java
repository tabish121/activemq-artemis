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

package org.apache.activemq.artemis.core.config.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Set;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.FileDeploymentManager;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectConfiguration;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPFederatedBrokerConnectionElement;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPFederationAddressPolicyElement;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPFederationAddressPolicyElement.AddressMatch;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPFederationQueuePolicyElement;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPFederationQueuePolicyElement.QueueMatch;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the loading of AMQP federation configured from the Federations portion of the XML
 * configuration file
 */
public class ConfigurationAMQPFederationsTest {

   private static final String AMQP_FEDERATIONS_XML_PATH = "ConfigurationTest-federations-amqp.xml";

   private FileConfiguration configuration;
   private List<AMQPBrokerConnectConfiguration> amqpBrokerConnections;

   @Before
   public void setUp() throws Exception {
      configuration = new FileConfiguration();

      FileDeploymentManager deploymentManager = new FileDeploymentManager(AMQP_FEDERATIONS_XML_PATH);
      deploymentManager.addDeployable(configuration);
      deploymentManager.readConfiguration();

      amqpBrokerConnections = configuration.getAMQPConnections();

      assertEquals(6, configuration.getAMQPConnection().size());
   }

   @Test
   public void testAMQPFederationToBrokerConnectionLoadsFederation1() throws Exception {
      final AMQPBrokerConnectConfiguration connection1 = findConnectionByName("amqp-federation-1:connection-1");

      assertNotNull(connection1);
      assertEquals(1, connection1.getConnectionElements().size());
      assertTrue(connection1.getConnectionElements().get(0) instanceof AMQPFederatedBrokerConnectionElement);
      assertEquals("tcp://test-amqp-federation-1:6556", connection1.getUri());
      assertEquals("westuser", connection1.getUser());
      assertEquals("32a10275cf4ab4e9", connection1.getPassword());
      assertTrue(connection1.isAutostart());
      assertEquals(42, connection1.getRetryInterval());
      assertEquals(1, connection1.getReconnectAttempts());

      final AMQPFederatedBrokerConnectionElement federation1 =
         (AMQPFederatedBrokerConnectionElement) connection1.getConnectionElements().get(0);

      assertNotNull(federation1);
      assertEquals("connection-1", federation1.getName());
      assertTrue(federation1.getProperties().isEmpty());

      assertEquals(1, federation1.getLocalAddressPolicies().size());
      assertEquals(1, federation1.getLocalQueuePolicies().size());
      assertEquals(0, federation1.getRemoteAddressPolicies().size());
      assertEquals(0, federation1.getRemoteQueuePolicies().size());

      final AMQPFederationAddressPolicyElement addressPolicy1 = federation1.getLocalAddressPolicies().iterator().next();
      final AMQPFederationQueuePolicyElement queuePolicy1 = federation1.getLocalQueuePolicies().iterator().next();

      assertNotNull(addressPolicy1);
      assertNotNull(queuePolicy1);

      assertTrue(addressPolicy1.getProperties().isEmpty());
      assertFalse(queuePolicy1.getProperties().isEmpty());
      assertEquals("value1", queuePolicy1.getProperties().get("property1"));

      final AMQPBrokerConnectConfiguration connection2 = findConnectionByName("amqp-federation-1:connection-2");

      assertNotNull(connection2);
      assertEquals(1, connection2.getConnectionElements().size());
      assertTrue(connection2.getConnectionElements().get(0) instanceof AMQPFederatedBrokerConnectionElement);
      assertEquals("tcp://test-amqp-federation-2:6556", connection2.getUri());
      assertEquals("eastuser", connection2.getUser());
      assertEquals("32a10275cf4ab4e9", connection2.getPassword());
      assertFalse(connection2.isAutostart());
      assertEquals(43, connection2.getRetryInterval());
      assertEquals(2, connection2.getReconnectAttempts());

      final AMQPFederatedBrokerConnectionElement federation2 =
         (AMQPFederatedBrokerConnectionElement) connection2.getConnectionElements().get(0);

      assertNotNull(federation2);
      assertEquals("connection-2", federation2.getName());
      assertTrue(federation2.getProperties().containsKey("key1"));
      assertTrue(federation2.getProperties().containsKey("key2"));
      assertEquals("value1", federation2.getProperties().get("key1"));
      assertEquals("value2", federation2.getProperties().get("key2"));

      assertEquals(1, federation2.getLocalAddressPolicies().size());
      assertEquals(1, federation2.getLocalQueuePolicies().size());
      assertEquals(0, federation2.getRemoteAddressPolicies().size());
      assertEquals(0, federation2.getRemoteQueuePolicies().size());

      final AMQPFederationAddressPolicyElement addressPolicy2 = federation2.getLocalAddressPolicies().iterator().next();
      final AMQPFederationQueuePolicyElement queuePolicy2 = federation2.getLocalQueuePolicies().iterator().next();

      assertNotNull(addressPolicy2);
      assertNotNull(queuePolicy2);

      assertTrue(addressPolicy1.getAutoDelete());
      assertEquals(12345, addressPolicy1.getAutoDeleteDelay().intValue());
      assertEquals(12, addressPolicy1.getAutoDeleteMessageCount().intValue());
      assertEquals(5, addressPolicy1.getMaxHops());
      assertTrue(addressPolicy1.isEnableDivertBindings());
      assertTrue(queuePolicy1.isIncludeFederated());
      assertEquals(-5, queuePolicy1.getPriorityAdjustment().intValue());

      assertTrue(addressPolicy1.getProperties().isEmpty());
      assertFalse(queuePolicy1.getProperties().isEmpty());
      assertEquals("value1", queuePolicy1.getProperties().get("property1"));

      // Should have applied same policy to both
      assertEquals(addressPolicy1, addressPolicy2);
      assertEquals(queuePolicy1, queuePolicy2);
   }

   @Test
   public void testAMQPFederationToBrokerConnectionLoadsFederation2() throws Exception {
      final AMQPBrokerConnectConfiguration connection1 = findConnectionByName("amqp-federation-2:connection-1");

      assertEquals("globaluser", connection1.getUser());
      assertEquals("32a10275cf4ab4e9", connection1.getPassword());

      assertNotNull(connection1);
      assertEquals(1, connection1.getConnectionElements().size());
      assertTrue(connection1.getConnectionElements().get(0) instanceof AMQPFederatedBrokerConnectionElement);
      assertEquals("tcp://test-amqp-federation-1:6556", connection1.getUri());

      final AMQPFederatedBrokerConnectionElement federation1 =
         (AMQPFederatedBrokerConnectionElement) connection1.getConnectionElements().get(0);

      assertNotNull(federation1);
      assertEquals("connection-1", federation1.getName());

      assertEquals(2, federation1.getLocalAddressPolicies().size());
      assertEquals(0, federation1.getLocalQueuePolicies().size());
      assertEquals(0, federation1.getRemoteAddressPolicies().size());
      assertEquals(0, federation1.getRemoteQueuePolicies().size());

      final AMQPFederationAddressPolicyElement addressPolicy1 = findAddressPolicyByName("address-federation-2-1", federation1.getLocalAddressPolicies());
      final AMQPFederationAddressPolicyElement addressPolicy2 = findAddressPolicyByName("address-federation-2-2", federation1.getLocalAddressPolicies());

      assertNotNull(addressPolicy1);
      assertNotNull(addressPolicy2);
      assertTrue(addressPolicy1.getProperties().isEmpty());
      assertEquals(1, addressPolicy1.getIncludes().size());
      assertEquals(0, addressPolicy1.getExcludes().size());
      assertEquals(1, addressPolicy2.getIncludes().size());
      assertEquals(1, addressPolicy2.getExcludes().size());
      assertNotNull(addressPolicy2.getTransformerConfiguration());
      assertEquals("org.foo.FederationTransformer2", addressPolicy2.getTransformerConfiguration().getClassName());
      assertTrue(addressPolicy2.getTransformerConfiguration().getProperties().containsKey("federationTransformerKey1"));
      assertTrue(addressPolicy2.getTransformerConfiguration().getProperties().containsKey("federationTransformerKey2"));

      final AMQPBrokerConnectConfiguration connection2 = findConnectionByName("amqp-federation-2:connection-2");

      assertNotNull(connection2);
      assertEquals(1, connection2.getConnectionElements().size());
      assertTrue(connection2.getConnectionElements().get(0) instanceof AMQPFederatedBrokerConnectionElement);
      assertEquals("tcp://test-amqp-federation-2:6556", connection2.getUri());

      final AMQPFederatedBrokerConnectionElement federation2 =
         (AMQPFederatedBrokerConnectionElement) connection2.getConnectionElements().get(0);

      assertNotNull(federation2);
      assertEquals("connection-2", federation2.getName());

      assertEquals(0, federation2.getLocalAddressPolicies().size());
      assertEquals(1, federation2.getLocalQueuePolicies().size());
      assertEquals(0, federation2.getRemoteAddressPolicies().size());
      assertEquals(0, federation2.getRemoteQueuePolicies().size());

      final AMQPFederationQueuePolicyElement queuePolicy1 = findQueuePolicyByName("queue-federation-2", federation2.getLocalQueuePolicies());

      assertNotNull(queuePolicy1);
      assertTrue(queuePolicy1.getProperties().isEmpty());
   }

   @Test
   public void testAMQPFederationToBrokerConnectionLoadsFederation3() throws Exception {
      final AMQPBrokerConnectConfiguration connection1 = findConnectionByName("amqp-federation-3:connection-1");
      final AMQPFederatedBrokerConnectionElement federation1 =
         (AMQPFederatedBrokerConnectionElement) connection1.getConnectionElements().get(0);

      assertEquals(1, connection1.getTransportConfigurations().size());

      final TransportConfiguration transportOptions = connection1.getTransportConfigurations().get(0);

      assertEquals("connector1", transportOptions.getName());
      assertEquals("amqp-federation-host-5", transportOptions.getParams().get("host"));
      assertEquals("5678", transportOptions.getParams().get("port"));

      assertEquals(1, federation1.getLocalAddressPolicies().size());
      assertEquals(1, federation1.getLocalQueuePolicies().size());
      assertEquals(1, federation1.getRemoteAddressPolicies().size());
      assertEquals(1, federation1.getRemoteQueuePolicies().size());

      final AMQPFederationAddressPolicyElement addressPolicy1 = findAddressPolicyByName("address-federation-3", federation1.getLocalAddressPolicies());
      final AMQPFederationAddressPolicyElement addressPolicy2 = findAddressPolicyByName("address-federation-3", federation1.getRemoteAddressPolicies());

      assertNotNull(addressPolicy1);
      assertNotNull(addressPolicy2);
      assertEquals(addressPolicy1, addressPolicy2);

      final AddressMatch addressMatch = addressPolicy1.getIncludes().iterator().next();
      assertEquals("the_address", addressMatch.getAddressMatch());
      assertNotNull(addressPolicy1.getTransformerConfiguration());
      assertEquals("org.foo.FederationTransformer3", addressPolicy1.getTransformerConfiguration().getClassName());
      assertTrue(addressPolicy1.getTransformerConfiguration().getProperties().containsKey("federationTransformerKey1"));
      assertTrue(addressPolicy1.getTransformerConfiguration().getProperties().containsKey("federationTransformerKey2"));

      final AMQPFederationQueuePolicyElement queuePolicy1 = findQueuePolicyByName("queue-federation-3", federation1.getLocalQueuePolicies());
      final AMQPFederationQueuePolicyElement queuePolicy2 = findQueuePolicyByName("queue-federation-3", federation1.getRemoteQueuePolicies());

      assertNotNull(queuePolicy1);
      assertNotNull(queuePolicy2);
      assertEquals(queuePolicy1, queuePolicy2);
      final QueueMatch queueMatch = queuePolicy1.getExcludes().iterator().next();
      assertEquals("the_queue", queueMatch.getQueueMatch());
      assertEquals("#", queueMatch.getAddressMatch());
      assertNotNull(queuePolicy1.getTransformerConfiguration());
      assertEquals("org.foo.FederationTransformer3", queuePolicy1.getTransformerConfiguration().getClassName());
      assertTrue(queuePolicy1.getTransformerConfiguration().getProperties().containsKey("federationTransformerKey1"));
      assertTrue(queuePolicy1.getTransformerConfiguration().getProperties().containsKey("federationTransformerKey2"));
   }

   @Test
   public void testAMQPFederationToBrokerConnectionLoadsFederation4() throws Exception {
      final AMQPBrokerConnectConfiguration connection1 = findConnectionByName("amqp-federation-4:connection-1");
      final AMQPFederatedBrokerConnectionElement federation1 =
         (AMQPFederatedBrokerConnectionElement) connection1.getConnectionElements().get(0);

      assertEquals(1, connection1.getTransportConfigurations().size());

      final TransportConfiguration transportOptions = connection1.getTransportConfigurations().get(0);

      assertEquals("connector1", transportOptions.getName());
      assertEquals("amqp-federation-host-5", transportOptions.getParams().get("host"));
      assertEquals("5678", transportOptions.getParams().get("port"));

      assertEquals(1, federation1.getLocalAddressPolicies().size());
      assertEquals(1, federation1.getLocalQueuePolicies().size());
      assertEquals(1, federation1.getRemoteAddressPolicies().size());
      assertEquals(1, federation1.getRemoteQueuePolicies().size());

      final AMQPFederationAddressPolicyElement addressPolicy1 = findAddressPolicyByName("address-federation-4", federation1.getLocalAddressPolicies());
      final AMQPFederationAddressPolicyElement addressPolicy2 = findAddressPolicyByName("address-federation-4", federation1.getRemoteAddressPolicies());

      assertNotNull(addressPolicy1);
      assertNotNull(addressPolicy2);
      assertEquals(addressPolicy1, addressPolicy2);

      final AddressMatch addressMatch = addressPolicy1.getIncludes().iterator().next();
      assertEquals("the_address", addressMatch.getAddressMatch());
      assertNull(addressPolicy1.getTransformerConfiguration());

      final AMQPFederationQueuePolicyElement queuePolicy1 = findQueuePolicyByName("queue-federation-4", federation1.getLocalQueuePolicies());
      final AMQPFederationQueuePolicyElement queuePolicy2 = findQueuePolicyByName("queue-federation-4", federation1.getRemoteQueuePolicies());

      assertNotNull(queuePolicy1);
      assertNotNull(queuePolicy2);
      assertEquals(queuePolicy1, queuePolicy2);
      final QueueMatch queueMatch = queuePolicy1.getExcludes().iterator().next();
      assertEquals("the_queue", queueMatch.getQueueMatch());
      assertEquals("#", queueMatch.getAddressMatch());
      assertNull(queuePolicy1.getTransformerConfiguration());
   }

   private AMQPFederationAddressPolicyElement findAddressPolicyByName(String name, Set<AMQPFederationAddressPolicyElement> policies) {
      for (AMQPFederationAddressPolicyElement policy : policies) {
         if (policy.getName().equals(name)) {
            return policy;
         }
      }

      return null;
   }

   private AMQPFederationQueuePolicyElement findQueuePolicyByName(String name, Set<AMQPFederationQueuePolicyElement> policies) {
      for (AMQPFederationQueuePolicyElement policy : policies) {
         if (policy.getName().equals(name)) {
            return policy;
         }
      }

      return null;
   }

   private AMQPBrokerConnectConfiguration findConnectionByName(String name) {
      for (int i = 0; i < amqpBrokerConnections.size(); ++i) {
         final AMQPBrokerConnectConfiguration connection = amqpBrokerConnections.get(i);

         if (connection.getName().equals(name)) {
            return connection;
         }
      }

      return null;
   }
}
