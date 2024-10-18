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

package org.apache.activemq.artemis.protocol.amqp.connect.bridge;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.protocol.amqp.connect.AMQPBrokerConnection;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPConnectionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPSessionContext;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AMQP Bridge manager object that handles starting and stopping bridge
 * operations as needed for the parent broker connection.
 */
public class AMQPBridgeManager {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static final WildcardConfiguration DEFAULT_WILDCARD_CONFIGURATION = new WildcardConfiguration();

   private final String name;
   private final ActiveMQServer server;
   private final AMQPBrokerConnection brokerConnection;
   private final WildcardConfiguration wildcardConfiguration;
   private final ScheduledExecutorService scheduler;
   private final Map<String, Object> properties;
   private final Map<String, Predicate<Link>> linkClosedinterceptors = new ConcurrentHashMap<>();
   private final Set<AMQPBridgeFromAddressPolicyManager> fromAddressPolicies = new HashSet<>();
   private final Set<AMQPBridgeFromQueuePolicyManager> fromQueuePolicies = new HashSet<>();
   private final Set<AMQPBridgeToAddressPolicyManager> toAddressPolicies = new HashSet<>();
   private final Set<AMQPBridgeToQueuePolicyManager> toQueuePolicies = new HashSet<>();

   private volatile AMQPConnectionContext connection;
   private volatile AMQPSessionContext session;
   private volatile AMQPBridgeConfiguration configuration;

   private volatile boolean started;
   private volatile boolean connected;

   @SuppressWarnings("unchecked")
   AMQPBridgeManager(String name, AMQPBrokerConnection brokerConnection,
                     Set<AMQPBridgeAddressPolicy> fromAddressPolicies,
                     Set<AMQPBridgeAddressPolicy> toAddressPolicies,
                     Set<AMQPBridgeQueuePolicy> fromQueuePolicies,
                     Set<AMQPBridgeQueuePolicy> toQueuePolicies,
                     Map<String, Object> properties) {
      Objects.requireNonNull(name, "Bridge name cannot be null");

      this.name = name;
      this.server = brokerConnection.getServer();
      this.brokerConnection = brokerConnection;
      this.brokerConnection.addLinkClosedInterceptor(getName(), this::invokeLinkClosedInterceptors);
      this.scheduler = server.getScheduledPool();

      if (properties == null || properties.isEmpty()) {
         this.properties = Collections.EMPTY_MAP;
      } else {
         this.properties = (Map<String, Object>) Collections.unmodifiableMap(new HashMap<>(properties));
      }

      fromAddressPolicies.forEach(policy -> this.fromAddressPolicies.add(new AMQPBridgeFromAddressPolicyManager(this, policy)));
      fromQueuePolicies.forEach(policy -> this.fromQueuePolicies.add(new AMQPBridgeFromQueuePolicyManager(this, policy)));
      toAddressPolicies.forEach(policy -> this.toAddressPolicies.add(new AMQPBridgeToAddressPolicyManager(this, policy)));
      toQueuePolicies.forEach(policy -> this.toQueuePolicies.add(new AMQPBridgeToQueuePolicyManager(this, policy)));

      if (server.getConfiguration().getWildcardConfiguration() != null) {
         this.wildcardConfiguration = server.getConfiguration().getWildcardConfiguration();
      } else {
         this.wildcardConfiguration = DEFAULT_WILDCARD_CONFIGURATION;
      }
   }

   /**
    * Start the bridge instance if not already started.
    *
    * @throws ActiveMQException if an error occurs during the start.
    */
   public synchronized void start() throws ActiveMQException {
      if (!started) {
         started = true;
         if (connected) {
            // This occurs if not auto-started and started after connection was established.
            handleConnectionRestored(connection, session);
         }
      }
   }

   /**
    * Stop the bridge instance if not already stopped.
    *
    * @throws ActiveMQException if an error occurs during the stop.
    */
   public synchronized void stop() throws ActiveMQException {
      if (started) {
         started = false;
         handleConnectionDropped();
      }
   }

   /**
    * @return the unique name that was assigned to this server bridge connector.
    */
   public String getName() {
      return name;
   }

   /**
    * @return the {@link ActiveMQServer} instance assigned to this bridge.
    */
   public ActiveMQServer getServer() {
      return server;
   }

   /**
    * @return the {@link AMQPBrokerConnection} that this bridge is attached to.
    */
   public AMQPBrokerConnection getBrokerConnection() {
      return brokerConnection;
   }

   /**
    * @return the {@link WildcardConfiguration} that is in use by this bridge instance.
    */
   public WildcardConfiguration getWildcardConfiguration() {
      return wildcardConfiguration;
   }

   public ScheduledExecutorService getScheduler() {
      return scheduler;
   }

   /**
    * @return is this bridge instance started (may not be connected yet).
    */
   public boolean isStarted() {
      return started;
   }

   /**
    * Called by the parent broker connection when the connection has failed and this AMQP bridge
    * should tear down any active resources and await a reconnect if one is allowed.
    *
    * @throws ActiveMQException if an error occurs processing the connection dropped event
    */
   public synchronized void handleConnectionDropped() throws ActiveMQException {
      connected = false;

      final AtomicReference<Exception> errorCaught = new AtomicReference<>();

      fromAddressPolicies.forEach(manager -> {
         try {
            manager.stop();
         } catch (Exception ex) {
            logger.trace("Exception caught on from address policy manager stop: " , ex);
            errorCaught.compareAndExchange(null, ex);
         }
      });

      toAddressPolicies.forEach(manager -> {
         try {
            manager.stop();
         } catch (Exception ex) {
            logger.trace("Exception caught on to address policy manager stop: " , ex);
            errorCaught.compareAndExchange(null, ex);
         }
      });

      fromQueuePolicies.forEach(manager -> {
         try {
            manager.stop();
         } catch (Exception ex) {
            logger.trace("Exception caught on from queue policy manager stop: " , ex);
            errorCaught.compareAndExchange(null, ex);
         }
      });

      toQueuePolicies.forEach(manager -> {
         try {
            manager.stop();
         } catch (Exception ex) {
            logger.trace("Exception caught on to queue policy manager stop: " , ex);
            errorCaught.compareAndExchange(null, ex);
         }
      });

      session = null;

      if (errorCaught.get() != null) {
         final Exception error = errorCaught.get();
         if (error instanceof ActiveMQException) {
            throw (ActiveMQException) error;
         } else {
            throw (ActiveMQException) new ActiveMQException(error.getMessage()).initCause(error);
         }
      }
   }

   /**
    * Called by the parent broker connection when the connection has been established and this
    * AMQP bridge should build up its active state based on the configuration.
    *
    * @param connection
    *    The new {@link Connection} that represents the currently active connection.
    * @param session
    *    The new {@link Session} that was created for use by broker connection resources.
    *
    * @throws ActiveMQException if an error occurs processing the connection restored event
    */
   public synchronized void handleConnectionRestored(AMQPConnectionContext connection, AMQPSessionContext session) throws ActiveMQException {
      this.connection = connection;
      this.session = session;
      this.configuration = new AMQPBridgeConfiguration(connection, properties);

      if (started) {
         fromAddressPolicies.forEach(manager -> manager.start(session, configuration));
         toAddressPolicies.forEach(manager -> manager.start(session, configuration));
         fromQueuePolicies.forEach(manager -> manager.start(session, configuration));
         toQueuePolicies.forEach(manager -> manager.start(session, configuration));
      }
   }

   /**
    * Error signaling API that can be used to report errors during creation of AMQP links.
    *
    * @param cause
    *    The error that caused the resource creation to fail.
    */
   void signalResourceCreateError(Exception cause) {
      brokerConnection.connectError(cause);
   }

   /**
    * Error signaling API that can be used signal errors encountered during normal operations.
    *
    * @param cause
    *    The error that caused the operation to fail.
    */
   void signalError(Exception cause) {
      brokerConnection.runtimeError(cause);
   }

   /**
    * Adds a remote linked closed event interceptor that can intercept the closed event and
    * if it returns true indicate that the close has been handled and that no further action
    * need to be taken for this event.
    *
    * @param id
    *    A unique Id value that identifies the interceptor for later removal.
    * @param interceptor
    *    The predicate that will be called for any link close.
    *
    * @return this {@link AMQPBridgeManager} instance.
    */
   AMQPBridgeManager addLinkClosedInterceptor(String id, Predicate<Link> interceptor) {
      linkClosedinterceptors.put(id, interceptor);
      return this;
   }

   /**
    * Remove a previously registered link close interceptor from the list of close interceptor bindings.
    *
    * @param id
    *   The id of the interceptor to remove
    *
    * @return this {@link AMQPBridgeManager} instance.
    */
   AMQPBridgeManager removeLinkClosedInterceptor(String id) {
      linkClosedinterceptors.remove(id);
      return this;
   }

   protected boolean invokeLinkClosedInterceptors(Link link) {
      for (Map.Entry<String, Predicate<Link>> interceptor : linkClosedinterceptors.entrySet()) {
         if (interceptor.getValue().test(link)) {
            logger.trace("Remote link[{}] close intercepted and handled by interceptor: {}", link.getName(), interceptor.getKey());
            return true;
         }
      }

      return false;
   }
}
