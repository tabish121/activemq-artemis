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
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBridgeBrokerConnectionElement;
import org.apache.activemq.artemis.protocol.amqp.connect.AMQPBrokerConnection;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPSessionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple AMQP Bridge aggregation tool used to simplify broker connection management of multiple bridges
 */
public class AMQPBridgeManagers {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private final List<AMQPBridgeManager> bridgeManagers = new ArrayList<>();
   private final AMQPBrokerConnection brokerConnection;

   public AMQPBridgeManagers(AMQPBrokerConnection brokerConnection) {
      this.brokerConnection = brokerConnection;
   }

   /**
    * Stops all bridge managers ignoring any thrown exceptions and clears state
    * from all previously registered manager, new managers can be added after a
    * reset.
    */
   public void reset() {
      if (bridgeManagers != null && !bridgeManagers.isEmpty()) {
         bridgeManagers.forEach(bridgeManager -> {
            try {
               bridgeManager.stop();
            } catch (Exception e) {
               logger.trace("Ignoring ecxeption thrown during bridge manager stop: ", e);
            }
         });
      }

      bridgeManagers.clear();
   }

   /**
    * Adds a new bridge to the collection of managed bridges and starts the new {@link AMQPBridgeManager}
    *
    * @param configuration
    *    The configuration to use when creating a new bridge manager.
    */
   public void addBridgeManager(AMQPBridgeBrokerConnectionElement configuration) throws ActiveMQException {
      final AMQPBridgeManager bridgeManager = AMQPBridgeSupport.createManager(brokerConnection, configuration);

      try {
         bridgeManager.start();
      } catch (ActiveMQException e) {
         logger.debug("Error caught and rethrown while starting configured bridge connection:", e);
         throw e;
      }

      bridgeManagers.add(bridgeManager);
   }

   /**
    * Signal all bridge managers that the connection has been restored
    *
    * @param session
    *    The session in which the bridge manager resources will reside.
    *
    * @throws ActiveMQException if an error occurs during connection restoration.
    */
   public void handleConnectionRestored(AMQPSessionContext session) throws ActiveMQException {
      if (bridgeManagers != null && !bridgeManagers.isEmpty()) {
         for (AMQPBridgeManager bridgeManager : bridgeManagers) {
            try {
               bridgeManager.handleConnectionRestored(session.getAMQPConnectionContext(), session);
            } catch (ActiveMQException e) {
               logger.trace("AMQP Bridge connection {} threw an error on handling of connection restored: ", bridgeManager.getName(), e);
               throw e;
            }
         }
      }
   }

   /**
    * Signals all bridges that the current connection has dropped, exceptions are ignored.
    */
   public void handleConnectionDropped() {
      if (bridgeManagers != null || !bridgeManagers.isEmpty()) {
         for (AMQPBridgeManager bridgeManager : bridgeManagers) {
            if (bridgeManager != null) {
               try {
                  bridgeManager.handleConnectionDropped();
               } catch (ActiveMQException e) {
                  logger.trace("AMQP Bridge connection {} threw an error on handling of connection drop", bridgeManager.getName());
               }
            }
         }
      }
   }
}
