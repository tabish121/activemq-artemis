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

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPSessionContext;
import org.apache.qpid.proton.engine.Session;

/**
 * Base API for a bridge resource policy manager.
 */
public interface AMQPBridgePolicyManager {

   /**
    * @return if this policy manager is currently started.
    */
   boolean isStarted();

   /**
    * Starts the policy manager such that if connected or on later connect the manager
    * operations begin.
    *
    * @throws ActiveMQException
    */
   void start() throws ActiveMQException;

   /**
    * Stops the policy manager such that all manager services will cease and if
    * the connection was down and it returns manager operations will not restart
    * automatically.
    */
   void stop();

   /**
    * Called by the parent AMQP bridge manager when the connection has failed and this AMQP policy
    * manager should tear down any active resources and await a reconnect if one is allowed.
    */
   void connectionDropped();

   /**
    * Called by the parent AMQP bridge manager when the connection has been established and this
    * AMQP policy manager should build up its active state based on the configuration. If not
    * started when a connection is established the policy manager services should remain stopped.
    *
    * @param session
    *    The new {@link Session} that was created for use by broker connection resources.
    * @param configuration
    *    The bridge configuration that hold state relative to the new active connection.
    *
    * @throws ActiveMQException if an error occurs processing the connection restored event
    */
   void connectionRestored(AMQPSessionContext session, AMQPBridgeConfiguration configuration) throws ActiveMQException;

}
