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

package org.apache.activemq.artemis.protocol.amqp.support;

import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;

/**
 * Defines various AMQP specification and AMQP JMS mapping specification constants
 */
public abstract class AMQPConnectionConstants {

   private AMQPConnectionConstants() {
      // No reason to create this type.
   }

   /**
    * Offered or desired capability on a connection indicating that the connection has or
    * requires an anonymous relay ability.
    */
   public static final Symbol ANONYMOUS_RELAY = Symbol.valueOf("ANONYMOUS-RELAY");

   /**
    * Offered or desired capability on a connection indicating that the connection has or
    * requires an delayed delivery ability.
    */
   public static final Symbol DELAYED_DELIVERY = Symbol.valueOf("DELAYED_DELIVERY");

   /**
    * Offered or desired capability on a connection indicating that the connection has or
    * requires an shared subscription ability.
    */
   public static final Symbol SHARED_SUBS = Symbol.valueOf("SHARED-SUBS");

   /**
    * Offered or desired capability on a connection indicating that the connection has or
    * requires an single connection per container ID ability.
    */
   public static final Symbol SOLE_CONNECTION_CAPABILITY = Symbol.valueOf("sole-connection-for-container");

   /**
    * Connection property that can be applied to signal to the remote that the connection
    * opened failed on the remote and that a close frame is incoming.
    */
   public static final Symbol CONNECTION_OPEN_FAILED = Symbol.valueOf("amqp:connection-establishment-failed");

   /**
    * Property used in {@link ErrorCondition} information maps when a connection is redirected, this value
    * defines the network host / IP address where the connection is being redirected to.
    */
   public static final Symbol REDIRECT_NETWORK_HOST = Symbol.valueOf("network-host");

   /**
    * Property used in {@link ErrorCondition} information maps when a connection is redirected, this value
    * defines the port on the host where the connection is being redirected to.
    */
   public static final Symbol REDIRECT_PORT = Symbol.valueOf("port");

   /**
    * Property used in {@link ErrorCondition} information maps when a connection is redirected, this value
    * defines the transport scheme to use for the host where the connection is being redirected to.
    */
   public static final Symbol REDIRECT_SCHEME = Symbol.valueOf("scheme");

   /**
    * Property used in {@link ErrorCondition} information maps when a connection is redirected, this value
    * defines the actual host name to use for the host where the connection is being redirected to.
    */
   public static final Symbol REDIRECT_HOSTNAME = Symbol.valueOf("hostname");

   /**
    * Used when rejecting a connection because the container ID value is already in use and the sole
    * connection for container ID capability was sent in the remote open.
    */
   public static final Symbol CONTAINER_ID = Symbol.valueOf("container-id");

}
