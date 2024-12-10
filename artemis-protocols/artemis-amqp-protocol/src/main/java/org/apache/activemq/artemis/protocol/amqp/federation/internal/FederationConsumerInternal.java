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

package org.apache.activemq.artemis.protocol.amqp.federation.internal;

import java.util.function.Consumer;

import org.apache.activemq.artemis.protocol.amqp.federation.Federation;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationConsumer;

/**
 * Internal federated consumer API that is subject to change without notice.
 */
public interface FederationConsumerInternal extends FederationConsumer {

   /**
    * Starts the consumer instance which can include creating the remote resources
    * and performing any internal initialization needed to fully establish the
    * consumer instance or simply signaling the remote that messages will be
    * accepted once more. This call should not block and any errors encountered
    * on creation of the backing consumer resources should utilize the error
    * handling mechanisms of this {@link Federation} instance.
    * <p>
    * Calling start on an already started consumer should have no effect.
    * Calling start on a stopping consumer should throw an {@link IllegalStateException}.
    * Calling start on a closed consumer should throw an {@link IllegalStateException}.
    *
    * @throws IllegalStateException if start is called on an stopping or closed consumer.
    */
   void start();

   /**
    * @return <code>true</code> if the consumer is currently started.
    */
   boolean isStarted();

   /**
    * Stops message consumption on this consumer instance but leaves the consumer
    * in a state where it could be restarted by a call to {@link #start()} once
    * the consumer enters the stopped state.
    * <p>
    * Since the request to stop can take time to complete and this method cannot
    * block a callback must be provided by the caller that will respond when the
    * consumer has fully come to rest and all pending work is complete. Before the
    * stopped callback is signaled the state of the consumer will be stopping, and
    * afterwards the state will be stopped. A call to start the consumer while in
    * the stopping state should throw an exception as the outcome of the stop is not
    * yet known. A call to {@link #stop(Consumer)} in any state other than in the
    * started state should throw an exception since the callers callback can not
    * be signaled if the state is already stopping, stopped or closed.
    * <p>
    * The supplied {@link Consumer} will be provided a boolean <code>true</code> if
    * the stop operation succeeded or <code>false</code> if the operation timed out.
    * A timed out stop should leave the receiver in a stopping state preventing the
    * owner from restarting the receiver again as the remote state is now considered
    * unknown.
    *
    * @param onStopped
    *       A {@link Consumer} that will be run when the consumer has fully stopped or has timed out.
    *
    * @throws IllegalStateException if {@link #isStarted()} does not return <code>true</code>.
    */
   void stop(Consumer<Boolean> onStopped);

   /**
    * @return <code>true</code> if the consumer is in the process of stopping due to a call to {@link #stop(Consumer)}.
    */
   boolean isStopping();

   /**
    * @return <code>true</code> if the consumer has stopped due to a previous call to {@link #stop(Consumer)}.
    */
   boolean isStopped();

   /**
    * Close the federation consumer instance and cleans up its resources. This method
    * should not block and the actual resource shutdown work should occur asynchronously
    * however the closed state should be indicated immediately and any further attempts
    * start the consumer should result in an exception being thrown.
    */
   void close();

   /**
    * @return <code>true</code> if the consumer has been closed by a call to {@link #close()}.
    */
   boolean isClosed();

   /**
    * Provides and event point for notification of the consumer having been closed by
    * the remote.
    *
    * @param handler
    *    The handler that will be invoked when the remote closes this consumer.
    *
    * @return this consumer instance.
    */
   FederationConsumerInternal setRemoteClosedHandler(Consumer<FederationConsumerInternal> handler);

}
