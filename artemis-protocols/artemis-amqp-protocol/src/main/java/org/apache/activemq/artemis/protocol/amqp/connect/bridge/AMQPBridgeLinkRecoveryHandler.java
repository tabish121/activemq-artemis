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

import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * State object that tracks the current recovery state including
 * attempts count and delay duration tracking.
 */
public class AMQPBridgeLinkRecoveryHandler<E> implements Closeable {

   private final E linkEntry;
   private final Consumer<E> linkRecoveryConsumer;
   private final AMQPBridgeLinkConfiguration configuration;
   private final int maxRecoveryAttempts;

   private volatile int recoveryAttempts;
   private volatile long nextRecoveryDelay;
   private volatile ScheduledFuture<?> recoveryFuture;

   public AMQPBridgeLinkRecoveryHandler(E linkEntry,
                                        Consumer<E> linkRecoveryConsumer,
                                        AMQPBridgeLinkConfiguration configuration) {
      this.configuration = configuration;
      this.nextRecoveryDelay = configuration.getLinkRecoveryInitialDelay() > 0 ? configuration.getLinkRecoveryInitialDelay() : 1;
      this.maxRecoveryAttempts = configuration.getMaxLinkRecoveryAttempts();
      this.linkEntry = linkEntry;
      this.linkRecoveryConsumer = linkRecoveryConsumer;
   }

   @Override
   public void close() {
      final ScheduledFuture<?> future = this.recoveryFuture;

      if (future != null) {
         future.cancel(false);
      }

      recoveryFuture = null;
   }

   /**
    * When a link used by the bridge fails, this method is used to try and schedule a new
    * connection attempt if the configuration rules allow one. If the configuration does not
    * allow any (more) reconnection attempts this method returns false.
    *
    * @param scheduler
    *    The scheduler to use to schedule the next connection attempt.
    *
    * @return true if an attempt was scheduled or false if no attempts are allowed.
    */
   public boolean tryScheduleNextRecovery(ScheduledExecutorService scheduler) {
      Objects.requireNonNull(scheduler, "The scheduler to use cannot be null");

      if (maxRecoveryAttempts < 0 || recoveryAttempts < maxRecoveryAttempts) {
         recoveryFuture = scheduler.schedule(this::handleReconnectionAttempt, nextRecoveryDelay, TimeUnit.MILLISECONDS);
         return true;
      } else {
         return false;
      }
   }

   private void handleReconnectionAttempt() {
      recoveryFuture = null;

      if (maxRecoveryAttempts < 0) {
         recoveryAttempts++;
      }

      nextRecoveryDelay = configuration.getLinkRecoveryDelay() > 0 ? configuration.getLinkRecoveryDelay() : 1;

      // The consumer is expected to run the operation on the connection thread once invoked.
      linkRecoveryConsumer.accept(linkEntry);
   }
}
