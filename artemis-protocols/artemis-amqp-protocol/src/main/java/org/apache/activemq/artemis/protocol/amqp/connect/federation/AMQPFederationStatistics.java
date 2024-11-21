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
package org.apache.activemq.artemis.protocol.amqp.connect.federation;

/**
 * A statistics class that supports nesting to provide various elements in the federation
 * space a view of its own statistics for federation operations.
 */
public final class AMQPFederationStatistics {

   // TODO: Provide type specific instances that offer view of their respective statistics only

   private final AMQPFederationStatistics parent;

   private volatile long messagesSent;
   private volatile long messagesReceived;

   public AMQPFederationStatistics() {
      this.parent = null;
   }

   public long getMessagesSent() {
      return messagesSent;
   }

   public long getMessagesReceived() {
      return messagesReceived;
   }

   // Should only have the senders and receivers doing updates of these and then
   // have those results trickle up to the parent.

   public void incrementMessagesSent() {
      messagesSent = messagesSent + 1;
      if (parent != null) {
         parent.incrementMessagesSent();
      }
   }

   public void incrementMessagesReceived() {
      messagesReceived = messagesReceived + 1;
      if (parent != null) {
         parent.incrementMessagesReceived();
      }
   }
}
