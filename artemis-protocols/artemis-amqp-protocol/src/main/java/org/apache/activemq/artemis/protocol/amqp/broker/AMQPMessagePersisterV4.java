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
package org.apache.activemq.artemis.protocol.amqp.broker;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.persistence.CoreMessageObjectPools;
import org.apache.activemq.artemis.utils.DataConstants;

import static org.apache.activemq.artemis.core.persistence.PersisterIDs.AMQPMessagePersisterV4_ID;

/**
 * Persister V4 add encode of the AMQP priority value to the record to allow
 * messages to be reloaded and placed in the Queue based on the priority they
 * were sent with originally without adding a full rescan of the AMQP encoding.
 */
public class AMQPMessagePersisterV4 extends AMQPMessagePersisterV3 {

   public static final byte ID = AMQPMessagePersisterV4_ID;

   public static AMQPMessagePersisterV4 theInstance;

   public static AMQPMessagePersisterV4 getInstance() {
      if (theInstance == null) {
         theInstance = new AMQPMessagePersisterV4();
      }
      return theInstance;
   }

   @Override
   public byte getID() {
      return ID;
   }

   public AMQPMessagePersisterV4() {
      super();
   }

   @Override
   public int getEncodeSize(Message record) {
      return super.getEncodeSize(record) + DataConstants.SIZE_BYTE; // priority
   }

   @Override
   public void encode(ActiveMQBuffer buffer, Message record) {
      super.encode(buffer, record);

      buffer.writeByte(record.getPriority());
   }

   @Override
   public Message decode(ActiveMQBuffer buffer, Message ignore, CoreMessageObjectPools pool) {
      Message record = super.decode(buffer, ignore, pool);

      assert record != null && AMQPStandardMessage.class.equals(record.getClass());

      ((AMQPStandardMessage)record).reloadPriority(buffer.readByte());

      return record;
   }
}
