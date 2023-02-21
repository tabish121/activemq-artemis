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
package org.apache.activemq.artemis.protocol.amqp;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.persistence.CoreMessageObjectPools;
import org.apache.activemq.artemis.protocol.amqp.message.AMQPMessage;
import org.apache.activemq.artemis.spi.core.protocol.MessagePersister;
import org.apache.activemq.artemis.utils.DataConstants;
import static org.apache.activemq.artemis.core.persistence.PersisterIDs.AMQPMessagePersister_ID;

public class AMQPMessagePersister extends MessagePersister {

   public static final byte ID = AMQPMessagePersister_ID;

   public static AMQPMessagePersister INSTANCE;

   public static AMQPMessagePersister getInstance() {
      if (INSTANCE == null) {
         INSTANCE = new AMQPMessagePersister();
      }
      return INSTANCE;
   }

   protected AMQPMessagePersister() {
   }

   @Override
   public byte getID() {
      return ID;
   }

   @Override
   public int getEncodeSize(Message record) {
      return DataConstants.SIZE_BYTE + record.getPersistSize() +
         SimpleString.sizeofNullableString(record.getAddressSimpleString()) + DataConstants.SIZE_LONG + DataConstants.SIZE_LONG;
   }

   @Override
   public void encode(ActiveMQBuffer buffer, Message record) {
      super.encode(buffer, record);
      AMQPMessage msgEncode = (AMQPMessage)record;
      buffer.writeLong(record.getMessageID());
      buffer.writeLong(msgEncode.getMessageFormat());
      buffer.writeNullableSimpleString(record.getAddressSimpleString());
      record.persist(buffer);
   }

   @Override
   public Message decode(ActiveMQBuffer buffer, Message record,  CoreMessageObjectPools pool) {
      final long id = buffer.readLong();
      @SuppressWarnings("unused")
      final long format = buffer.readLong();
      final SimpleString address;

      if (pool == null) {
         address = buffer.readNullableSimpleString();
      } else {
         address = SimpleString.readNullableSimpleString(buffer.byteBuf(), pool.getAddressDecoderPool());
      }

      // TODO      record = new AMQPStandardMessage(format);
      record.reloadPersistence(buffer, pool);
      record.setMessageID(id);
      if (address != null) {
         record.setAddress(address);
      }
      return record;
   }
}
