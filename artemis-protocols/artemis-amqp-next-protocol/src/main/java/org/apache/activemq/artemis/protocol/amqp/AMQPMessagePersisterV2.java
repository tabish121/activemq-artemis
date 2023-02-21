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
import org.apache.activemq.artemis.utils.DataConstants;
import org.apache.activemq.artemis.utils.collections.TypedProperties;
import static org.apache.activemq.artemis.core.persistence.PersisterIDs.AMQPMessagePersisterV2_ID;

public class AMQPMessagePersisterV2 extends AMQPMessagePersister {

   public static final byte ID = AMQPMessagePersisterV2_ID;

   public static AMQPMessagePersisterV2 INSTANCE;

   public static AMQPMessagePersisterV2 getInstance() {
      if (INSTANCE == null) {
         INSTANCE = new AMQPMessagePersisterV2();
      }
      return INSTANCE;
   }

   protected AMQPMessagePersisterV2() {
      super();
   }

   @Override
   public byte getID() {
      return ID;
   }

   @Override
   public int getEncodeSize(Message record) {
      int encodeSize = super.getEncodeSize(record) + DataConstants.SIZE_INT;

      TypedProperties properties = null; // TODO ((AMQPMessage)record).getExtraProperties();

      return encodeSize + (properties != null ? properties.getEncodeSize() : 0);
   }

   @SuppressWarnings("unused")
   @Override
   public void encode(ActiveMQBuffer buffer, Message record) {
      super.encode(buffer, record);

      TypedProperties properties = null; // TODO ((AMQPMessage)record).getExtraProperties();
      if (properties == null) {
         buffer.writeInt(0);
      } else {
         buffer.writeInt(properties.getEncodeSize());
         properties.encode(buffer.byteBuf());
      }
   }

   @SuppressWarnings("unused")
   @Override
   public Message decode(ActiveMQBuffer buffer, Message ignore, CoreMessageObjectPools pool) {
      // IMPORTANT:
      // This is a sightly modified copy of the AMQPMessagePersister::decode body
      // to prevent extraProperties from be created twice: this would kill GC during journal loading
      final long id = buffer.readLong();
      final long format = buffer.readLong();
      // this instance is being used only if there are no extraProperties or just for debugging purposes:
      // on journal loading pool shouldn't be null so it shouldn't create any garbage.
      final SimpleString address;
      if (pool == null) {
         address = buffer.readNullableSimpleString();
      } else {
         address = SimpleString.readNullableSimpleString(buffer.byteBuf(), pool.getAddressDecoderPool());
      }
//      AMQPStandardMessage record = new AMQPStandardMessage(format);
//      record.reloadPersistence(buffer, pool);
//      record.setMessageID(id);
//      // END of AMQPMessagePersister::decode body copy
//      int size = buffer.readInt();
//      if (size != 0) {
//         final TypedProperties extraProperties = record.createExtraProperties();
//         extraProperties.decode(buffer.byteBuf(), pool != null ? pool.getPropertiesDecoderPools() : null);
//      }
//      record.reloadAddress(address);
//      return record;
      return null; // TODO
   }
}
