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
import org.apache.activemq.artemis.core.persistence.CoreMessageObjectPools;
import static org.apache.activemq.artemis.core.persistence.PersisterIDs.AMQPMessagePersisterV3_ID;

public class AMQPMessagePersisterV3 extends AMQPMessagePersisterV2 {

   public static final byte ID = AMQPMessagePersisterV3_ID;

   public static AMQPMessagePersisterV3 INSTANCE;

   public static AMQPMessagePersisterV3 getInstance() {
      if (INSTANCE == null) {
         INSTANCE = new AMQPMessagePersisterV3();
      }
      return INSTANCE;
   }

   protected AMQPMessagePersisterV3() {
      super();
   }

   @Override
   public byte getID() {
      return ID;
   }

   @Override
   public int getEncodeSize(Message record) {
      // Adds the size of expiration to previous size calculation.
      return super.getEncodeSize(record) + Long.BYTES;
   }

   @Override
   public void encode(ActiveMQBuffer buffer, Message record) {
      super.encode(buffer, record);

      buffer.writeLong(record.getExpiration());
   }

   @Override
   public Message decode(ActiveMQBuffer buffer, Message ignore, CoreMessageObjectPools pool) {
//      Message record = super.decode(buffer, ignore, pool);
//
//      assert record != null && AMQPStandardMessage.class.equals(record.getClass());
//
//      ((AMQPStandardMessage)record).reloadExpiration(buffer.readLong());
//
//      return record;
      return null; // TODO
   }
}
