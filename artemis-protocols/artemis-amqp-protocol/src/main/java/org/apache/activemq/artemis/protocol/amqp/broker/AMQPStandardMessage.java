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
package org.apache.activemq.artemis.protocol.amqp.broker;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.persistence.CoreMessageObjectPools;
import org.apache.activemq.artemis.core.persistence.Persister;
import org.apache.activemq.artemis.protocol.amqp.util.TLSEncode;
import org.apache.activemq.artemis.utils.DataConstants;
import org.apache.activemq.artemis.utils.collections.TypedProperties;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.EncoderState;

// see https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#section-message-format
public class AMQPStandardMessage extends AMQPMessage {

   // Buffer and state for the data backing this message.
   protected ProtonBuffer data;

   /**
    * Creates a new {@link AMQPStandardMessage} instance from binary encoded message data.
    *
    * @param messageFormat   The Message format tag given the in Transfer that carried this message
    * @param data            The encoded AMQP message
    * @param extraProperties Broker specific extra properties that should be carried with this message
    */
   public AMQPStandardMessage(long messageFormat, byte[] data, TypedProperties extraProperties) {
      this(messageFormat, data, extraProperties, null);
   }

   /**
    * Creates a new {@link AMQPStandardMessage} instance from binary encoded message data.
    *
    * @param messageFormat          The Message format tag given the in Transfer that carried this message
    * @param data                   The encoded AMQP message
    * @param extraProperties        Broker specific extra properties that should be carried with this message
    * @param coreMessageObjectPools Object pool used to accelerate some String operations.
    */
   public AMQPStandardMessage(long messageFormat,
                              byte[] data,
                              TypedProperties extraProperties,
                              CoreMessageObjectPools coreMessageObjectPools) {
      this(messageFormat, ReadableBuffer.ByteBufferReader.wrap(data), extraProperties, coreMessageObjectPools);
   }

   /**
    * Creates a new {@link AMQPStandardMessage} instance from binary encoded message data.
    *
    * @param messageFormat          The Message format tag given the in Transfer that carried this message
    * @param data                   The encoded AMQP message in an {@link ReadableBuffer} wrapper.
    * @param extraProperties        Broker specific extra properties that should be carried with this message
    * @param coreMessageObjectPools Object pool used to accelerate some String operations.
    */
   public AMQPStandardMessage(long messageFormat,
                              ReadableBuffer data,
                              TypedProperties extraProperties,
                              CoreMessageObjectPools coreMessageObjectPools) {
      super(messageFormat, extraProperties, coreMessageObjectPools);

      if (data.hasArray()) {
         this.data = ProtonByteBufferAllocator.DEFAULT.wrap(data.array(), data.arrayOffset(), data.remaining());
      } else {
         this.data = ProtonByteBufferAllocator.DEFAULT.allocate(data.remaining()).writeBytes(data.byteBuffer());
      }

      ensureMessageDataScanned();
   }

   /**
    * Creates a new {@link AMQPStandardMessage} instance from binary encoded message data.
    *
    * @param messageFormat          The Message format tag given the in Transfer that carried this message
    * @param data                   The encoded AMQP message in an {@link ReadableBuffer} wrapper.
    * @param extraProperties        Broker specific extra properties that should be carried with this message
    * @param coreMessageObjectPools Object pool used to accelerate some String operations.
    */
   public AMQPStandardMessage(long messageFormat,
                              ProtonBuffer data,
                              TypedProperties extraProperties,
                              CoreMessageObjectPools coreMessageObjectPools) {
      super(messageFormat, extraProperties, coreMessageObjectPools);
      this.data = data;
      ensureMessageDataScanned();
   }

   /**
    * Internal constructor used for persistence reload of the message.
    * <p>
    * The message will not be usable until the persistence mechanism populates the message
    * data and triggers a parse of the message contents to fill in the message state.
    *
    * @param messageFormat The Message format tag given the in Transfer that carried this message
    */
   AMQPStandardMessage(long messageFormat) {
      super(messageFormat);
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message copy() {
      ensureDataIsValid();

      AMQPStandardMessage newEncode = new AMQPStandardMessage(this.messageFormat, data.copy(), extraProperties, coreMessageObjectPools);
      newEncode.setMessageID(this.getMessageID());
      return newEncode;
   }

   @Override
   public int getEncodeSize() {
      ensureDataIsValid();
      // The encoded size will exclude any delivery annotations that are present as we will clip them.
      return data.getReadableBytes() - encodedDeliveryAnnotationsSize + getDeliveryAnnotationsForSendBufferSize();
   }

   @Override
   protected ProtonBuffer getData() {
      return data;
   }

   @Override
   public int getMemoryEstimate() {
      if (memoryEstimate == -1) {
         memoryEstimate = memoryOffset + (data != null ? data.capacity() : 0);
      }

      return memoryEstimate;
   }

   @Override
   public void persist(ActiveMQBuffer targetRecord) {
      ensureDataIsValid();
      targetRecord.writeInt(internalPersistSize());
      if (data.hasArray()) {
         targetRecord.writeBytes(data.getArray(), data.getArrayOffset(), data.getReadableBytes());
      } else {
         targetRecord.writeBytes(data.toByteBuffer());
      }
   }

   @Override
   public final org.apache.activemq.artemis.api.core.Message copy(long newID) {
      return copy().setMessageID(newID);
   }

   @Override
   public int getPersistSize() {
      ensureDataIsValid();
      return DataConstants.SIZE_INT + internalPersistSize();
   }

   @Override
   public void reloadPersistence(ActiveMQBuffer record, CoreMessageObjectPools pools) {
      int size = record.readInt();
      byte[] recordArray = new byte[size];
      record.readBytes(recordArray);
      data = ProtonByteBufferAllocator.DEFAULT.wrap(recordArray);

      // Message state is now that the underlying buffer is loaded, but the contents not yet scanned
      resetMessageData();
      modified = false;
      messageDataScanned = RELOAD_PERSISTENCE;
      // can happen when moved to a durable location.  We must re-encode here to
      // avoid a subsequent redelivery from suddenly appearing with a durable header
      // tag when the initial delivery did not.
      if (!isDurable()) {
         setDurable(true);
         reencode();
      }
   }

   @Override
   public long getPersistentSize() throws ActiveMQException {
      return getEncodeSize();
   }

   @Override
   public Persister<org.apache.activemq.artemis.api.core.Message> getPersister() {
      return AMQPMessagePersisterV2.getInstance();
   }

   @Override
   public void reencode() {
      ensureMessageDataScanned();

      // The address was updated on a message with Properties so we update them
      // for cases where there are no properties we aren't adding a properties
      // section which seems wrong but this preserves previous behavior.
      if (properties != null && address != null) {
         properties.setTo(address.toString());
      }

      encodeMessage();
      scanMessageData();
   }

   @Override
   protected synchronized void ensureDataIsValid() {
      if (modified) {
         encodeMessage();
      }
   }

   @Override
   protected synchronized void encodeMessage() {
      this.modified = false;
      this.messageDataScanned = NOT_SCANNED;
      int estimated = Math.max(1500, data != null ? data.capacity() + 1000 : 0);

      final EncoderState state = TLSEncode.getEncoderState();
      final ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate(estimated);

      try {
         if (header != null) {
            DEFAULT_ENCODER.writeObject(buffer, state, header);
         }

         // We currently do not encode any delivery annotations but it is conceivable
         // that at some point they may need to be preserved, this is where that needs
         // to happen.

         if (messageAnnotations != null) {
            DEFAULT_ENCODER.writeObject(buffer, state, messageAnnotations);
         }
         if (properties != null) {
            DEFAULT_ENCODER.writeObject(buffer, state, properties);
         }

         // Whenever possible avoid encoding sections we don't need to by
         // checking if application properties where loaded or added and
         // encoding only in that case.
         if (applicationProperties != null) {
            DEFAULT_ENCODER.writeObject(buffer, state, applicationProperties);

            // Now raw write the remainder body and footer if present.
            if (data != null && remainingBodyPosition != VALUE_NOT_PRESENT) {
               data.getBytes(remainingBodyPosition, buffer);
            }
         } else if (data != null && applicationPropertiesPosition != VALUE_NOT_PRESENT) {
            // Writes out ApplicationProperties, Body and Footer in one go if present.
            data.getBytes(applicationPropertiesPosition, buffer);
         } else if (data != null && remainingBodyPosition != VALUE_NOT_PRESENT) {
            // No Application properties at all so raw write Body and Footer sections
            data.getBytes(remainingBodyPosition, buffer);
         }
      } finally {
         state.reset();
      }

      data = buffer;
   }
}
