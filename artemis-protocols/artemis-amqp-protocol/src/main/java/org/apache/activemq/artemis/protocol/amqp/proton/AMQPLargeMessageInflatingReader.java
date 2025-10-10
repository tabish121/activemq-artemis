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

package org.apache.activemq.artemis.protocol.amqp.proton;

import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.zip.Inflater;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPLargeMessage;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPSessionCallback;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPInternalErrorException;
import org.apache.activemq.artemis.protocol.amqp.util.NettyReadable;
import org.apache.activemq.artemis.protocol.amqp.util.TLSEncode;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.messaging.Footer;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncodingCodes;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.codec.TypeConstructor;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Reader of {@link AMQPLargeMessage} content which reads and inflates all bytes and completes once a non-partial
 * delivery is read.
 */
public class AMQPLargeMessageInflatingReader implements MessageReader {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private enum State {
      /**
       * Awaiting initial decode of first section in delivery which could be delivery annotations or could be the first
       * data section which will be the compressed AMQP message.
       */
      INITIALIZING,
      /**
       * Accumulating the bytes from the remote that comprise the sections that come before the first Data section that
       * carries the compressed message body.
       */
      PROCESSING_MESSAGE_PREAMBLE,
      /**
       * Accumulating the message payload from the incoming Data sections that comprise the compressed AMQP message.
       */
      BODY_INFLATING,
      /**
       * The full message has been read and no more incoming bytes are accepted.
       */
      DONE,
      /**
       * Indicates the reader is closed and cannot be used until opened.
       */
      CLOSED
   }

   private final ProtonAbstractReceiver serverReceiver;
   private final CompositeByteBuf pendingRecvBuffer = Unpooled.compositeBuffer();
   private final NettyReadable pendingReadable = new NettyReadable(pendingRecvBuffer);

   private volatile AMQPLargeMessage currentMessage;
   private volatile State state = State.CLOSED;
   private DeliveryAnnotations deliveryAnnotations;

   private final Inflater inflater = new Inflater();
   private final byte[] scratchBuffer = new byte[1024];

   public AMQPLargeMessageInflatingReader(ProtonAbstractReceiver serverReceiver) {
      this.serverReceiver = serverReceiver;
   }

   @Override
   public DeliveryAnnotations getDeliveryAnnotations() {
      return deliveryAnnotations;
   }

   @Override
   public void close() {
      if (state != State.CLOSED) {
         try {
            final AMQPSessionCallback sessionSPI = serverReceiver.getSessionContext().getSessionSPI();

            if (currentMessage != null) {
               sessionSPI.execute(() -> {
                  // Run the file delete on the session thread, this allows processing of the
                  // last addBytes to complete which might allow the message to be fully read
                  // in which case currentMessage will be nulled and we won't delete it as it
                  // will have already been handed to the connection thread for enqueue.
                  if (currentMessage != null) {
                     try {
                        currentMessage.deleteFile();
                     } catch (Throwable error) {
                        ActiveMQServerLogger.LOGGER.errorDeletingLargeMessageFile(error);
                     } finally {
                        currentMessage = null;
                     }
                  }
               });
            }
         } catch (Exception ex) {
            logger.trace("AMQP Large Message reader close ignored error: ", ex);
         }

         inflater.reset();
         deliveryAnnotations = null;
         state = State.CLOSED;
      }
   }

   @Override
   public MessageReader open() {
      if (state != State.CLOSED) {
         throw new IllegalStateException("Message reader must be properly closed before open call");
      }

      state = State.INITIALIZING;

      return this;
   }

   @Override
   public Message readBytes(Delivery delivery) throws Exception {
      if (state == State.CLOSED) {
         throw new IllegalStateException("AMQP Compressed Large Message Reader is closed and read cannot proceed");
      }

      try {
         serverReceiver.connection.requireInHandler();
         serverReceiver.getConnection().disableAutoRead();

         final Receiver receiver = ((Receiver) delivery.getLink());
         final ReadableBuffer dataBuffer = receiver.recv();
         final AMQPSessionCallback sessionSPI = serverReceiver.getSessionContext().getSessionSPI();

         if (currentMessage == null) {
            final long id = sessionSPI.getStorageManager().generateID();
            final AMQPLargeMessage localCurrentMessage = new AMQPLargeMessage(id, delivery.getMessageFormat(), null, sessionSPI.getCoreMessageObjectPools(), sessionSPI.getStorageManager());

            localCurrentMessage.parseHeader(dataBuffer);

            sessionSPI.getStorageManager().onLargeMessageCreate(id, localCurrentMessage);
            currentMessage = localCurrentMessage;
         }

         sessionSPI.execute(() -> processRead(delivery, dataBuffer, delivery.isPartial()));

         return null; // Event fired once message is fully processed.
      } catch (Exception e) {
         serverReceiver.getConnection().enableAutoRead();
         throw e;
      }
   }

   private void processRead(Delivery delivery, ReadableBuffer dataBuffer, boolean isPartial) {
      final AMQPLargeMessage localCurrentMessage = currentMessage;

      // This method runs on the session thread and if the close is called and the scheduled file
      // delete occurs on the session thread first then current message will be null and we return.
      // But if the closed delete hasn't run first we can safely continue processing this message
      // in hopes we already read all the bytes before the connection was dropped.
      if (localCurrentMessage == null) {
         return;
      }

      final Receiver receiver = ((Receiver) delivery.getLink());
      final ReadableBuffer recieved = receiver.recv();

      // Store what we read into a composite as we may need to hold onto some or all of
      // the received data until a complete type is available.
      pendingRecvBuffer.addComponent(true, Unpooled.wrappedBuffer(recieved.byteBuffer()));

      final DecoderImpl decoder = TLSEncode.getDecoder();
      decoder.setBuffer(pendingReadable);

      try {
         while (pendingRecvBuffer.isReadable()) {
            pendingRecvBuffer.markReaderIndex();

            try {
               if (state == State.PROCESSING_MESSAGE_PREAMBLE) {
                  //tryReadHeadersAndProperties(pendingRecvBuffer);
               } else if (state == State.BODY_INFLATING) {
                  //tryReadMessageBody(delivery, pendingRecvBuffer);
               } else {
                  scanForNextMessageSection(decoder);
               }

               // Advance mark so read bytes can be discarded and we can start from this
               // location next time.
               pendingRecvBuffer.markReaderIndex();
            } catch (ActiveMQException ex) {
               throw ex;
            } catch (Exception e) {
               // We expect exceptions from proton when only partial section are received within
               // a frame so we will continue trying to decode until either we read a complete
               // section or we consume everything to the point of completing the delivery.
               if (delivery.isPartial()) {
                  pendingRecvBuffer.resetReaderIndex();
                  break; // Not enough data to decode yet.
               } else {
                  throw new ActiveMQAMQPInternalErrorException(
                     "Decoding error encounted in tunneled core large message.", e);
               }
            } finally {
               pendingRecvBuffer.discardReadComponents();
            }
         }

         if (!delivery.isPartial()) {
            if (currentMessage == null) {
               throw new ActiveMQAMQPInternalErrorException(
                  "Tunneled Core large message delivery contained no large message body.");
            }

            final Message result = currentMessage.toMessage();

            // We don't want a close to delete the file now, so we release these resources.
            currentMessage.releaseResources(serverReceiver.getConnection().isLargeMessageSync(), true);
            currentMessage = null;

            state = State.DONE;

            serverReceiver.onMessageComplete(delivery, result, deliveryAnnotations);
         }
      } catch (Throwable e) {
         serverReceiver.onExceptionWhileReading(e);
      } finally {
         decoder.setBuffer(null);
         serverReceiver.connection.runNow(serverReceiver.getConnection()::enableAutoRead);
      }

      try {
         localCurrentMessage.addBytes(dataBuffer);

         if (!isPartial) {
            localCurrentMessage.releaseResources(serverReceiver.getConnection().isLargeMessageSync(), true);
            // We don't want a close to delete the file now, we've released the resources.
            currentMessage = null;
            serverReceiver.connection.runNow(() -> serverReceiver.onMessageComplete(delivery, localCurrentMessage, localCurrentMessage.getDeliveryAnnotations()));
         }
      } catch (Throwable e) {
         serverReceiver.onExceptionWhileReading(e);
      } finally {
         serverReceiver.connection.runNow(serverReceiver.getConnection()::enableAutoRead);
      }
   }

   protected Binary scanForMessagePayload(Delivery delivery) {
      final Receiver receiver = ((Receiver) delivery.getLink());
      final ReadableBuffer recievedBuffer = receiver.recv();

      if (recievedBuffer.remaining() == 0) {
         throw new IllegalArgumentException("Received empty delivery when expecting a compressed AMQP message encoding");
      }

      final DecoderImpl decoder = TLSEncode.getDecoder();

      decoder.setBuffer(recievedBuffer);

      Data payloadData = null;

      try {
         while (recievedBuffer.hasRemaining()) {
            final TypeConstructor<?> constructor = decoder.readConstructor();

            if (Header.class.equals(constructor.getTypeClass())) {
               constructor.skipValue(); // Ignore for forward compatibility
            } else if (DeliveryAnnotations.class.equals(constructor.getTypeClass())) {
               deliveryAnnotations = (DeliveryAnnotations) constructor.readValue();
            } else if (MessageAnnotations.class.equals(constructor.getTypeClass())) {
               constructor.skipValue(); // Ignore for forward compatibility
            } else if (Properties.class.equals(constructor.getTypeClass())) {
               constructor.skipValue(); // Ignore for forward compatibility
            } else if (ApplicationProperties.class.equals(constructor.getTypeClass())) {
               constructor.skipValue(); // Ignore for forward compatibility
            } else if (Data.class.equals(constructor.getTypeClass())) {
               if (payloadData != null) {
                  throw new IllegalArgumentException("Received an unexpected additional Data section in compressed AMQP message");
               }

               payloadData = (Data) constructor.readValue();
            } else if (AmqpValue.class.equals(constructor.getTypeClass())) {
               throw new IllegalArgumentException("Received an AmqpValue payload in compressed AMQP message");
            } else if (AmqpSequence.class.equals(constructor.getTypeClass())) {
               throw new IllegalArgumentException("Received an AmqpSequence payload in compressed AMQP message");
            } else if (Footer.class.equals(constructor.getTypeClass())) {
               if (payloadData == null) {
                  throw new IllegalArgumentException("Received a Footer but no actual message payload in compressed AMQP message");
               }

               constructor.skipValue(); // Ignore for forward compatibility
            }
         }
      } finally {
         decoder.setBuffer(null);
      }

      if (payloadData == null) {
         throw new IllegalArgumentException("Did not receive a Data section payload in compressed AMQP message");
      }

      final Binary payloadBinary = payloadData.getValue();

      if (payloadBinary == null || payloadBinary.getLength() <= 0) {
         throw new IllegalArgumentException("Received an unexpected empty message payload in core tunneled AMQP message");
      }

      return payloadBinary;
   }

   protected ReadableBuffer inflateBufferFromBinary(Binary payloadBinary) throws Exception {
      final ByteBuffer input = payloadBinary.asByteBuffer();
      final ByteBuf output = Unpooled.buffer();

      if (input.hasArray()) {
         inflater.setInput(input.array(), input.arrayOffset(), input.remaining());
      } else {
         inflater.setInput(input);
      }

      while (!inflater.finished()) {
         int decompressedSize = inflater.inflate(scratchBuffer);
         output.writeBytes(scratchBuffer, 0, decompressedSize);
      }

      return new NettyReadable(output);
   }

   private void scanForNextMessageSection(DecoderImpl decoder) throws ActiveMQException {
      final TypeConstructor<?> constructor = decoder.readConstructor();

      if (Header.class.equals(constructor.getTypeClass())) {
         constructor.skipValue(); // Ignore for forward compatibility
      } else if (DeliveryAnnotations.class.equals(constructor.getTypeClass())) {
         deliveryAnnotations = (DeliveryAnnotations) constructor.readValue();
      } else if (MessageAnnotations.class.equals(constructor.getTypeClass())) {
         constructor.skipValue(); // Ignore for forward compatibility
      } else if (Properties.class.equals(constructor.getTypeClass())) {
         constructor.skipValue(); // Ignore for forward compatibility
      } else if (ApplicationProperties.class.equals(constructor.getTypeClass())) {
         constructor.skipValue(); // Ignore for forward compatibility
      } else if (Data.class.equals(constructor.getTypeClass())) {
         // Store how much we need to read before the Data section payload is consumed.
         final int dataSectionRemaining = readNextDataSectionSize(pendingReadable);

         if (state.ordinal() < State.PROCESSING_MESSAGE_PREAMBLE.ordinal()) {
            // coreHeadersBuffer = Unpooled.buffer(dataSectionRemaining, dataSectionRemaining);
            // largeMessageSectionRemaining = dataSectionRemaining;
            state = State.BODY_INFLATING;
         } else {
            throw new IllegalStateException("Data section found when not expecting any more input.");
         }
      } else if (AmqpValue.class.equals(constructor.getTypeClass())) {
         throw new IllegalArgumentException("Received an AmqpValue payload in compressed influght AMQP message");
      } else if (AmqpSequence.class.equals(constructor.getTypeClass())) {
         throw new IllegalArgumentException("Received an AmqpSequence payload in compressed inflight AMQP message");
      } else if (Footer.class.equals(constructor.getTypeClass())) {
         if (currentMessage == null) {
            throw new IllegalArgumentException("Received an Footer but no actual message paylod in compressed AMQP message");
         }

         constructor.skipValue(); // Ignore for forward compatibility
      }
   }

   // This reads the size for the encoded Binary that should follow a detected Data section and
   // leaves the buffer at the head of the payload of the Binary, readers from here should just
   // use the returned size to determine when all the binary payload is consumed.
   private static int readNextDataSectionSize(ReadableBuffer buffer) throws ActiveMQException {
      final byte encodingCode = buffer.get();

      return switch (encodingCode) {
         case EncodingCodes.VBIN8 -> buffer.get() & 0xFF;
         case EncodingCodes.VBIN32 -> buffer.getInt();
         case EncodingCodes.NULL -> 0;
         default -> throw new ActiveMQException("Expected Binary type but found encoding: " + encodingCode);
      };
   }
}
