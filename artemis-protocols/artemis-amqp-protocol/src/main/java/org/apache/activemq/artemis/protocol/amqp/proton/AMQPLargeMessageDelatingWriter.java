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

import static org.apache.activemq.artemis.protocol.amqp.proton.AMQPArtemisMessageFormats.AMQP_COMPRESSED_MESSAGE_FORMAT;

import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.zip.Deflater;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.message.LargeBodyReader;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPLargeMessage;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessageBrokerAccessor;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPSessionCallback;
import org.apache.activemq.artemis.protocol.amqp.broker.ActiveMQProtonRemotingConnection;
import org.apache.activemq.artemis.protocol.amqp.util.NettyReadable;
import org.apache.activemq.artemis.protocol.amqp.util.NettyWritable;
import org.apache.activemq.artemis.protocol.amqp.util.TLSEncode;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.proton.codec.EncodingCodes;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.codec.WritableBuffer;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;

/**
 * A writer of {@link AMQPLargeMessage} content that handles the read from large message file and write into the AMQP
 * sender with some respect for the AMQP frame size in use by this connection.
 */
public class AMQPLargeMessageDelatingWriter implements MessageWriter {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static final byte DATA_DESCRIPTOR = 0x75;
   private static final int DATA_SECTION_SIZE_OFFSET = 4;
   private static final int DATA_SECTION_ENCODING_BYTES = Long.BYTES;

   private enum State {
      /**
       * Writing the optional AMQP Header which is needed for the receiver to create the large message.
       */
      STREAMING_HEADER,
      /**
       * Writing the optional AMQP delivery annotations which can provide additional context.
       */
      STREAMING_DELIVERY_ANNOTATIONS,
      /**
       * Encode Header and then all non-body section if the message is marked as re-encoded
       */
      REENCODE_MESSAGE_PREAMBLE,
      /**
       * Writing the possibly re-encoded message sections using with compression
       */
      STREAMING_REENCODED_SECTIONS,
      /**
       * Writing the message payload using with compression
       */
      STREAMING_MESSAGE_FROM_FILE,
      /**
       * Done writing, no more bytes will be written.
       */
      DONE,
      /**
       * The writer is closed and cannot be used again until open is called.
       */
      CLOSED
   }

   private final ProtonServerSenderContext serverSender;
   private final AMQPConnectionContext connection;
   private final AMQPSessionCallback sessionSPI;
   private final Sender protonSender;
   private final Deflater deflater = new Deflater();
   private final byte[] scratchBuffer = new byte[1024];

   private MessageReference reference;
   private AMQPLargeMessage message;
   private LargeBodyReader largeBodyReader;
   private DeliveryAnnotations annotations;
   private Delivery delivery;
   private ByteBuf stagingBuffer;
   private long position;
   private int frameSize;
   private State state = State.CLOSED;

   public AMQPLargeMessageDelatingWriter(ProtonServerSenderContext serverSender) {
      this.serverSender = serverSender;
      this.connection = serverSender.getSessionContext().getAMQPConnectionContext();
      this.sessionSPI = serverSender.getSessionContext().getSessionSPI();
      this.protonSender = serverSender.getSender();
   }

   @Override
   public boolean isWriting() {
      return state != State.CLOSED;
   }

   @Override
   public void close() {
      if (state != State.CLOSED) {
         try {
            try {
               if (largeBodyReader != null) {
                  largeBodyReader.close();
               }
            } catch (Exception e) {
               logger.warn("Error on close of large body reader:{}", e.getMessage(), e);
            }

            if (message != null) {
               message.usageDown();
            }
         } finally {
            resetClosed();
         }
      }
   }

   @Override
   public AMQPLargeMessageDelatingWriter open(MessageReference reference) {
      if (state != State.CLOSED) {
         throw new IllegalStateException("Trying to open an AMQP Large Message writer that was not closed");
      }

      this.reference = reference;
      this.message = (AMQPLargeMessage) reference.getMessage();
      this.message.usageUp();

      try {
         largeBodyReader = message.getLargeBodyReader();
         largeBodyReader.open();
      } catch (Exception e) {
         serverSender.reportDeliveryError(this, reference, e);
      }

      resetOpen();

      return this;
   }

   private void resetClosed() {
      message = null;
      reference = null;
      delivery = null;
      largeBodyReader = null;
      position = 0;
      stagingBuffer = null;
      state = State.CLOSED;
      deflater.reset();
   }

   private void resetOpen() {
      position = 0;
      state = State.STREAMING_DELIVERY_ANNOTATIONS;
   }

   @Override
   public void writeBytes(MessageReference messageReference) {
      if (protonSender.getLocalState() == EndpointState.CLOSED) {
         logger.debug("Not delivering message {} as the sender is closed and credits were available, if you see too many of these it means clients are issuing credits and closing the connection with pending credits a lot of times", messageReference);
         return;
      }

      if (state == State.CLOSED) {
         throw new IllegalStateException("Cannot write to an AMQP Large Message Writer that has been closed");
      }

      if (state == State.DONE) {
         throw new IllegalStateException(
            "Cannot write to an AMQP Large Message Writer that was already used to write a message and was not reset");
      }

      if (sessionSPI.invokeOutgoing(message, (ActiveMQProtonRemotingConnection) sessionSPI.getTransportConnection().getProtocolConnection()) != null) {
         // an interceptor rejected the delivery
         // since we opened the message as part of the queue executor we must close it now
         close();
         return;
      }

      // This will either return the delivery annotations in the reference, or will return any annotations
      // set it the now deprecated annotations for send instance in the message itself.
      message.checkReference(reference);

      annotations = reference.getProtocolData(DeliveryAnnotations.class);
      delivery = serverSender.createDelivery(messageReference, AMQP_COMPRESSED_MESSAGE_FORMAT);
      frameSize = protonSender.getSession().getConnection().getTransport().getOutboundFrameSizeLimit() - 50 - (delivery.getTag() != null ? delivery.getTag().length : 0);

      tryDelivering();
   }

   /**
    * Used to provide re-entry from the flow control executor when IO back-pressure has eased
    */
   private void resume() {
      connection.runLater(this::tryDelivering);
   }

   private void tryDelivering() {
      if (state == State.CLOSED) {
         logger.trace("AMQP Large Message Writer was closed before queued write attempt was executed");
         return;
      }

      final ByteBuf frameBuffer = PooledByteBufAllocator.DEFAULT.directBuffer(frameSize, frameSize);
      final NettyReadable frameView = new NettyReadable(frameBuffer);

      try {
         // In order to treat the internal buffer as a NIO buffer in proton we need
         // to allocate the full size up front otherwise we run into trouble.
         frameBuffer.ensureWritable(frameSize);

         switch (state) {
            case STREAMING_HEADER:
               if (!trySendMessageHeader(frameBuffer, frameView)) {
                  return;
               }
            case STREAMING_DELIVERY_ANNOTATIONS:
               if (!trySendDeliveryAnnotations(frameBuffer, frameView)) {
                  return;
               }
            case REENCODE_MESSAGE_PREAMBLE:
               position = doReencodeMessagePreamble();
            case STREAMING_REENCODED_SECTIONS:
               if (!tryStreamReencodedSections(frameBuffer, frameView)) {
                  return;
               }
            case STREAMING_MESSAGE_FROM_FILE:
               if (!tryStreamMessageFromFile(frameBuffer, frameView)) {
                  return;
               }

               break;
            default:
               throw new IllegalStateException("The writer already wrote a message and was not reset");
         }

         serverSender.reportDeliveryComplete(this, reference, delivery, true);
      } catch (Exception deliveryError) {
         serverSender.reportDeliveryError(this, reference, deliveryError);
      }
   }

   private boolean trySendMessageHeader(ByteBuf frameBuffer, NettyReadable frameView) {
      if (protonSender.getLocalState() != EndpointState.CLOSED && state == State.STREAMING_HEADER) {
         final Header header = AMQPMessageBrokerAccessor.getCurrentHeader(message);

         if (header != null) {
            TLSEncode.getEncoder().writeObject(new NettyWritable(frameBuffer));
         }

         state = State.STREAMING_DELIVERY_ANNOTATIONS;
      }

      return state == State.STREAMING_DELIVERY_ANNOTATIONS;
   }

   // Will return true when the optional delivery annotations are fully sent or are not present, and false
   // if not able to send due to a flow control event.
   private boolean trySendDeliveryAnnotations(ByteBuf frameBuffer, NettyReadable frameView) {
      for (; protonSender.getLocalState() != EndpointState.CLOSED && state == State.STREAMING_DELIVERY_ANNOTATIONS; ) {
         if (annotations != null && annotations.getValue() != null && !annotations.getValue().isEmpty()) {
            if (isFlowControlled(frameBuffer, frameView)) {
               break; // Resume will restart writing from where we left off.
            }

            final ByteBuf annotationsBuffer = getOrCreateDeliveryAnnotationsBuffer();
            final int readSize = Math.min(frameBuffer.writableBytes(), annotationsBuffer.readableBytes());

            annotationsBuffer.readBytes(frameBuffer, readSize);

            // In case the Delivery Annotations encoding exceed the AMQP frame size we
            // flush and keep sending until done or until flow controlled.
            if (!frameBuffer.isWritable()) {
               protonSender.send(frameView);
               frameBuffer.clear();
               connection.instantFlush();
            }

            if (!annotationsBuffer.isReadable()) {
               state = State.REENCODE_MESSAGE_PREAMBLE;
               stagingBuffer.clear();
            }
         } else {
            state = State.REENCODE_MESSAGE_PREAMBLE;
         }
      }

      return state == State.REENCODE_MESSAGE_PREAMBLE;
   }

   private int doReencodeMessagePreamble() {
      final Header header = AMQPMessageBrokerAccessor.getCurrentHeader(message);

      if (header == null && !message.isReencoded()) {
         // Nothing needs to be written here, skip and start after delivery annotations.
         return message.getPositionAfterDeliveryAnnotations();
      }

      final EncoderImpl encoder = TLSEncode.getEncoder();
      final ByteBuf encodingBuffer = PooledByteBufAllocator.DEFAULT.directBuffer();

      try {
         encoder.setByteBuffer(new NettyWritable(encodingBuffer));

         // Header is always encoded here and skipped in the large body file along with any
         // delivery annotations sent from the client.
         if (header != null) {
            encoder.writeObject(header);
         }

         // Delivery Annotations are written ahead of the Data section wrapper that contains
         // the compressed message so we skip it here.

         if (message.isReencoded()) {
            final MessageAnnotations messageAnnotations = AMQPMessageBrokerAccessor.getDecodedMessageAnnotations(message);
            final Properties messageProperties = AMQPMessageBrokerAccessor.getCurrentProperties(message);
            final ApplicationProperties applicationProperties = AMQPMessageBrokerAccessor.getDecodedApplicationProperties(message);

            if (messageAnnotations != null) {
               encoder.writeObject(messageAnnotations);
            }

            if (messageProperties != null) {
               encoder.writeObject(messageProperties);
            }

            if (applicationProperties != null) {
               encoder.writeObject(applicationProperties);
            }
         }

         // If there were no delivery annotations the buffer wouldn't have been allocated.
         if (stagingBuffer == null) {
            stagingBuffer = Unpooled.buffer();
         }

         writeDataSectionTypeInfo(stagingBuffer, 0);

         deflater.setInput(encodingBuffer.nioBuffer());
         deflater.finish();

         int compressedBytes = 0;
         int stepResult = 0;

         while (!deflater.finished()) {
            compressedBytes += stepResult = deflater.deflate(scratchBuffer);
            stagingBuffer.writeBytes(scratchBuffer, 0, stepResult);
         }

         deflater.reset();

         // Update the Data section header with the result of the deflate operation
         stagingBuffer.setInt(DATA_SECTION_SIZE_OFFSET, compressedBytes);

         return message.isReencoded() ? AMQPMessageBrokerAccessor.getRemainingBodyPosition(message) :
                                        message.getPositionAfterDeliveryAnnotations();
      } finally {
         encoder.setByteBuffer((WritableBuffer) null);
         encodingBuffer.release();
      }
   }

   // Should return true whenever the deflated message preamble have been fully written and false otherwise
   // so that more writes can be attempted after flow control allows it.
   private boolean tryStreamReencodedSections(ByteBuf frameBuffer, NettyReadable frameView) {
      for (; protonSender.getLocalState() != EndpointState.CLOSED && state == State.STREAMING_REENCODED_SECTIONS; ) {
         if (stagingBuffer != null && stagingBuffer.isReadable()) {
            if (isFlowControlled(frameBuffer, frameView)) {
               break; // Resume will restart writing from where we left off.
            }

            final int readSize = Math.min(frameBuffer.writableBytes(), stagingBuffer.readableBytes());

            stagingBuffer.readBytes(frameBuffer, readSize);

            // In case the message preamble encoding exceed the AMQP frame size we flush and keep sending
            // until done or until flow controlled. Then when all encoded bytes are written we flush the
            // frame buffer one last time so that streaming of the large body can start with a fresh frame
            // buffer since we need to encode the bytes one Data section at a time as we deflate the contents
            // of the large body file.
            final boolean completed = !stagingBuffer.isReadable();

            if (!frameBuffer.isWritable() || completed) {
               protonSender.send(frameView);
               frameBuffer.clear();
               connection.instantFlush();

               if (completed) {
                  state = State.STREAMING_MESSAGE_FROM_FILE;
                  stagingBuffer = null;
               }
            }
         } else {
            state = State.STREAMING_MESSAGE_FROM_FILE;
         }
      }

      return state == State.STREAMING_MESSAGE_FROM_FILE;
   }

   // Should return true whenever the deflated message contents have been fully written and false otherwise
   // so that more writes can be attempted after flow control allows it.
   private boolean tryStreamMessageFromFile(ByteBuf frameBuffer, NettyReadable frameView) throws ActiveMQException {
      try (LargeBodyReader context = message.getLargeBodyReader()) {
         context.open();
         context.position(position);

         final long bodySize = largeBodyReader.getSize();
         final ByteBuffer readBuffer = ByteBuffer.allocate(frameSize);

         for (; protonSender.getLocalState() != EndpointState.CLOSED && state == State.STREAMING_MESSAGE_FROM_FILE; ) {
            if (isFlowControlled(frameBuffer, frameView)) {
               break; // Resume will restart writing from where we left off.
            }

            // Each frame must be one Data section as we won't know the outcome of the deflate
            // until we fill the frame or reach the end of the file and we need to write the
            // data section size prior to sending the frame.
            writeDataSectionTypeInfo(frameBuffer, 0);

            long remainingBodySize = bodySize - position;

            while (frameBuffer.isWritable() && !deflater.finished()) {
               // There could be some input left over from a previous loop if we couldn't fit
               // the last read completely into the frame buffer during deflate processing..
               if (deflater.needsInput()) {
                  readBuffer.clear();

                  final int readSize = largeBodyReader.readInto(readBuffer);

                  position += readSize;
                  remainingBodySize -= readSize;

                  deflater.setInput(readBuffer);

                  if (remainingBodySize == 0) {
                     deflater.finish();
                  }
               }

               final int deflateRequest = Math.min(scratchBuffer.length, frameBuffer.writableBytes());
               final int deflateResult = deflater.deflate(scratchBuffer, 0, deflateRequest);

               // TODO: Internal NIO buffer might give a more direct path to capturing the bytes
               //frameBuffer.internalNioBuffer(deflateRequest, deflateResult);

               frameBuffer.writeBytes(scratchBuffer, 0, deflateResult);
            }

            // Before sending the frame buffer write the outcome into the Data section size entry.
            frameBuffer.setInt(DATA_SECTION_SIZE_OFFSET, frameBuffer.readableBytes() - DATA_SECTION_ENCODING_BYTES);

            protonSender.send(frameView);
            frameBuffer.clear();
            connection.instantFlush();

            if (deflater.finished()) {
               state = State.DONE;
            }
         }

         return state == State.DONE;
      }
   }

   private ByteBuf getOrCreateDeliveryAnnotationsBuffer() {
      if (stagingBuffer == null) {
         stagingBuffer = Unpooled.buffer();

         final EncoderImpl encoder = TLSEncode.getEncoder();

         try {
            encoder.setByteBuffer(new NettyWritable(stagingBuffer));
            encoder.writeObject(annotations);
         } finally {
            encoder.setByteBuffer((WritableBuffer) null);
         }
      }

      return stagingBuffer;
   }

   private boolean isFlowControlled(ByteBuf frameBuffer, ReadableBuffer frameView) {
      if (!connection.flowControl(this::resume)) {
         if (frameBuffer.isReadable()) {
            protonSender.send(frameView); // Store pending work in the sender for later flush.
         }
         return true;
      } else {
         return false;
      }
   }

   private void writeDataSectionTypeInfo(ByteBuf buffer, int encodedSize) {
      buffer.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
      buffer.writeByte(EncodingCodes.SMALLULONG);
      buffer.writeByte(DATA_DESCRIPTOR);
      buffer.writeByte(EncodingCodes.VBIN32);
      buffer.writeInt(encodedSize); // Core message will encode into this size.
   }
}
