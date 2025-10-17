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

import static org.apache.activemq.artemis.protocol.amqp.proton.AMQPArtemisMessageFormats.AMQP_COMPRESSED_TUNNELED_CORE_LARGE_MESSAGE_FORMAT;

import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.zip.Deflater;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.message.LargeBodyReader;
import org.apache.activemq.artemis.core.persistence.impl.journal.LargeServerMessageImpl;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPLargeMessageDelatingWriter.State;
import org.apache.activemq.artemis.protocol.amqp.util.NettyReadable;
import org.apache.activemq.artemis.protocol.amqp.util.NettyWritable;
import org.apache.activemq.artemis.protocol.amqp.util.TLSEncode;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
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
 * Writer of tunneled large Core messages that will be written as the body of an AMQP delivery with a custom message
 * format that indicates this payload. The writer will read bytes from the Core large message file and write them into
 * an AMQP Delivery that will be sent across to the remote peer where it can be processed and a Core message recreated
 * for dispatch as if it had been sent from a Core connection.
 */
public class AMQPTunneledCoreLargeMessageDeflatingWriter implements MessageWriter {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static final byte DATA_DESCRIPTOR = 0x75;
   private static final int DATA_SECTION_ENCODING_BYTES = Long.BYTES;
   private static final int DATA_SECTION_SIZE_OFFSET = 4;
   private static final int FRAME_BUFFER_LOW_WATER_MARK = 64;

   private enum State {
      /**
       * Writing the optional AMQP delivery annotations which can provide additional context.
       */
      STREAMING_DELIVERY_ANNOTATIONS,
      /**
       * Writing the optional AMQP delivery annotations which can provide additional context falling
       * back to an encode into memory and chunked writes into the frame buffer and then send.
       */
      STREAMING_DELIVERY_ANNOTATIONS_FALLBACK,
      /**
       * Writing the core message headers and properties that describe the message.
       */
      STREAMING_CORE_HEADERS,
      /**
       * Writing the actual message payload from the large message file.
       */
      STREAMING_MESSAGE_CONTENTS,
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
   private final Sender protonSender;
   private final Deflater deflater = new Deflater();

   private MessageReference reference;
   private LargeServerMessageImpl message;
   private LargeBodyReader largeBodyReader;
   private Delivery delivery;
   private ByteBuf inflightBuffer;
   private int frameSize;
   private long position;

   private volatile State state = State.CLOSED;

   public AMQPTunneledCoreLargeMessageDeflatingWriter(ProtonServerSenderContext serverSender) {
      this.serverSender = serverSender;
      this.connection = serverSender.getSessionContext().getAMQPConnectionContext();
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
            reset(State.CLOSED);
         }
      }
   }

   @Override
   public AMQPTunneledCoreLargeMessageDeflatingWriter open(MessageReference messageReference) {
      if (state != State.CLOSED) {
         throw new IllegalStateException("Trying to open an AMQP Large Message writer that was not closed");
      }

      reset(State.STREAMING_DELIVERY_ANNOTATIONS);

      reference = messageReference;
      message = (LargeServerMessageImpl) messageReference.getMessage();
      message.usageUp();

      try {
         largeBodyReader = message.getLargeBodyReader();
         largeBodyReader.open();
      } catch (Exception e) {
         serverSender.reportDeliveryError(this, reference, e);
      }

      return this;
   }

   private void reset(State newState) {
      message = null;
      reference = null;
      delivery = null;
      position = 0;
      state = newState;
      inflightBuffer = null;
      largeBodyReader = null;
      deflater.reset();
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

      delivery = serverSender.createDelivery(messageReference, AMQP_COMPRESSED_TUNNELED_CORE_LARGE_MESSAGE_FORMAT);
      frameSize = protonSender.getSession().getConnection().getTransport().getOutboundFrameSizeLimit() - 50 - (delivery.getTag() != null ? delivery.getTag().length : 0);

      tryDelivering();
   }

   /**
    * Used to provide re-entry from the flow control executor when IO back-pressure has eased
    */
   private void resume() {
      connection.runNow(this::tryDelivering);
   }

   private void tryDelivering() {
      if (state == State.CLOSED) {
         logger.trace("AMQP Core Large Message deflating Writer was closed before queued write attempt was executed");
         return;
      }

      final ByteBuf frameBuffer = PooledByteBufAllocator.DEFAULT.directBuffer(frameSize, frameSize);
      final NettyReadable frameView = new NettyReadable(frameBuffer);
      final EncoderImpl encoder = TLSEncode.getEncoder();

      encoder.setByteBuffer(new NettyWritable(frameBuffer));

      try {
         // In order to treat the internal buffer as a NIO buffer in proton we need
         // to allocate the full size up front otherwise we run into trouble.
         frameBuffer.ensureWritable(frameSize);

         switch (state) {
            case STREAMING_DELIVERY_ANNOTATIONS:
               if (!trySendDeliveryAnnotations(encoder, frameBuffer, frameView)) {
                  return;
               }
            case STREAMING_DELIVERY_ANNOTATIONS_FALLBACK:
               if (!trySendDeliveryAnnotationsFallback(encoder, frameBuffer, frameView)) {
                  return;
               }
            case STREAMING_CORE_HEADERS:
               if (!trySendHeadersAndProperties(frameBuffer, frameView)) {
                  return;
               }
            case STREAMING_MESSAGE_CONTENTS:
               if (!tryStreamMessageContents(frameBuffer, frameView)) {
                  return;
               }

               serverSender.reportDeliveryComplete(this, reference, delivery, true);
               break;
            default:
               throw new IllegalStateException("The writer already wrote a message and was not reset");
         }
      } catch (Exception deliveryError) {
         encoder.setByteBuffer((NettyWritable) null);
         if (inflightBuffer != null) {
            inflightBuffer.release();
            inflightBuffer = null;
         }
         serverSender.reportDeliveryError(this, reference, deliveryError);
      } finally {
         frameBuffer.release();
      }
   }

   // Will return true when the optional delivery annotations are fully sent or are not present, and false
   // if the state of the sender prevent the write of the delivery annotations.
   private boolean trySendDeliveryAnnotations(EncoderImpl encoder, ByteBuf frameBuffer, NettyReadable frameView) {
      if (state == State.STREAMING_DELIVERY_ANNOTATIONS && protonSender.getLocalState() != EndpointState.CLOSED) {
         DeliveryAnnotations annotations = reference.getProtocolData(DeliveryAnnotations.class);

         if (annotations != null && annotations.getValue() != null && !annotations.getValue().isEmpty()) {
            final int savedWriterIndex = frameBuffer.writerIndex();

            try {
               encoder.writeObject(annotations);
            } catch (IndexOutOfBoundsException e) {
               frameBuffer.writerIndex(savedWriterIndex);
               state = State.STREAMING_DELIVERY_ANNOTATIONS_FALLBACK;
            }
         }

         state = State.STREAMING_CORE_HEADERS;
      }

      return state != State.STREAMING_DELIVERY_ANNOTATIONS;
   }

   // Handles the case where delivery annotations cannot be encoded into the frame buffer limits and must be
   // encoded into a staging buffer and written in chunks. The method returns true once the staging buffer is
   // cleared possibly leaving some bytes in the frame buffer for the next operation to complete. This method
   // will skip running if the previous stage moved the state past the fallback step.
   private boolean trySendDeliveryAnnotationsFallback(EncoderImpl encoder, ByteBuf frameBuffer, NettyReadable frameView) {
      for (; state == State.STREAMING_DELIVERY_ANNOTATIONS_FALLBACK && protonSender.getLocalState() != EndpointState.CLOSED; ) {
         DeliveryAnnotations annotations = reference.getProtocolData(DeliveryAnnotations.class);

         if (annotations != null && annotations.getValue() != null && !annotations.getValue().isEmpty()) {
            if (isFlowControlled(frameBuffer, frameView)) {
               break; // Resume will restart writing from where we left off.
            }

            final ByteBuf annotationsBuffer = getOrCreateAnnotationsBuffer(encoder, annotations);
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
               state = State.STREAMING_CORE_HEADERS;
               annotationsBuffer.clear();
            }
         } else {
            state = State.STREAMING_CORE_HEADERS;
         }
      }

      return state != State.STREAMING_DELIVERY_ANNOTATIONS_FALLBACK;
   }

   // TODO: Headers buffer handling and compression in its own frame.

   // Will return true when the header was fully sent false if not all the header
   // data could be sent due to a flow control event.
   private boolean trySendHeadersAndProperties(ByteBuf frameBuffer, NettyReadable frameView) {
      for (; protonSender.getLocalState() != EndpointState.CLOSED && state == State.STREAMING_CORE_HEADERS; ) {
         if (isFlowControlled(frameBuffer, frameView)) {
            break; // Resume will restart writing the headers section from where we left off.
         }

         final ByteBuf headerBuffer = getOrCreateMessageHeaderBuffer();
         final int readSize = Math.min(frameBuffer.writableBytes(), headerBuffer.readableBytes());

         headerBuffer.readBytes(frameBuffer, readSize);

         // In case the Core message header and properties exceed the AMQP frame size we
         // flush and keep sending until done or until flow controlled.
         if (!frameBuffer.isWritable()) {
            protonSender.send(frameView);
            frameBuffer.clear();
            connection.instantFlush();
         }

         if (!headerBuffer.isReadable()) {
            encodingBuffer = null;
            state = State.STREAMING_MESSAGE_CONTENTS;
         }
      }

      return state == State.STREAMING_MESSAGE_CONTENTS;
   }

   // Should return true whenever the deflated message contents have been fully written and false otherwise
   // so that more writes can be attempted after flow control allows it.
   private boolean tryStreamMessageContents(ByteBuf frameBuffer, NettyReadable frameView) throws ActiveMQException {
      largeBodyReader.position(position);

      final long bodySize = largeBodyReader.getSize();
      final ByteBuf readBuffer = getOrCreateInflightBuffer();

      for (; protonSender.getLocalState() != EndpointState.CLOSED && state == State.STREAMING_MESSAGE_CONTENTS; ) {
         if (isFlowControlled(frameBuffer, frameView)) {
            break; // Resume will restart writing from where we left off.
         }

         // If the free space drops to low we flush the pending frame and start fresh in order to more
         // efficiently write the compressed body and avoid a case where only the space remaining to
         // write the Data section header remains as we need won't know the section size value until
         // the bytes are compressed.
         if (frameBuffer.writableBytes() < FRAME_BUFFER_LOW_WATER_MARK) {
            protonSender.send(frameView);
            frameBuffer.clear();
            connection.instantFlush();
         }

         // Each frame must be one Data section as we won't know the outcome of the deflate
         // until we fill the frame or reach the end of the file and we need to write the
         // data section size prior to sending the frame.
         final int frameBufferSizeIndex = writeDataSectionTypeInfo(frameBuffer);
         final int dataStartIndex = frameBuffer.writerIndex();

         long remainingBodySize = bodySize - position;

         while (frameBuffer.isWritable() && !deflater.finished()) {
            // There could be some input left over from a previous loop if we couldn't fit
            // the last read completely into the frame buffer during deflate processing.

            if (deflater.needsInput()) {
               int fileReadSize = 0;

               // There may be space left from previous stages that (re)encoded message sections
               // into the inflight buffer that holds the bytes that need to be compressed here.
               // We can also check and truncate the inflight buffer if a previous stage needed to
               // over-provision that buffer in order to fit the full encoding of a section.
               if (readBuffer.readableBytes() < frameSize) {
                  if (readBuffer.capacity() > frameSize) {
                     readBuffer.capacity(frameSize);
                  }

                  fileReadSize = largeBodyReader.readInto(
                     readBuffer.internalNioBuffer(readBuffer.writerIndex(), readBuffer.writableBytes()));

                  readBuffer.writerIndex(readBuffer.writerIndex() + fileReadSize);
               }

               deflater.setInput(readBuffer.internalNioBuffer(readBuffer.readerIndex(), readBuffer.readableBytes()));

               readBuffer.clear(); // We've handed the view off so reset for future file read.

               // Mark the read bytes as consumed since they stay in the deflate handler
               // until fully consumed and we read another chunk.
               position += fileReadSize;
               remainingBodySize -= fileReadSize;

               if (remainingBodySize == 0) {
                  deflater.finish();
               }
            }

            final int deflateResult = deflater.deflate(
               frameBuffer.internalNioBuffer(frameBuffer.writerIndex(), frameBuffer.writableBytes()));

            frameBuffer.writerIndex(frameBuffer.writerIndex() + deflateResult);
         }

         // Before sending the frame buffer write the outcome into the Data section size entry.
         frameBuffer.setInt(frameBufferSizeIndex, frameBuffer.writerIndex() - dataStartIndex);

         protonSender.send(frameView);
         frameBuffer.clear();

         if (deflater.finished()) {
            // We can defer to the on complete handler here which can avoid an empty Transfer frame being
            // written just to indicate no more data is incoming for this delivery.
            connection.flush();
            state = State.DONE;
         } else {
            connection.instantFlush();
         }
      }

      return state == State.DONE;
   }

   /*
    * Delivery annotations are written without compression to allow for addition of annotations
    * specific to the compressed body to follow.
    */
   private ByteBuf getOrCreateAnnotationsBuffer(EncoderImpl encoder, DeliveryAnnotations annotations) {
      if (inflightBuffer == null) {
         final WritableBuffer oldFrameBuffer = encoder.getBuffer();

         inflightBuffer = PooledByteBufAllocator.DEFAULT.directBuffer(frameSize);

         // In order to access the buffer internal NIO buffer for some operations
         // we realize the capacity we know we want now. This could grow if the data
         // sections encoded into it exceed the frame size which we will have to deal
         // with once we start writing message payload to the frame buffer.
         inflightBuffer.ensureWritable(frameSize);

         try {
            encoder.setByteBuffer(new NettyWritable(inflightBuffer));
            encoder.writeObject(annotations);
         } finally {
            encoder.setByteBuffer(oldFrameBuffer);
         }
      }

      return inflightBuffer;
   }

   private ByteBuf getOrCreateMessageHeaderBuffer() {
      if (inflightBuffer == null) {
         final int encodedSize = message.getHeadersAndPropertiesEncodeSize();
         final ByteBuf sourceBuffer = Unpooled.buffer(encodedSize, encodedSize);

         message.encodeHeadersAndProperties(sourceBuffer);

         // Create a space to deflate the encoded header and properties with space for the Data section header
         inflightBuffer = PooledByteBufAllocator.DEFAULT.directBuffer(frameSize);
         writeDataSectionTypeInfo(inflightBuffer, 0);

         deflater.setInput(sourceBuffer.nioBuffer());
         deflater.finish();

         int compressedBytes = 0;
         int stepResult = 0;

         while (!deflater.finished()) {
            compressedBytes += stepResult = deflater.deflate(scratchBuffer);
            encodingBuffer.writeBytes(scratchBuffer, 0, stepResult);
         }

         deflater.reset();

         // Update the Data section header with the result of the deflate operation
         encodingBuffer.setInt(DATA_SECTION_SIZE_OFFSET, compressedBytes);
      }

      return encodingBuffer;
   }

   private ByteBuf getOrCreateInflightBuffer() {
      if (inflightBuffer == null) {
         inflightBuffer = PooledByteBufAllocator.DEFAULT.directBuffer(frameSize);
      }

      return inflightBuffer;
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

   private int writeDataSectionTypeInfo(ByteBuf buffer) {
      buffer.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
      buffer.writeByte(EncodingCodes.SMALLULONG);
      buffer.writeByte(DATA_DESCRIPTOR);
      buffer.writeByte(EncodingCodes.VBIN32);
      buffer.writeInt(0);

      return buffer.writerIndex() - DATA_SECTION_SIZE_OFFSET;
   }
}
