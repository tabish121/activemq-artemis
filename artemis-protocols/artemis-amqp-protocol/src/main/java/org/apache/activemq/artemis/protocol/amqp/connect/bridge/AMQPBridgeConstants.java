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

import java.util.Map;

import org.apache.qpid.proton.amqp.Symbol;

/**
 * Constants class for values used in the AMQP Bridge implementation.
 */
public final class AMQPBridgeConstants {

   /**
    * Property value that can be applied to bridge configuration that controls the timeout value
    * for a link attach to complete before the attach attempt is considered to have failed. The value
    * is configured in seconds (default is 30 seconds).
    */
   public static final String LINK_ATTACH_TIMEOUT = "attach-timeout";

   /**
    * Configuration property that defines the amount of credits to batch to an AMQP receiver link
    * and the top up limit when sending more credit once the credits are determined to be running
    * low.
    */
   public static final String RECEIVER_CREDITS = "amqpCredits";

   /**
    * A low water mark for receiver credits that indicates more should be sent to top it up to the
    * original credit batch size.
    */
   public static final String RECEIVER_CREDITS_LOW = "amqpLowCredits";

   /**
    * Configuration property that defines the amount of credits to batch to an AMQP receiver link
    * and the top up value when sending more credit once the broker has capacity available for
    * them.
    */
   public static final String PULL_RECEIVER_BATCH_SIZE = "amqpPullConsumerCredits";

   /**
    * Configuration property used to convey the local side value to use when considering if a message
    * is a large message,
    */
   public static final String LARGE_MESSAGE_THRESHOLD = "minLargeMessageSize";

   /**
    * Configuration property used to convey the value to use when considering if bridged queue consumers
    * should filter using the filters defined on individual queue subscriptions. This can be used to prevent
    * multiple subscriptions on the same queue based on local demand with differing subscription filters
    * but does imply that message that don't match those filters would be bridged to the local broker. The
    * filters applied would be JMS filters which implies the remote needs to support JMS filters to apply
    * them to the created receiver instance.
    */
   public static final String IGNORE_QUEUE_CONSUMER_FILTERS = "ignoreQueueConsumerFilters";

   /**
    * Configuration property used to convey the value to use when considering if bridged queue consumers
    * should filter using the filters defined on individual queue definitions. The filters applied would
    * be JMS filters which implies the remote needs to support JMS filters to apply them to the created
    * receiver instance.
    */
   public static final String IGNORE_QUEUE_FILTERS = "ignoreQueueFilters";

   /**
    * Configuration property used to convey the value to use when considering if bridged queue consumers should
    * apply a consumer priority offset based on the subscription priority or should use a singular priority
    * offset based on policy configuration. This can be used to prevent multiple subscriptions on the same queue
    * based on local demand with differing consumer priorities but does imply that care needs to be taken to
    * ensure remote consumers would normally have a higher priority value than the configured default priority
    * offset.
    */
   public static final String IGNORE_QUEUE_CONSUMER_PRIORITIES = "ignoreQueueConsumerPriorities";

   /**
    * Property added to the receiver properties when opening an AMQP bridge address or queue consumer
    * that indicates the consumer priority that should be used when creating the remote consumer. The
    * value assign to the properties {@link Map} is a signed integer value.
    */
   public static final Symbol BRIDGE_RECEIVER_PRIORITY = Symbol.getSymbol("priority");

   /**
    * Default timeout value (in seconds) used to control when a link attach is considered to have
    * failed due to not responding to an attach request.
    */
   public static final int DEFAULT_LINK_ATTACH_TIMEOUT = 30;

   /**
    * Default credits granted to a receiver that is in pull mode.
    */
   public static final int DEFAULT_PULL_CREDIT_BATCH_SIZE = 10;

   /**
    * Default value for the core message tunneling feature that indicates if core protocol messages
    * should be streamed as binary blobs as the payload of an custom AMQP message which avoids any
    * conversions of the messages to / from AMQP.
    */
   public static final boolean DEFAULT_CORE_MESSAGE_TUNNELING_ENABLED = true;

   /**
    * Default value for the filtering applied to bridge Queue consumers that controls if
    * the filter specified by a consumer subscription is used or if the higher level Queue
    * filter only is applied when creating a bridge Queue consumer.
    */
   public static final boolean DEFAULT_IGNNORE_QUEUE_CONSUMER_FILTERS = false;

   /**
    * Default value for the filtering applied to bridge Queue consumers that controls if
    * the filter specified by a Queue is applied when creating a bridge Queue consumer.
    */
   public static final boolean DEFAULT_IGNNORE_QUEUE_FILTERS = false;

   /**
    * Default priority adjustment used for a bridge queue policy if no value was specific in the
    * broker configuration file.
    */
   public static final int DEFAULT_PRIORITY_ADJUSTMENT_VALUE = -1;

   /**
    * Default value for the priority applied to bridge queue receivers that controls if the
    * priority specified by a local consumer subscription is used or if the policy priority
    * offset value is simply applied to the default consumer priority value.
    */
   public static final boolean DEFAULT_IGNNORE_QUEUE_CONSUMER_PRIORITIES = false;

}
