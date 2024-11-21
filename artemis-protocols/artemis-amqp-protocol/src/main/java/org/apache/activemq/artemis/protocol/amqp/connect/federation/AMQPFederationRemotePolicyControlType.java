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

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanOperationInfo;
import javax.management.NotCompliantMBeanException;

import org.apache.activemq.artemis.core.management.impl.AbstractControl;
import org.apache.activemq.artemis.core.management.impl.MBeanInfoHelper;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.logs.AuditLogger;

/**
 * Management service control instance for an AMQPFederation policy manager instance that
 * originates from the remote broker. This control type appears to track the sending side
 * of a federation policy and its producer instances.
 */
public class AMQPFederationRemotePolicyControlType extends AbstractControl implements AMQPFederationRemotePolicyControl {

   private final AMQPFederationSenderController sender;
   private final String policyName;

   public AMQPFederationRemotePolicyControlType(String policyName, ActiveMQServer server, AMQPFederationSenderController sender) throws NotCompliantMBeanException {
      super(AMQPFederationRemotePolicyControl.class, server.getStorageManager());

      this.sender = sender;
      this.policyName = policyName;
   }

   @Override
   public String getType() {
      if (AuditLogger.isBaseLoggingEnabled()) {
         AuditLogger.getType(this.sender);
      }
      clearIO();
      try {
         return ""; // TODO
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getName() {
      if (AuditLogger.isBaseLoggingEnabled()) {
         AuditLogger.getName(this.sender);
      }
      clearIO();
      try {
         return policyName;
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getMessagesSent() {
      if (AuditLogger.isBaseLoggingEnabled()) {
         AuditLogger.getMessagesReceived(this.sender);
      }
      clearIO();
      try {
         return 0; // TODO
      } finally {
         blockOnIO();
      }
   }

   @Override
   protected MBeanOperationInfo[] fillMBeanOperationInfo() {
      return MBeanInfoHelper.getMBeanOperationsInfo(AMQPFederationRemotePolicyControl.class);
   }

   @Override
   protected MBeanAttributeInfo[] fillMBeanAttributeInfo() {
      return MBeanInfoHelper.getMBeanAttributesInfo(AMQPFederationRemotePolicyControl.class);
   }
}
