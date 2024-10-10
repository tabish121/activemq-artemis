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

import org.apache.activemq.artemis.api.core.management.BrokerConnectionServiceControl;
import org.apache.activemq.artemis.core.management.impl.AbstractControl;
import org.apache.activemq.artemis.core.management.impl.MBeanInfoHelper;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.server.BrokerConnection;
import org.apache.activemq.artemis.logs.AuditLogger;

/**
 * Management service control for an AMQPFederation instance.
 */
public class AMQPFederationAddressPolicyControl extends AbstractControl implements BrokerConnectionServiceControl {

   public static final String SERVICE_TYPE = "federation";

   private final AMQPFederation federation;

   public AMQPFederationAddressPolicyControl(AMQPFederation federation, BrokerConnection brokerConnection, StorageManager storageManager) throws NotCompliantMBeanException {
      super(BrokerConnectionServiceControl.class, storageManager);

      this.federation = federation;
   }

   @Override
   public boolean isStarted() {
      if (AuditLogger.isBaseLoggingEnabled()) {
         AuditLogger.isStarted(this.federation);
      }
      clearIO();
      try {
         return federation.isStarted();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void start() throws Exception {
      if (AuditLogger.isBaseLoggingEnabled()) {
         AuditLogger.startBrokerConnectionService(federation.getName());
      }
      clearIO();
      try {
         federation.start();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void stop() throws Exception {
      if (AuditLogger.isBaseLoggingEnabled()) {
         AuditLogger.stopBrokerConnection(federation.getName());
      }
      clearIO();
      try {
         federation.stop();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getType() {
      if (AuditLogger.isBaseLoggingEnabled()) {
         AuditLogger.getType(this.federation);
      }
      clearIO();
      try {
         return SERVICE_TYPE;
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getName() {
      if (AuditLogger.isBaseLoggingEnabled()) {
         AuditLogger.getName(this.federation);
      }
      clearIO();
      try {
         return federation.getName();
      } finally {
         blockOnIO();
      }
   }

   @Override
   protected MBeanOperationInfo[] fillMBeanOperationInfo() {
      return MBeanInfoHelper.getMBeanOperationsInfo(BrokerConnectionServiceControl.class);
   }

   @Override
   protected MBeanAttributeInfo[] fillMBeanAttributeInfo() {
      return MBeanInfoHelper.getMBeanAttributesInfo(BrokerConnectionServiceControl.class);
   }
}
