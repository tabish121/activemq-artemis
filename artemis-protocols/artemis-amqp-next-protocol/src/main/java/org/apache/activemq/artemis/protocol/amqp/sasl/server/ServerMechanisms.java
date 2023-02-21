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

package org.apache.activemq.artemis.protocol.amqp.sasl.server;

import org.apache.qpid.protonj2.types.Symbol;

/**
 * Enumeration of Server provided mechanisms that is ordered by preference and provides
 * APIs to check on applicable and server default mechanisms.
 */
public enum ServerMechanisms {

   GSSAPI {

      @Override
      public String getName() {
          return GSSAPIMechanism.NAME;
      }

      @Override
      public ServerMechanism createMechanism() {
          return new GSSAPIMechanism();
      }
   },
   EXTERNAL {

      @Override
      public String getName() {
          return ExternalMechanism.NAME;
      }

      @Override
      public ServerMechanism createMechanism() {
          return new ExternalMechanism();
      }
   },
   PLAIN {

      @Override
      public String getName() {
          return PlainMechanism.NAME;
      }

      @Override
      public ServerMechanism createMechanism() {
          return new PlainMechanism();
      }
   },
   ANONYMOUS {

      @Override
      public String getName() {
          return AnonymousMechanism.NAME;
      }

      @Override
      public ServerMechanism createMechanism() {
          return new AnonymousMechanism();
      }
  };

   /**
    * @return the {@link String} that represents the {@link Mechanism} name.
    */
   public abstract String getName();

   /**
    * Creates the object that implements the SASL Mechanism represented by this enumeration.
    *
    * @return a new SASL {@link ServerMechanism} type that will be used for authentication.
    */
   public abstract ServerMechanism createMechanism();

   /**
    * Given a {@link Symbol} that contains a mechanism name, lookup the
    * matching mechanism if supported and return it.
    *
    * @param mechanism
    *       The name of a SASL mechanism that is being looked up.
    *
    * @return a {@link ServerMechanism} if a match is found or null if none.
    */
   public static ServerMechanisms valueOf(Symbol mechanism) {
      if (mechanism == null || mechanism.getLength() == 0) {
         return null;
      } else {
         return ServerMechanisms.valueOf(mechanism.toString());
      }
   }
}
