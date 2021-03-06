/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.client;

import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;

import java.util.List;

public interface Instance {
    String getRootTabletLocation();

    List<String> getMasterLocations();

    String getInstanceID();

    String getInstanceName();

    String getZooKeepers();

    int getZooKeepersSessionTimeOut();

    Connector getConnector(String user, byte[] pass) throws AccumuloException, AccumuloSecurityException;

    cloudbase.core.client.Connector getNativeConnector(String user, byte[] pass) throws CBException, CBSecurityException;

    Connector getConnector(String user, CharSequence pass) throws AccumuloException, AccumuloSecurityException;

    cloudbase.core.client.Connector getNativeConnector(String user, CharSequence pass) throws CBException, CBSecurityException;

    AccumuloConfiguration getConfiguration();

    void setConfiguration(AccumuloConfiguration conf);
}
