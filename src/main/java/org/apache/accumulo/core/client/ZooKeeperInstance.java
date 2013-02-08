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

import java.util.List;
import java.util.UUID;

public class ZooKeeperInstance implements Instance {

    public final cloudbase.core.client.ZooKeeperInstance impl;

    public ZooKeeperInstance(String instanceName, String zooKeepers) {
        this.impl = new cloudbase.core.client.ZooKeeperInstance(instanceName, zooKeepers);
    }

    public ZooKeeperInstance(String instanceName, String zooKeepers, int sessionTimeout) {
        this.impl = new cloudbase.core.client.ZooKeeperInstance(instanceName, zooKeepers, sessionTimeout);
    }

    public ZooKeeperInstance(UUID instanceId, String zooKeepers) {
        this.impl = new cloudbase.core.client.ZooKeeperInstance(instanceId, zooKeepers);
    }

    public ZooKeeperInstance(cloudbase.core.client.ZooKeeperInstance impl) {
        this.impl = impl;
    }

    public String getInstanceID() {
        return impl.getInstanceID();
    }

    public List<String> getMasterLocations() {
        return impl.getMasterLocations();
    }

    public String getRootTabletLocation() {
        return impl.getRootTabletLocation();
    }

    public String getInstanceName() {
        return impl.getInstanceName();
    }

    public String getZooKeepers() {
        return impl.getZooKeepers();
    }

    public int getZooKeepersSessionTimeOut() {
        return impl.getZooKeepersSessionTimeOut();
    }

    public Connector getConnector(String user, CharSequence pass) throws AccumuloException, AccumuloSecurityException {
        try {
            return new Connector(getNativeConnector(user, pass));
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    public cloudbase.core.client.Connector getNativeConnector(String user, CharSequence pass) throws CBException, CBSecurityException {
        return impl.getConnector(user, pass);
    }

    public Connector getConnector(String user, byte[] pass) throws AccumuloException, AccumuloSecurityException {
        try {
            return new Connector(getNativeConnector(user, pass));
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    public cloudbase.core.client.Connector getNativeConnector(String user, byte[] pass) throws CBException, CBSecurityException {
        return impl.getConnector(user, pass);
    }

    public String toString() {
        return impl.toString();
    }
}
