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
package cloudbase.core.client.mock;

import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import cloudbase.core.client.Connector;
import cloudbase.core.conf.CBConfiguration;

import java.util.List;

public class MockInstanceShim extends MockInstance {

    private final MockInstance impl;

    public MockInstanceShim(MockInstance mockInstance) {
        super();
        this.impl = mockInstance;
    }

    public MockInstanceShim(String instanceName) {
        this(new MockInstance(instanceName));
    }

    @Override
    public String getRootTabletLocation() {
        return impl.getRootTabletLocation();
    }

    @Override
    public List<String> getMasterLocations() {
        return impl.getMasterLocations();
    }

    @Override
    public String getInstanceID() {
        return impl.getInstanceID();
    }

    @Override
    public String getInstanceName() {
        return impl.getInstanceName();
    }

    @Override
    public String getZooKeepers() {
        return impl.getZooKeepers();
    }

    @Override
    public int getZooKeepersSessionTimeOut() {
        return impl.getZooKeepersSessionTimeOut();
    }

    @Override
    public Connector getConnector(String user, byte[] pass) throws CBException, CBSecurityException {
        return new MockConnectorShim(user, impl);
    }

    @Override
    public Connector getConnector(String user, CharSequence pass) throws CBException, CBSecurityException {
        return new MockConnectorShim(user, impl);
    }

    @Override
    public CBConfiguration getConfiguration() {
        return impl.getConfiguration();
    }

    @Override
    public void setConfiguration(CBConfiguration conf) {
        impl.setConfiguration(conf);
    }
}
