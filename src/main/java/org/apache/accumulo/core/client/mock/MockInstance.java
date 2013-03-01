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
package org.apache.accumulo.core.client.mock;

import com.texeltek.accumulocloudbaseshim.AccumuloConfigurationShim;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.io.Text;

import java.util.List;

public class MockInstance implements Instance {

    public final cloudbase.core.client.mock.MockInstance impl;

    public MockInstance() {
        this.impl = new cloudbase.core.client.mock.MockInstance();
    }

    public MockInstance(String instanceName) {
        this.impl = new cloudbase.core.client.mock.MockInstance(instanceName);
    }

    public MockInstance(cloudbase.core.client.mock.MockInstance impl) {
        this.impl = impl;
    }

    public String getRootTabletLocation() {
        return impl.getRootTabletLocation();
    }

    public List<String> getMasterLocations() {
        return impl.getMasterLocations();
    }

    public String getInstanceID() {
        return impl.getInstanceID();
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

    public Connector getConnector(String user, byte[] pass) throws AccumuloException, AccumuloSecurityException {
        try {
            return new MockConnector(getNativeConnector(user, pass));
        } catch (cloudbase.core.client.CBException e) {
            throw new AccumuloException(e);
        } catch (cloudbase.core.client.CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    public cloudbase.core.client.mock.MockConnector getNativeConnector(String user, byte[] pass) throws cloudbase.core.client.CBException, cloudbase.core.client.CBSecurityException {
        return new cloudbase.core.client.mock.MockConnectorShim(user, impl);
    }

    public Connector getConnector(String user, CharSequence pass) throws AccumuloException, AccumuloSecurityException {
        return getConnector(user, TextUtil.getBytes(new Text(pass.toString())));
    }

    public cloudbase.core.client.mock.MockConnector getNativeConnector(String user, CharSequence pass) throws cloudbase.core.client.CBException, cloudbase.core.client.CBSecurityException {
        return new cloudbase.core.client.mock.MockConnectorShim(user);
    }

    @Override
    public AccumuloConfiguration getConfiguration() {
        return new AccumuloConfigurationShim(impl.getConfiguration());
    }

    @Override
    public void setConfiguration(AccumuloConfiguration conf) {
        impl.setConfiguration(conf.impl);
    }

    public String toString() {
        return impl.toString();
    }
}
