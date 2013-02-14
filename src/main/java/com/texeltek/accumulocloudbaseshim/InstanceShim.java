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
package com.texeltek.accumulocloudbaseshim;

import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;

import java.util.List;

public class InstanceShim implements Instance {

    public final cloudbase.core.client.Instance impl;

    public InstanceShim(cloudbase.core.client.Instance impl) {
        this.impl = impl;
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
    public Connector getConnector(String user, byte[] pass) throws AccumuloException, AccumuloSecurityException {
        try {
            return new Connector(impl.getConnector(user, pass));
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    @Override
    public cloudbase.core.client.Connector getNativeConnector(String user, byte[] pass) throws CBException, CBSecurityException {
        return impl.getConnector(user, pass);
    }

    @Override
    public Connector getConnector(String user, CharSequence pass) throws AccumuloException, AccumuloSecurityException {
        try {
            return new Connector(impl.getConnector(user, pass));
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    @Override
    public cloudbase.core.client.Connector getNativeConnector(String user, CharSequence pass) throws CBException, CBSecurityException {
        return impl.getConnector(user, pass);
    }

    @Override
    public AccumuloConfiguration getConfiguration() {
        return new AccumuloConfigurationShim(impl.getConfiguration());
    }

    @Override
    public void setConfiguration(AccumuloConfiguration conf) {
        impl.setConfiguration(conf.impl);
    }
}
