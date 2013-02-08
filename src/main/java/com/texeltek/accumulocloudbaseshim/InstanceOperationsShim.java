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
import cloudbase.core.conf.CBConfiguration;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.InstanceOperations;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class InstanceOperationsShim implements InstanceOperations {
    public final cloudbase.core.client.admin.InstanceOperations impl;
    public final cloudbase.core.client.Connector connector;

    public InstanceOperationsShim(cloudbase.core.client.admin.InstanceOperations impl,
                                  cloudbase.core.client.Connector connector) {
        this.impl = impl;
        this.connector = connector;
    }

    public void setProperty(String property, String value) throws AccumuloException, AccumuloSecurityException {
        try {
            impl.setProperty(property, value);
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    public void removeProperty(String property) throws AccumuloException, AccumuloSecurityException {
        try {
            impl.removeProperty(property);
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    public Map<String, String> getSystemConfiguration() throws AccumuloException, AccumuloSecurityException {
        Map<String, String> systemConfig = new TreeMap<String, String>();
        for (Map.Entry<String, String> sysEntry : CBConfiguration.getSystemConfiguration(connector.getInstance())) {
            systemConfig.put(sysEntry.getKey(), sysEntry.getValue());
        }
        return systemConfig;
    }

    public Map<String, String> getSiteConfiguration() throws AccumuloException, AccumuloSecurityException {
        Map<String, String> siteConfig = new TreeMap<String, String>();
        for (Map.Entry<String, String> siteEntry : CBConfiguration.getSiteConfiguration()) {
            siteConfig.put(siteEntry.getKey(), siteEntry.getValue());
        }
        return siteConfig;
    }

    public List<String> getTabletServers() {
        return impl.getTabletServers();
    }

    public String toString() {
        return impl.toString();
    }
}
