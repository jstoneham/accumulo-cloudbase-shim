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
package org.apache.accumulo.core.client.admin;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;

import java.util.List;
import java.util.Map;

public interface InstanceOperations {

    public void setProperty(final String property, final String value) throws AccumuloException, AccumuloSecurityException;

    public void removeProperty(final String property) throws AccumuloException, AccumuloSecurityException;

    public Map<String, String> getSystemConfiguration() throws AccumuloException, AccumuloSecurityException;

    public Map<String, String> getSiteConfiguration() throws AccumuloException, AccumuloSecurityException;

    public List<String> getTabletServers();
}
