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

import cloudbase.core.client.mock.MockCloudbaseShim;
import com.texeltek.accumulocloudbaseshim.BatchScannerShim;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;

public class MockAccumulo {
    public final MockCloudbaseShim impl;

    public MockAccumulo(MockCloudbaseShim impl) {
        this.impl = impl;
    }

    public void setProperty(String key, String value) {
        impl.setProperty(key, value);
    }

    public String removeProperty(String key) {
        throw new UnsupportedOperationException();
    }

    public void createTable(String user, String table) {
        impl.createTable(user, table);
    }

    public void addMutation(String table, Mutation m) {
        impl.addMutation(table, m.impl);
    }

    public BatchScanner createBatchScanner(String tableName, Authorizations authorizations) {
        return new BatchScannerShim(impl.createBatchScanner(tableName, authorizations.impl));
    }

    public void createTable(String username, String tableName, boolean useVersions, TimeType timeType) {
        throw new UnsupportedOperationException();
    }

    public String toString() {
        return impl.toString();
    }
}
