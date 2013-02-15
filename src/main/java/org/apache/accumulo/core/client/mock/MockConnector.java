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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import com.texeltek.accumulocloudbaseshim.BatchDeleterShim;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.security.Authorizations;

public class MockConnector extends Connector {

    public final cloudbase.core.client.mock.MockConnector impl;
    public final cloudbase.core.client.mock.MockCloudbase cb;

    public MockConnector(String username) {
        impl = new cloudbase.core.client.mock.MockConnectorShim(username, cb = new cloudbase.core.client.mock.MockCloudbase());
    }

    public MockConnector(MockInstance instance, String user, byte[] password) throws AccumuloException, AccumuloSecurityException {
        super(instance, user, password);
        impl = new cloudbase.core.client.mock.MockConnectorShim(user, cb = new cloudbase.core.client.mock.MockCloudbase());
    }

    public MockConnector(cloudbase.core.client.mock.MockConnector impl) {
        super(impl);
        this.impl = impl;
        this.cb = null;
    }

    @Override
    public BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations, int numQueryThreads, long maxMemory, long maxLatency,
                                           int maxWriteThreads) throws TableNotFoundException {
        return new BatchDeleterShim(new cloudbase.core.client.mock.MockBatchDeleter(cb, tableName, authorizations.impl));
    }

    public String toString() {
        return impl.toString();
    }
}
