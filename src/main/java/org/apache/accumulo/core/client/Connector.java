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
import com.texeltek.accumulocloudbaseshim.*;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.security.Authorizations;

public class Connector {
    public final cloudbase.core.client.Connector impl;

    public Connector(Instance instance, String user, byte[] password) throws AccumuloException, AccumuloSecurityException {
        try {
            this.impl = instance.getNativeConnector(user, password);
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    public Connector() {
        impl = null;
    }

    public Connector(cloudbase.core.client.Connector impl) {
        this.impl = impl;
    }

    public BatchScanner createBatchScanner(String tableName, Authorizations authorizations, int numQueryThreads) throws TableNotFoundException {
        try {
            return new BatchScannerShim(impl.createBatchScanner(tableName, authorizations.impl, numQueryThreads));
        } catch (cloudbase.core.client.TableNotFoundException e) {
            throw new TableNotFoundException(e);
        }
    }

    public BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations, int numQueryThreads, long maxMemory, long maxLatency,
                                           int maxWriteThreads) throws TableNotFoundException {
        try {
            return new BatchDeleterShim(impl.createBatchDeleter(tableName, authorizations.impl, numQueryThreads, maxMemory, maxLatency, maxWriteThreads));
        } catch (cloudbase.core.client.TableNotFoundException e) {
            throw new TableNotFoundException(e);
        }
    }

    public BatchWriter createBatchWriter(String tableName, long maxMemory, long maxLatency, int maxWriteThreads) throws TableNotFoundException {
        try {
            return new BatchWriterShim(impl.createBatchWriter(tableName, maxMemory, maxLatency, maxWriteThreads));
        } catch (cloudbase.core.client.TableNotFoundException e) {
            throw new TableNotFoundException(e);
        }
    }

    public MultiTableBatchWriter createMultiTableBatchWriter(long maxMemory, long maxLatency, int maxWriteThreads) {
        return new MultiTableBatchWriterShim(impl.createMultiTableBatchWriter(maxMemory, (int) maxLatency, maxWriteThreads));
    }

    public Scanner createScanner(String tableName, Authorizations authorizations) throws TableNotFoundException {
        try {
            return new ScannerShim(impl.createScanner(tableName, authorizations.impl));
        } catch (cloudbase.core.client.TableNotFoundException e) {
            throw new TableNotFoundException(e);
        }
    }

    public Instance getInstance() {
        cloudbase.core.client.Instance instance = impl.getInstance();
        if (instance instanceof cloudbase.core.client.mock.MockInstance) {
            return new MockInstance((cloudbase.core.client.mock.MockInstance) instance);
        } else if (instance instanceof cloudbase.core.client.ZooKeeperInstance) {
            return new ZooKeeperInstance((cloudbase.core.client.ZooKeeperInstance) instance);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    public String whoami() {
        return impl.whoami();
    }

    public synchronized TableOperations tableOperations() {
        return new TableOperationsShim(impl.tableOperations(), impl);
    }

    public synchronized SecurityOperations securityOperations() {
        return new SecurityOperationsShim(impl.securityOperations());
    }

    public synchronized InstanceOperations instanceOperations() {
        return new InstanceOperationsShim(impl.instanceOperations(), impl);
    }

    public String toString() {
        return impl.toString();
    }
}
