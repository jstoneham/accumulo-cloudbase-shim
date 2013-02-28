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

import cloudbase.core.client.*;

/**
 * Very simple MultiTableBatchWriter for mocks
 */
public class MockMultiTableBatchWriterShim implements MultiTableBatchWriter{

    private Connector connector;
    private boolean closed = false;
    private final long maxMemory;
    private final long maxLatency;
    private final int maxWriteThreads;

    public MockMultiTableBatchWriterShim(Connector connector, long maxMemory, long maxLatency, int maxWriteThreads) {
        this.connector = connector;
        this.maxMemory = maxMemory;
        this.maxLatency = maxLatency;
        this.maxWriteThreads = maxWriteThreads;
    }

    @Override
    public BatchWriter getBatchWriter(String table) throws CBException, CBSecurityException, TableNotFoundException {
        return connector.createBatchWriter(table, maxMemory, maxLatency, maxWriteThreads);
    }

    @Override
    public void flush() throws MutationsRejectedException {
    }

    @Override
    public void close() throws MutationsRejectedException {
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }
}
