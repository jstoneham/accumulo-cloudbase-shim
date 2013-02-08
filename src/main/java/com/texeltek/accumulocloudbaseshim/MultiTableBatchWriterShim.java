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
import org.apache.accumulo.core.client.*;

public class MultiTableBatchWriterShim implements MultiTableBatchWriter {
    public final cloudbase.core.client.MultiTableBatchWriter impl;

    public MultiTableBatchWriterShim(cloudbase.core.client.MultiTableBatchWriter impl) {
        this.impl = impl;
    }

    public BatchWriter getBatchWriter(String table) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        try {
            return new BatchWriterShim(impl.getBatchWriter(table));
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        } catch (cloudbase.core.client.TableNotFoundException e) {
            throw new TableNotFoundException(e);
        }
    }

    public boolean isClosed() {
        return impl.isClosed();
    }

    public void flush() throws MutationsRejectedException {
        try {
            impl.flush();
        } catch (cloudbase.core.client.MutationsRejectedException e) {
            throw new MutationsRejectedException(e);
        }
    }

    public void close() throws MutationsRejectedException {
        try {
            impl.close();
        } catch (cloudbase.core.client.MutationsRejectedException e) {
            throw new MutationsRejectedException(e);
        }
    }

    public String toString() {
        return impl.toString();
    }
}
