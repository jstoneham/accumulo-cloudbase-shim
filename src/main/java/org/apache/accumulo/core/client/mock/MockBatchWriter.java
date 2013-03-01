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

import cloudbase.core.client.mock.MockBatchWriterShim;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;

public class MockBatchWriter implements BatchWriter {

    public final cloudbase.core.client.mock.MockBatchWriter impl;

    MockBatchWriter(MockAccumulo acu, String tablename) {
        this.impl = new MockBatchWriterShim(acu.impl, tablename);
    }

    @Override
    public void addMutation(Mutation m) throws MutationsRejectedException {
        try {
            impl.addMutation(m.impl);
        } catch (cloudbase.core.client.MutationsRejectedException e) {
            throw new MutationsRejectedException(e);
        }
    }

    @Override
    public void addMutations(Iterable<Mutation> iterable) throws MutationsRejectedException {
        try {
            for (Mutation m : iterable) {
                impl.addMutation(m.impl);
            }
        } catch (cloudbase.core.client.MutationsRejectedException e) {
            throw new MutationsRejectedException(e);
        }
    }

    @Override
    public void flush() throws MutationsRejectedException {
        try {
            impl.flush();
        } catch (cloudbase.core.client.MutationsRejectedException e) {
            throw new MutationsRejectedException(e);
        }
    }

    @Override
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
