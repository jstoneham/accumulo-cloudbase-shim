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

import cloudbase.core.client.BatchDeleter;
import cloudbase.core.client.BatchWriter;
import cloudbase.core.client.MutationsRejectedException;
import cloudbase.core.client.TableNotFoundException;
import cloudbase.core.data.Key;
import cloudbase.core.data.Mutation;
import cloudbase.core.data.Value;
import cloudbase.core.security.Authorizations;
import cloudbase.core.security.ColumnVisibility;

import java.util.ConcurrentModificationException;
import java.util.Map.Entry;

public class MockBatchDeleter extends MockBatchScanner implements BatchDeleter {

    private final MockCloudbase cb;
    private final String tableName;

    public MockBatchDeleter(MockCloudbase cb, String tableName, Authorizations auths) {
        super(cb.tables.get(tableName), auths);
        this.cb = cb;
        this.tableName = tableName;
    }

    @Override
    public void delete() throws MutationsRejectedException, TableNotFoundException {
        BatchWriter writer = new MockBatchWriter(cb, tableName);
        try {
            for (Entry<Key, Value> next : this) {
                Key k = next.getKey();
                Mutation m = new Mutation(k.getRow());
                m.putDelete(k.getColumnFamily(), k.getColumnQualifier(), new ColumnVisibility(k.getColumnVisibility()), k.getTimestamp());
                writer.addMutation(m);
            }
        } catch (ConcurrentModificationException e) {
            // ignore for now
        } finally {
            writer.close();
        }
    }
}
