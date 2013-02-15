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

import com.texeltek.accumulocloudbaseshim.SortedKeyValueIteratorShim;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.impl.ScannerOptions;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class MockScannerBase extends ScannerOptions implements ScannerBase {

    protected final cloudbase.core.client.mock.MockScannerBase impl;

    public MockScannerBase(cloudbase.core.client.mock.MockScannerBase impl) {
        this.impl = impl;
    }

    public SortedKeyValueIterator<Key, Value> createFilter(SortedKeyValueIterator<Key, Value> inner) throws IOException {
        return new SortedKeyValueIteratorShim(impl.createFilter(((SortedKeyValueIteratorShim) inner).impl));
    }

    public Iterator<Map.Entry<Key, Value>> iterator() {
        throw new UnsupportedOperationException();
    }

    public String toString() {
        return impl.toString();
    }
}