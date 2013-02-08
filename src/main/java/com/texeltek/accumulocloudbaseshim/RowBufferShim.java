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

import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.util.Iterator;
import java.util.Map;

public class RowBufferShim implements IsolatedScanner.RowBuffer {
    public final cloudbase.core.client.IsolatedScanner.RowBuffer impl;

    public RowBufferShim(cloudbase.core.client.IsolatedScanner.RowBuffer impl) {
        this.impl = impl;
    }

    public void add(Map.Entry<Key, Value> entry) {
        impl.add(new CloudbaseEntryShim(entry.getKey().impl, entry.getValue().impl));
    }

    public Iterator<Map.Entry<Key, Value>> iterator() {
        return new KeyValueWrappingIterator(impl.iterator());
    }

    public void clear() {
        impl.clear();
    }

    public String toString() {
        return impl.toString();
    }
}
