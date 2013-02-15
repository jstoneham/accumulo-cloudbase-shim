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

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.NoSuchMetaStoreException;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class FileSKVIteratorShim implements FileSKVIterator {

    public final cloudbase.core.file.FileSKVIterator impl;

    public FileSKVIteratorShim(cloudbase.core.file.FileSKVIterator impl) {
        this.impl = impl;
    }

    @Override
    public Key getFirstKey() throws IOException {
        return new Key(impl.getFirstKey());
    }

    @Override
    public Key getLastKey() throws IOException {
        return new Key(impl.getLastKey());
    }

    @Override
    public DataInputStream getMetaStore(String name) throws IOException, NoSuchMetaStoreException {
        return impl.getMetaStore(name);
    }

    @Override
    public void closeDeepCopies() throws IOException {
        impl.closeDeepCopies();
    }

    @Override
    public void close() throws IOException {
        impl.close();
    }

    @Override
    public void setInterruptFlag(AtomicBoolean flag) {
        impl.setInterruptFlag(flag);
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        impl.init(((SortedKeyValueIteratorShim) source).impl, options, ((IteratorEnvironmentShim) env).impl);
    }

    @Override
    public boolean hasTop() {
        return impl.hasTop();
    }

    @Override
    public void next() throws IOException {
        impl.next();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        impl.seek(range.impl, ByteSequenceShim.cloudbaseCollection(columnFamilies), inclusive);
    }

    @Override
    public Key getTopKey() {
        return new Key(impl.getTopKey());
    }

    @Override
    public Value getTopValue() {
        return new Value(impl.getTopValue());
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        return new SortedKeyValueIteratorShim(impl.deepCopy(((IteratorEnvironmentShim) env).impl));
    }
}
