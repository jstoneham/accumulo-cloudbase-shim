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
package org.apache.accumulo.core.iterators;

import com.texeltek.accumulocloudbaseshim.ByteSequenceShim;
import com.texeltek.accumulocloudbaseshim.IteratorEnvironmentShim;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.system.InterruptibleIterator;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Transformer;
import org.apache.commons.collections.map.TransformedSortedMap;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class SortedMapIterator implements InterruptibleIterator {

    public final cloudbase.core.iterators.SortedMapIterator impl;

    public SortedMapIterator(cloudbase.core.iterators.SortedMapIterator impl) {
        this.impl = impl;
    }

    public SortedMapIterator deepCopy(IteratorEnvironment env) {
        return new SortedMapIterator(impl.deepCopy(((IteratorEnvironmentShim) env).impl));
    }

    @SuppressWarnings("unchecked")
    public SortedMapIterator(SortedMap<Key, Value> map) {
        this.impl = new cloudbase.core.iterators.SortedMapIterator(
                TransformedSortedMap.decorateTransform(map,
                        new Transformer() {
                            @Override
                            public Object transform(Object input) {
                                return ((Key) input).impl;
                            }
                        },
                        new Transformer() {
                            @Override
                            public Object transform(Object input) {
                                return ((Value) input).impl;
                            }
                        }
                ));
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

    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setInterruptFlag(AtomicBoolean flag) {
        impl.setInterruptFlag(flag);
    }
}
