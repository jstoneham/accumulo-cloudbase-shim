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

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public abstract class Filter extends WrappingIterator implements OptionDescriber {

    public final cloudbase.core.iterators.filter.Filter impl;

    public Filter() {
        super();
        this.impl = null;
    }

    public Filter(cloudbase.core.iterators.filter.Filter impl) {
        super(null);
        this.impl = impl;
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void next() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        throw new UnsupportedOperationException();
    }

    public abstract boolean accept(Key k, Value v);

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public IteratorOptions describeOptions() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean validateOptions(Map<String, String> options) {
        throw new UnsupportedOperationException();
    }

    public static void setNegate(IteratorSetting is, boolean negate) {
        throw new UnsupportedOperationException();
    }
}
