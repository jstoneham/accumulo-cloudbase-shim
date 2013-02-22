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

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.format.Formatter;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.collections.Transformer;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;

public class FormatterShim implements Formatter {

    public final cloudbase.core.util.format.Formatter impl;

    public FormatterShim(cloudbase.core.util.format.Formatter impl) {
        this.impl = impl;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void initialize(final Iterable<Map.Entry<Key, Value>> scanner, boolean printTimestamps) {
        impl.initialize(
                new Iterable<Map.Entry<cloudbase.core.data.Key, cloudbase.core.data.Value>>() {
                    @Override
                    public Iterator<Map.Entry<cloudbase.core.data.Key, cloudbase.core.data.Value>> iterator() {
                        return IteratorUtils.transformedIterator(scanner.iterator(),
                                new Transformer() {
                                    @Override
                                    public Map.Entry<cloudbase.core.data.Key, cloudbase.core.data.Value> transform(Object input) {
                                        Map.Entry<Key, Value> entry = (Map.Entry<Key, Value>) input;
                                        return new AbstractMap.SimpleEntry<cloudbase.core.data.Key, cloudbase.core.data.Value>(entry.getKey().impl, entry.getValue().impl);
                                    }
                                });
                    }
                }, printTimestamps);
    }

    @Override
    public boolean hasNext() {
        return impl.hasNext();
    }

    @Override
    public String next() {
        return impl.next();
    }

    @Override
    public void remove() {
        impl.remove();
    }
}
