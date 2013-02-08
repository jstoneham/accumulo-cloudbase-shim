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

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.collections.Transformer;
import org.apache.commons.collections.iterators.TransformIterator;

import java.util.Iterator;
import java.util.Map;

public class ScannerShim extends ScannerShimBase implements Scanner {
    public final cloudbase.core.client.Scanner impl;

    public ScannerShim(cloudbase.core.client.Scanner impl) {
        super(impl);
        this.impl = impl;
    }

    public void setTimeOut(int timeOut) {
        impl.setTimeOut(timeOut);
    }

    public int getTimeOut() {
        return impl.getTimeOut();
    }

    public void setRange(Range range) {
        impl.setRange(range.impl);
    }

    public Range getRange() {
        return new Range(impl.getRange());
    }

    public void setBatchSize(int size) {
        impl.setBatchSize(size);
    }

    public int getBatchSize() {
        return impl.getBatchSize();
    }

    public void enableIsolation() {
        impl.enableIsolation();
    }

    public void disableIsolation() {
        impl.disableIsolation();
    }

    public Iterator<Map.Entry<Key, Value>> iterator() {
        return new TransformIterator(impl.iterator(), new Transformer() {
            public Object transform(Object input) {
                Map.Entry<cloudbase.core.data.Key, cloudbase.core.data.Value> entry =
                        (Map.Entry<cloudbase.core.data.Key, cloudbase.core.data.Value>) input;
                return new EntryShim(new Key(entry.getKey()), new Value(entry.getValue()));
            }
        });
    }

    public String toString() {
        return impl.toString();
    }
}
