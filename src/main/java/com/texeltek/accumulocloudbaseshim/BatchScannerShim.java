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

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.collections.Transformer;
import org.apache.commons.collections.iterators.TransformIterator;

import java.util.*;

public class BatchScannerShim extends ScannerBaseShim implements BatchScanner {
    public final cloudbase.core.client.BatchScanner impl;

    public BatchScannerShim(cloudbase.core.client.BatchScanner impl) {
        super(impl);
        this.impl = impl;
    }

    public void setRanges(Collection<Range> ranges) {
        List<cloudbase.core.data.Range> impls = new ArrayList<cloudbase.core.data.Range>(ranges.size());
        for (Range range : ranges) {
            impls.add(range.impl);
        }
        impl.setRanges(impls);
    }

    public void close() {
        impl.close();
    }

    public Iterator<Map.Entry<Key, Value>> iterator() {
        return new TransformIterator(
                new CloudbaseBatchScannerCloseableIterator(impl),
                new Transformer() {
                    @Override
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
