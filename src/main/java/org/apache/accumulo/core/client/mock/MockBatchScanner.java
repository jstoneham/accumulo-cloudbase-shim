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

import com.texeltek.accumulocloudbaseshim.KeyValueWrappingIterator;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;

import java.util.*;

public class MockBatchScanner extends MockScannerBase implements BatchScanner {

    public final cloudbase.core.client.mock.MockBatchScanner impl;

    public MockBatchScanner(cloudbase.core.client.mock.MockBatchScanner impl) {
        super(impl);
        this.impl = impl;
    }

    public void setRanges(Collection<Range> ranges) {
        List<cloudbase.core.data.Range> newRanges = new ArrayList<cloudbase.core.data.Range>(ranges.size());
        for (Range range : ranges) {
            newRanges.add(range.impl);
        }
        impl.setRanges(newRanges);
    }

    public Iterator<Map.Entry<Key, Value>> iterator() {
        return new KeyValueWrappingIterator(impl.iterator());
    }

    public void close() {
        impl.close();
    }

    public String toString() {
        return impl.toString();
    }
}
