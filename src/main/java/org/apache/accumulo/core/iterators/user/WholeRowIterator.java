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
package org.apache.accumulo.core.iterators.user;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class WholeRowIterator implements SortedKeyValueIterator {
    public static SortedMap<Key, Value> decodeRow(Key rowKey, Value rowValue) throws IOException {
        SortedMap<Key, Value> result = new TreeMap<Key, Value>();
        SortedMap<cloudbase.core.data.Key, cloudbase.core.data.Value> decoded =
                cloudbase.core.iterators.WholeRowIterator.decodeRow(rowKey.impl, rowValue.impl);
        for (Map.Entry<cloudbase.core.data.Key, cloudbase.core.data.Value> entry : decoded.entrySet()) {
            result.put(new Key(entry.getKey()), new Value(entry.getValue()));
        }
        return result;
    }
}
