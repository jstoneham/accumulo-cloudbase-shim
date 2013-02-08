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

import cloudbase.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.collections.Transformer;
import org.apache.commons.collections.iterators.TransformIterator;

import java.util.*;

public class BatchDeleterShim extends ScannerShimBase implements BatchDeleter {
    public final cloudbase.core.client.BatchDeleter impl;

    public BatchDeleterShim(cloudbase.core.client.BatchDeleter impl) {
        super(impl);
        this.impl = impl;
    }

    public void delete() throws MutationsRejectedException, TableNotFoundException {
        try {
            impl.delete();
        } catch (cloudbase.core.client.MutationsRejectedException e) {
            throw new MutationsRejectedException(e);
        } catch (cloudbase.core.client.TableNotFoundException e) {
            throw new TableNotFoundException(e);
        }
    }

    public void setRanges(Collection<Range> ranges) {
        List<cloudbase.core.data.Range> impls = new ArrayList<cloudbase.core.data.Range>(ranges.size());
        for (Range range : ranges) {
            impls.add(range.impl);
        }
        impl.setRanges(impls);
    }

    public void close() {
        ((BatchScanner) impl).close();
    }

    public Iterator<Map.Entry<Key, Value>> iterator() {
        return new TransformIterator(((BatchScanner) impl).iterator(), new Transformer() {
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
