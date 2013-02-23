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
package org.apache.accumulo.core.file.map;

import com.texeltek.accumulocloudbaseshim.IteratorEnvironmentShim;
import com.texeltek.accumulocloudbaseshim.SortedKeyValueIteratorShim;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.util.Stat;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Transformer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class MyMapFile {

    public static final String EXTENSION = "map";

    public static final String INDEX_FILE_NAME = "index";

    public static final String DATA_FILE_NAME = "data";

    public static final Stat mapFileSeekTimeStat = new Stat();

    public static final Stat mapFileSeekScans = new Stat();

    public static final Stat mapFileSeekScanTime = new Stat();

    public static final Stat mapFileSeekScanCompareTime = new Stat();

    protected MyMapFile() {
    } // no public ctor

    public static class Reader implements FileSKVIterator {

        public final cloudbase.core.file.map.MyMapFile.Reader impl;

        public Class getKeyClass() {
            return impl.getKeyClass();
        }

        public Class getValueClass() {
            return impl.getValueClass();
        }

        public Reader(FileSystem fs, String dirName, Configuration conf) throws IOException {
            this.impl = new cloudbase.core.file.map.MyMapFile.Reader(fs, dirName, conf);
        }

        public Reader(FileSystem fs, String dirName, WritableComparator comparator, Configuration conf) throws IOException {
            this.impl = new cloudbase.core.file.map.MyMapFile.Reader(fs, dirName, comparator, conf);
        }

        @Override
        public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
            return new SortedKeyValueIteratorShim(impl.deepCopy(((IteratorEnvironmentShim) env).impl));
        }

        @Override
        public void closeDeepCopies() throws IOException {
            impl.closeDeepCopies();
        }

        public synchronized void reset() throws IOException {
            impl.reset();
        }

        public synchronized WritableComparable midKey() throws IOException {
            return impl.midKey();
        }

        public synchronized void finalKey(WritableComparable key) throws IOException {
            impl.finalKey(key);
        }

        public synchronized boolean seek(WritableComparable key) throws IOException {
            return impl.seek(key);
        }

        public synchronized boolean seek(WritableComparable key, WritableComparable lastKey) throws IOException {
            return impl.seek(key, lastKey);
        }

        public synchronized int getIndexPosition(WritableComparable key) throws IOException {
            return impl.getIndexPosition(key);
        }

        public synchronized void printIndex() throws IOException {
            impl.printIndex();
        }

        public synchronized boolean next(WritableComparable key, Writable val) throws IOException {
            return impl.next(key, val);
        }

        public synchronized Writable get(WritableComparable key, Writable val) throws IOException {
            return impl.get(key, val);
        }

        public synchronized WritableComparable getClosest(WritableComparable key, Writable val) throws IOException {
            return impl.getClosest(key, val);
        }

        public synchronized WritableComparable getClosest(WritableComparable key, Writable val, final boolean before) throws IOException {
            return impl.getClosest(key, val, before);
        }

        public synchronized WritableComparable getClosest(WritableComparable key, Writable val, final boolean before, WritableComparable lastKey)
                throws IOException {
            return impl.getClosest(key, val, before, lastKey);
        }

        public synchronized void close() throws IOException {
            impl.close();
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
            impl.seek(range.impl,
                    CollectionUtils.collect(columnFamilies,
                            new Transformer() {
                                @Override
                                public Object transform(Object o) {
                                    return ((ByteSequence) o).impl;
                                }
                            }), inclusive);
        }

        @Override
        public synchronized Key getFirstKey() throws IOException {
            return new Key(impl.getFirstKey());
        }

        @Override
        public Key getLastKey() throws IOException {
            return new Key(impl.getLastKey());
        }

        @Override
        public DataInputStream getMetaStore(String name) throws IOException {
            return impl.getMetaStore(name);
        }

        public void dropIndex() {
            impl.dropIndex();
        }

        @Override
        public void setInterruptFlag(AtomicBoolean flag) {
            impl.setInterruptFlag(flag);
        }
    }

    public static void rename(FileSystem fs, String oldName, String newName) throws IOException {
        cloudbase.core.file.map.MyMapFile.rename(fs, oldName, newName);
    }

    public static void delete(FileSystem fs, String name) throws IOException {
        cloudbase.core.file.map.MyMapFile.delete(fs, name);
    }

    public static int getIndexInterval() {
        return cloudbase.core.file.map.MyMapFile.getIndexInterval();
    }

    public static long fix(FileSystem fs, Path dir, Class<? extends WritableComparable> keyClass, Class<? extends Writable> valueClass, boolean dryrun,
                           Configuration conf) throws Exception {
        return cloudbase.core.file.map.MyMapFile.fix(fs, dir, keyClass, valueClass, dryrun, conf);
    }

    public static void main(String[] args) throws Exception {
        cloudbase.core.file.map.MyMapFile.main(args);
    }

}