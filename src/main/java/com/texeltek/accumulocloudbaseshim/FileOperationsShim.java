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

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.blockfile.cache.BlockCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.Set;

public class FileOperationsShim extends FileOperations {

    public cloudbase.core.file.FileOperations impl;

    public FileOperationsShim(cloudbase.core.file.FileOperations impl) {
        super(impl);
    }

    @Override
    public FileSKVIterator openReader(String file, Range range, Set<ByteSequence> columnFamilies, boolean inclusive, FileSystem fs, Configuration conf, AccumuloConfiguration tableConf) throws IOException {
        return new FileSKVIteratorShim(impl.openReader(file, range.impl, ByteSequenceShim.cloudbaseSet(columnFamilies), inclusive, fs, conf, tableConf.impl));
    }

    @Override
    public FileSKVIterator openReader(String file, Range range, Set<ByteSequence> columnFamilies, boolean inclusive, FileSystem fs, Configuration conf, AccumuloConfiguration tableConf, BlockCache dataCache, BlockCache indexCache) throws IOException {
        return new FileSKVIteratorShim(impl.openReader(file, range.impl, ByteSequenceShim.cloudbaseSet(columnFamilies), inclusive, fs, conf, tableConf.impl));
    }

    @Override
    public FileSKVIterator openReader(String file, boolean seekToBeginning, FileSystem fs, Configuration conf, AccumuloConfiguration acuconf) throws IOException {
        return new FileSKVIteratorShim(impl.openReader(file, seekToBeginning, fs, conf, acuconf.impl));
    }

    @Override
    public FileSKVIterator openReader(String file, boolean seekToBeginning, FileSystem fs, Configuration conf, AccumuloConfiguration acuconf, BlockCache dataCache, BlockCache indexCache) throws IOException {
        return new FileSKVIteratorShim(impl.openReader(file, seekToBeginning, fs, conf, acuconf.impl, ((BlockCacheShim) dataCache).impl, ((BlockCacheShim) indexCache).impl));
    }

    @Override
    public FileSKVWriter openWriter(String file, FileSystem fs, Configuration conf, AccumuloConfiguration acuconf) throws IOException {
        return new FileSKVWriterShim(impl.openWriter(file, fs, conf, acuconf.impl));
    }

    @Override
    public FileSKVIterator openIndex(String file, FileSystem fs, Configuration conf, AccumuloConfiguration acuconf) throws IOException {
        return new FileSKVIteratorShim(impl.openIndex(file, fs, conf, acuconf.impl));
    }

    @Override
    public FileSKVIterator openIndex(String file, FileSystem fs, Configuration conf, AccumuloConfiguration acuconf, BlockCache dCache, BlockCache iCache) throws IOException {
        return new FileSKVIteratorShim(impl.openIndex(file, fs, conf, acuconf.impl, ((BlockCacheShim) dCache).impl, ((BlockCacheShim) iCache).impl));
    }

    @Override
    public long getFileSize(String file, FileSystem fs, Configuration conf, AccumuloConfiguration acuconf) throws IOException {
        return impl.getFileSize(file, fs, conf, acuconf.impl);
    }
}
