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
package org.apache.accumulo.core.file;

import com.texeltek.accumulocloudbaseshim.FileOperationsShim;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.file.blockfile.cache.BlockCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.Set;

public abstract class FileOperations {

    public final cloudbase.core.file.FileOperations impl;

    public FileOperations(cloudbase.core.file.FileOperations impl) {
        this.impl = impl;
    }

    public static Set<String> getValidExtensions() {
        return cloudbase.core.file.FileOperations.getValidExtensions();
    }

    public static String getNewFileExtension(AccumuloConfiguration acuconf) {
        return cloudbase.core.file.FileOperations.getNewFileExtension(acuconf.impl);
    }

    public static FileOperations getInstance() {
        return new FileOperationsShim(cloudbase.core.file.FileOperations.getInstance());
    }

    public abstract FileSKVIterator openReader(String file, Range range, Set<ByteSequence> columnFamilies, boolean inclusive, FileSystem fs, Configuration conf,
                                               AccumuloConfiguration tableConf) throws IOException;

    public abstract FileSKVIterator openReader(String file, Range range, Set<ByteSequence> columnFamilies, boolean inclusive, FileSystem fs, Configuration conf,
                                               AccumuloConfiguration tableConf, BlockCache dataCache, BlockCache indexCache) throws IOException;

    public abstract FileSKVIterator openReader(String file, boolean seekToBeginning, FileSystem fs, Configuration conf, AccumuloConfiguration acuconf)
            throws IOException;

    public abstract FileSKVIterator openReader(String file, boolean seekToBeginning, FileSystem fs, Configuration conf, AccumuloConfiguration acuconf,
                                               BlockCache dataCache, BlockCache indexCache) throws IOException;

    public abstract FileSKVWriter openWriter(String file, FileSystem fs, Configuration conf, AccumuloConfiguration acuconf) throws IOException;

    public abstract FileSKVIterator openIndex(String file, FileSystem fs, Configuration conf, AccumuloConfiguration acuconf) throws IOException;

    public abstract FileSKVIterator openIndex(String file, FileSystem fs, Configuration conf, AccumuloConfiguration acuconf, BlockCache dCache, BlockCache iCache)
            throws IOException;

    public abstract long getFileSize(String file, FileSystem fs, Configuration conf, AccumuloConfiguration acuconf) throws IOException;
}
