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

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.commons.collections.Transformer;
import org.apache.commons.collections.map.TransformedSortedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.*;

public class FileUtil {

    public static class FileInfo {
        public final cloudbase.core.file.FileUtil.FileInfo impl;

        public FileInfo(cloudbase.core.file.FileUtil.FileInfo impl) {
            this.impl = impl;
        }

        public FileInfo(Key firstKey, Key lastKey) {
            this.impl = new cloudbase.core.file.FileUtil.FileInfo(firstKey.impl, lastKey.impl);
        }

        public Text getFirstRow() {
            return impl.getFirstRow();
        }

        public Text getLastRow() {
            return impl.getLastRow();
        }
    }

    private static String createTmpDir(AccumuloConfiguration acuConf, FileSystem fs) throws IOException {
        throw new UnsupportedOperationException();
    }

    public static Collection<String> reduceFiles(AccumuloConfiguration acuConf, Configuration conf, FileSystem fs, Text prevEndRow, Text endRow,
                                                 Collection<String> mapFiles, int maxFiles, String tmpDir, int pass) throws IOException {
        return cloudbase.core.file.FileUtil.reduceFiles(conf, fs, prevEndRow, endRow, mapFiles, maxFiles, tmpDir, pass);
    }

    @SuppressWarnings("unchecked")
    public static SortedMap<Double, Key> findMidPoint(FileSystem fs, AccumuloConfiguration acuConf, Text prevEndRow, Text endRow, Collection<String> mapFiles,
                                                      double minSplit) throws IOException {
        return TransformedSortedMap.decorateTransform(cloudbase.core.file.FileUtil.findMidPoint(prevEndRow, endRow, mapFiles, minSplit),
                new Transformer() {
                    @Override
                    public Object transform(Object input) {
                        return (Double) input;
                    }
                },
                new Transformer() {
                    @Override
                    public Object transform(Object input) {
                        return new Key((cloudbase.core.data.Key) input);
                    }
                }
        );
    }

    public static double estimatePercentageLTE(FileSystem fs, AccumuloConfiguration acuconf, Text prevEndRow, Text endRow, Collection<String> mapFiles,
                                               Text splitRow) throws IOException {
        return cloudbase.core.file.FileUtil.estimatePercentageLTE(prevEndRow, endRow, mapFiles, splitRow);
    }

    @SuppressWarnings("unchecked")
    public static SortedMap<Double, Key> findMidPoint(FileSystem fs, AccumuloConfiguration acuConf, Text prevEndRow, Text endRow, Collection<String> mapFiles,
                                                      double minSplit, boolean useIndex) throws IOException {
        return TransformedSortedMap.decorateTransform(cloudbase.core.file.FileUtil.findMidPoint(prevEndRow, endRow, mapFiles, minSplit, useIndex),
                new Transformer() {
                    @Override
                    public Object transform(Object input) {
                        return (Double) input;
                    }
                },
                new Transformer() {
                    @Override
                    public Object transform(Object input) {
                        return new Key((cloudbase.core.data.Key) input);
                    }
                }
        );
    }

    private static void cleanupIndexOp(AccumuloConfiguration acuConf, String tmpDir, FileSystem fs, ArrayList<FileSKVIterator> readers) throws IOException {
        throw new UnsupportedOperationException();
    }

    private static long countIndexEntries(AccumuloConfiguration acuConf, Text prevEndRow, Text endRow, Collection<String> mapFiles, boolean useIndex,
                                          Configuration conf, FileSystem fs, ArrayList<FileSKVIterator> readers) throws IOException {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    public static Map<String, FileInfo> tryToGetFirstAndLastRows(FileSystem fs, AccumuloConfiguration acuConf, Set<String> mapfiles) {
        return TransformedSortedMap.decorateTransform(cloudbase.core.file.FileUtil.tryToGetFirstAndLastRows(mapfiles),
                new Transformer() {
                    @Override
                    public Object transform(Object input) {
                        return (String) input;
                    }
                },
                new Transformer() {
                    @Override
                    public Object transform(Object input) {
                        return new FileInfo((cloudbase.core.file.FileUtil.FileInfo) input);
                    }
                }
        );
    }

    public static FileSystem getFileSystem(Configuration conf, AccumuloConfiguration acuconf) throws IOException {
        throw new UnsupportedOperationException();
    }
}
