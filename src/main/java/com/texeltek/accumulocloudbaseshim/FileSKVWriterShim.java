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

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVWriter;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Set;

public class FileSKVWriterShim implements FileSKVWriter {

    public final cloudbase.core.file.FileSKVWriter impl;

    public FileSKVWriterShim(cloudbase.core.file.FileSKVWriter impl) {
        this.impl = impl;
    }

    @Override
    public boolean supportsLocalityGroups() {
        return impl.supportsLocalityGroups();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void startNewLocalityGroup(String name, Set<ByteSequence> columnFamilies) throws IOException {
        impl.startNewLocalityGroup(name, ByteSequenceShim.cloudbaseSet(columnFamilies));
    }

    @Override
    public void startDefaultLocalityGroup() throws IOException {
        impl.startDefaultLocalityGroup();
    }

    @Override
    public void append(Key key, Value value) throws IOException {
        impl.append(key.impl, value.impl);
    }

    @Override
    public DataOutputStream createMetaStore(String name) throws IOException {
        return impl.createMetaStore(name);
    }

    @Override
    public void close() throws IOException {
        impl.close();
    }
}
