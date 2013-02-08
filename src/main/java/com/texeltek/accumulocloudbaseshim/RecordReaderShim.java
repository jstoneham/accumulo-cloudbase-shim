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

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class RecordReaderShim extends RecordReader<Key, Value> {
    private final RecordReader<cloudbase.core.data.Key, cloudbase.core.data.Value> impl;

    public RecordReaderShim(RecordReader<cloudbase.core.data.Key, cloudbase.core.data.Value> impl) {
        this.impl = impl;
    }

    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        impl.initialize(inputSplit, taskAttemptContext);
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        return impl.nextKeyValue();
    }

    public Key getCurrentKey() throws IOException, InterruptedException {
        return new Key(impl.getCurrentKey());
    }

    public Value getCurrentValue() throws IOException, InterruptedException {
        return new Value((impl.getCurrentValue()));
    }

    public float getProgress() throws IOException, InterruptedException {
        return impl.getProgress();
    }

    public void close() throws IOException {
        impl.close();
    }

    public String toString() {
        return impl.toString();
    }
}
