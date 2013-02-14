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
package org.apache.accumulo.core.client.mapreduce;

import com.texeltek.accumulocloudbaseshim.RecordReaderShim;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AccumuloInputFormat extends InputFormatBase<Key, Value> {

    public RecordReader<Key, Value> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new RecordReaderShim(impl.createRecordReader(split, context));
    }

    public static class RangeInputSplit extends InputSplit implements Writable {
        public final cloudbase.core.client.mapreduce.CloudbaseInputFormat.RangeInputSplit impl;

        public RangeInputSplit() {
            this.impl = new cloudbase.core.client.mapreduce.CloudbaseInputFormat.RangeInputSplit();
        }

        public Range getRange() {
            throw new UnsupportedOperationException();
        }

        public static float getProgress(ByteSequence start, ByteSequence end, ByteSequence position) {
            return cloudbase.core.client.mapreduce.CloudbaseInputFormat.RangeInputSplit.getProgress(start.impl, end.impl, position.impl);
        }

        public float getProgress(Key currentKey) {
            return impl.getProgress(currentKey.impl);
        }

        public long getLength() throws IOException {
            return this.impl.getLength();
        }

        public String[] getLocations() throws IOException {
            return impl.getLocations();
        }

        public void readFields(DataInput in) throws IOException {
            impl.readFields(in);
        }

        public void write(DataOutput out) throws IOException {
            impl.write(out);
        }
    }
}