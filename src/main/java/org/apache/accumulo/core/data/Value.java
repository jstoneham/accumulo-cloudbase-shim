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
package org.apache.accumulo.core.data;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class Value implements WritableComparable<Object> {
    public final cloudbase.core.data.Value impl;

    public Value() {
        impl = new cloudbase.core.data.Value();
    }

    public Value(byte[] bytes) {
        impl = new cloudbase.core.data.Value(bytes, false);
    }

    public Value(byte[] bytes, boolean copy) {
        impl = new cloudbase.core.data.Value(bytes, copy);
    }

    public Value(final Value ibw) {
        impl = new cloudbase.core.data.Value(ibw.impl);
    }

    public Value(final byte[] newData, final int offset, final int length) {
        impl = new cloudbase.core.data.Value(newData, offset, length);
    }

    public Value(cloudbase.core.data.Value impl) {
        this.impl = impl;
    }

    public byte[] get() {
        return impl.get();
    }

    public void set(final byte[] b) {
        impl.set(b);
    }

    public void copy(byte[] b) {
        impl.copy(b);
    }

    public int getSize() {
        return impl.getSize();
    }

    public void readFields(final DataInput in) throws IOException {
        impl.readFields(in);
    }

    public void write(final DataOutput out) throws IOException {
        impl.write(out);
    }

    public int hashCode() {
        return impl.hashCode();
    }

    public int compareTo(Object right_obj) {
        return impl.compareTo(((Value) right_obj).impl);
    }

    public int compareTo(final byte[] that) {
        return impl.compareTo(that);
    }

    public boolean equals(Object right_obj) {
        if (!(right_obj instanceof Value)) {
            return false;
        }
        return impl.equals(((Value) right_obj).impl);
    }

    public String toString() {
        return impl.toString();
    }

    public static class Comparator extends WritableComparator {
        private BytesWritable.Comparator comparator = new BytesWritable.Comparator();

        public Comparator() {
            super(Value.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return comparator.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    static { // register this comparator
        WritableComparator.define(Value.class, new Comparator());
    }

    public static byte[][] toArray(final List<byte[]> array) {
        // List#toArray doesn't work on lists of byte [].
        byte[][] results = new byte[array.size()][];
        for (int i = 0; i < array.size(); i++) {
            results[i] = array.get(i);
        }
        return results;
    }
}
