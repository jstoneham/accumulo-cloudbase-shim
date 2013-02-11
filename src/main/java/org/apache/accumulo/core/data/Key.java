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

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Key implements WritableComparable<Key>, Cloneable {

    public final cloudbase.core.data.Key impl;

    public boolean equals(Object o) {
        if (!(o instanceof Key)) {
            return false;
        }
        return impl.equals(((Key) o).impl);
    }

    public Key() {
        impl = new cloudbase.core.data.Key();
    }

    public Key(cloudbase.core.data.Key impl) {
        this.impl = impl;
    }

    public Key(Text row) {
        impl = new cloudbase.core.data.Key(row);
    }

    public Key(Text row, long ts) {
        impl = new cloudbase.core.data.Key(row, ts);
    }

    public Key(byte row[], int rOff, int rLen, byte cf[], int cfOff, int cfLen, byte cq[], int cqOff, int cqLen, byte cv[], int cvOff, int cvLen, long ts) {
        impl = new cloudbase.core.data.Key(row, rOff, rLen, cf, cfOff, cfLen, cq, cqOff, cqLen, cv, cvOff, cvLen, ts);
    }

    public Key(byte[] row, byte[] colFamily, byte[] colQualifier, byte[] colVisibility, long timestamp) {
        impl = new cloudbase.core.data.Key(row, colFamily, colQualifier, colVisibility, timestamp);
    }

    public Key(byte[] row, byte[] cf, byte[] cq, byte[] cv, long ts, boolean deleted) {
        impl = new cloudbase.core.data.Key(row, cf, cq, cv, ts, deleted);
    }

    public Key(byte[] row, byte[] cf, byte[] cq, byte[] cv, long ts, boolean deleted, boolean copy) {
        impl = new cloudbase.core.data.Key(row, cf, cq, cv, ts, deleted, copy);
    }

    public Key(Text row, Text cf) {
        impl = new cloudbase.core.data.Key(row, cf);
    }

    public Key(Text row, Text cf, Text cq) {
        impl = new cloudbase.core.data.Key(row, cf, cq);
    }

    public Key(Text row, Text cf, Text cq, Text cv) {
        impl = new cloudbase.core.data.Key(row, cf, cq, cv);
    }

    public Key(Text row, Text cf, Text cq, long ts) {
        impl = new cloudbase.core.data.Key(row, cf, cq, ts);
    }

    public Key(Text row, Text cf, Text cq, Text cv, long ts) {
        impl = new cloudbase.core.data.Key(row, cf, cq, cv, ts);
    }

    public Key(Text row, Text cf, Text cq, ColumnVisibility cv, long ts) {
        impl = new cloudbase.core.data.Key(row, cf, cq, new Text(cv.getExpression()), ts);
    }

    public Key(CharSequence row) {
        impl = new cloudbase.core.data.Key(row);
    }

    public Key(CharSequence row, CharSequence cf) {
        impl = new cloudbase.core.data.Key(row, cf);
    }

    public Key(CharSequence row, CharSequence cf, CharSequence cq) {
        impl = new cloudbase.core.data.Key(row, cf, cq);
    }

    public Key(CharSequence row, CharSequence cf, CharSequence cq, CharSequence cv) {
        impl = new cloudbase.core.data.Key(row, cf, cq, cv);
    }

    public Key(CharSequence row, CharSequence cf, CharSequence cq, long ts) {
        impl = new cloudbase.core.data.Key(row, cf, cq, ts);
    }

    public Key(CharSequence row, CharSequence cf, CharSequence cq, CharSequence cv, long ts) {
        impl = new cloudbase.core.data.Key(row, cf, cq, cv, ts);
    }

    public Key(CharSequence row, CharSequence cf, CharSequence cq, ColumnVisibility cv, long ts) {
        impl = new cloudbase.core.data.Key(row, cf, cq, cv.toString(), ts);
    }

    public Key followingKey(PartialKey part) {
        return new Key(impl.followingKey(translatePartialKey(part)));
    }

    public Key(Key other) {
        this.impl = new cloudbase.core.data.Key(other.impl);
    }

    public Text getRow(Text r) {
        return impl.getRow(r);
    }

    public Text getRow() {
        return impl.getRow();
    }

    public ByteSequence getRowData() {
        return new ArrayByteSequence(impl.getRowData());
    }

    public int compareRow(Text r) {
        return impl.compareRow(r);
    }

    public ByteSequence getColumnFamilyData() {
        return new ArrayByteSequence(impl.getColumnFamilyData());
    }

    public Text getColumnFamily(Text cf) {
        return impl.getColumnFamily(cf);
    }

    public Text getColumnFamily() {
        return impl.getColumnFamily();
    }

    public int compareColumnFamily(Text cf) {
        return impl.compareColumnFamily(cf);
    }

    public ByteSequence getColumnQualifierData() {
        return new ArrayByteSequence(impl.getColumnQualifierData());
    }

    public Text getColumnQualifier(Text cq) {
        return impl.getColumnQualifier(cq);
    }

    public Text getColumnQualifier() {
        return impl.getColumnQualifier();
    }

    public int compareColumnQualifier(Text cq) {
        return impl.compareColumnQualifier(cq);
    }

    public void setTimestamp(long ts) {
        impl.setTimestamp(ts);
    }

    public long getTimestamp() {
        return impl.getTimestamp();
    }

    public boolean isDeleted() {
        return impl.isDeleted();
    }

    public void setDeleted(boolean deleted) {
        impl.setDeleted(deleted);
    }

    public ByteSequence getColumnVisibilityData() {
        return new ArrayByteSequence(impl.getColumnVisibilityData());
    }

    public final Text getColumnVisibility() {
        return impl.getColumnVisibility();
    }

    public final Text getColumnVisibility(Text cv) {
        return impl.getColumnVisibility(cv);
    }

    public void set(Key k) {
        impl.set(k.impl);
    }

    public void readFields(DataInput in) throws IOException {
        impl.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        impl.write(out);
    }

    public boolean equals(Key other, PartialKey part) {
        return impl.equals(other.impl, translatePartialKey(part));
    }

    public int compareTo(Key other, PartialKey part) {
        return impl.compareTo(other.impl, translatePartialKey(part));
    }

    public int compareTo(Key other) {
        return impl.compareTo(other.impl);
    }

    public int hashCode() {
        return impl.hashCode();
    }

    public static String toPrintableString(byte ba[], int offset, int len, int maxLen) {
        return cloudbase.core.data.Key.toPrintableString(ba, offset, len, maxLen);
    }

    public String toString() {
        return impl.toString();
    }

    public String toStringNoTime() {
        return impl.toStringNoTime();
    }

    public int getLength() {
        return impl.getLength();
    }

    public int getSize() {
        return impl.getSize();
    }

    public Object clone() throws CloneNotSupportedException {
        return new Key((cloudbase.core.data.Key) impl.clone());
    }

    private cloudbase.core.data.PartialKey translatePartialKey(PartialKey part) {
        return cloudbase.core.data.PartialKey.valueOf(part.name());
    }
}
