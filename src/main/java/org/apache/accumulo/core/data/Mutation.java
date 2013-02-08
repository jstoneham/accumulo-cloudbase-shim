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
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Mutation implements Writable {

    public final cloudbase.core.data.Mutation impl;

    public Mutation(Text row) {
        this.impl = new cloudbase.core.data.Mutation(row);
    }

    public Mutation(CharSequence row) {
        this.impl = new cloudbase.core.data.Mutation(row);
    }

    public Mutation() {
        this.impl = new cloudbase.core.data.Mutation();
    }

    public Mutation(Mutation m) {
        this.impl = new cloudbase.core.data.Mutation(m.impl);
    }

    public byte[] getRow() {
        return impl.getRow();
    }

    public void put(Text columnFamily, Text columnQualifier, Value value) {
        impl.put(columnFamily, columnQualifier, value.impl);
    }

    public void put(Text columnFamily, Text columnQualifier, ColumnVisibility columnVisibility, Value value) {
        impl.put(columnFamily, columnQualifier, toNative(columnVisibility), value.impl);
    }

    private cloudbase.core.security.ColumnVisibility toNative(ColumnVisibility columnVisibility) {
        return new cloudbase.core.security.ColumnVisibility(columnVisibility.getExpression());
    }

    public void put(Text columnFamily, Text columnQualifier, long timestamp, Value value) {
        impl.put(columnFamily, columnQualifier, timestamp, value.impl);
    }

    public void put(Text columnFamily, Text columnQualifier, ColumnVisibility columnVisibility, long timestamp, Value value) {
        impl.put(columnFamily, columnQualifier, toNative(columnVisibility), timestamp, value.impl);
    }

    public void putDelete(Text columnFamily, Text columnQualifier) {
        impl.putDelete(columnFamily, columnQualifier);
    }

    public void putDelete(Text columnFamily, Text columnQualifier, ColumnVisibility columnVisibility) {
        impl.putDelete(columnFamily, columnQualifier, toNative(columnVisibility));
    }

    public void putDelete(Text columnFamily, Text columnQualifier, long timestamp) {
        impl.putDelete(columnFamily, columnQualifier, timestamp);
    }

    public void putDelete(Text columnFamily, Text columnQualifier, ColumnVisibility columnVisibility, long timestamp) {
        impl.putDelete(columnFamily, columnQualifier, toNative(columnVisibility), timestamp);
    }

    public void put(CharSequence columnFamily, CharSequence columnQualifier, Value value) {
        impl.put(columnFamily, columnQualifier, value.impl);
    }

    public void put(CharSequence columnFamily, CharSequence columnQualifier, ColumnVisibility columnVisibility, Value value) {
        impl.put(columnFamily, columnQualifier, toNative(columnVisibility), value.impl);
    }

    public void put(CharSequence columnFamily, CharSequence columnQualifier, long timestamp, Value value) {
        impl.put(columnFamily, columnQualifier, timestamp, value.impl);
    }

    public void put(CharSequence columnFamily, CharSequence columnQualifier, ColumnVisibility columnVisibility, long timestamp, Value value) {
        impl.put(columnFamily, columnQualifier, toNative(columnVisibility), timestamp, value.impl);
    }

    public void putDelete(CharSequence columnFamily, CharSequence columnQualifier) {
        impl.putDelete(columnFamily, columnQualifier);
    }

    public void putDelete(CharSequence columnFamily, CharSequence columnQualifier, ColumnVisibility columnVisibility) {
        impl.putDelete(columnFamily, columnQualifier, toNative(columnVisibility));
    }

    public void putDelete(CharSequence columnFamily, CharSequence columnQualifier, long timestamp) {
        impl.putDelete(columnFamily, columnQualifier, timestamp);
    }

    public void putDelete(CharSequence columnFamily, CharSequence columnQualifier, ColumnVisibility columnVisibility, long timestamp) {
        impl.putDelete(columnFamily, columnQualifier, toNative(columnVisibility), timestamp);
    }

    public void put(CharSequence columnFamily, CharSequence columnQualifier, CharSequence value) {
        impl.put(columnFamily, columnQualifier, value);
    }

    public void put(CharSequence columnFamily, CharSequence columnQualifier, ColumnVisibility columnVisibility, CharSequence value) {
        impl.put(columnFamily, columnQualifier, toNative(columnVisibility), value);
    }

    public void put(CharSequence columnFamily, CharSequence columnQualifier, long timestamp, CharSequence value) {
        impl.put(columnFamily, columnQualifier, timestamp, value);
    }

    public void put(CharSequence columnFamily, CharSequence columnQualifier, ColumnVisibility columnVisibility, long timestamp, CharSequence value) {
        impl.put(columnFamily, columnQualifier, toNative(columnVisibility), timestamp, value);
    }

    public List<ColumnUpdate> getUpdates() {
        List<cloudbase.core.data.ColumnUpdate> updates = impl.getUpdates();
        List<ColumnUpdate> results = new ArrayList<ColumnUpdate>(updates.size());
        for (cloudbase.core.data.ColumnUpdate update : updates) {
            results.add(new ColumnUpdate(update));
        }
        return results;
    }

    public long numBytes() {
        return impl.numBytes();
    }

    public long estimatedMemoryUsed() {
        return impl.estimatedMemoryUsed();
    }

    public int size() {
        return impl.size();
    }

    public void readFields(DataInput in) throws IOException {
        impl.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        impl.write(out);
    }

    public boolean equals(Object o) {
        if (!(o instanceof Mutation)) {
            return false;
        }
        return impl.equals(((Mutation) o).impl);
    }

    public int hashCode() {
        return impl.hashCode();
    }

    public boolean equals(Mutation m) {
        return impl.equals(m.impl);
    }

    public String toString() {
        return impl.toString();
    }
}
