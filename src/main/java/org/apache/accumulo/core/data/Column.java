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

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Column implements WritableComparable<Column> {

    public final cloudbase.core.data.Column impl;

    public int compareTo(Column that) {
        return impl.compareTo(that.impl);
    }

    public void readFields(DataInput in) throws IOException {
        impl.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        impl.write(out);
    }

    public Column() {
        impl = new cloudbase.core.data.Column();
    }

    public Column(cloudbase.core.data.Column impl) {
        this.impl = impl;
    }

    public Column(byte[] columnFamily, byte[] columnQualifier, byte[] columnVisibility) {
        impl = new cloudbase.core.data.Column(columnFamily, columnQualifier, columnVisibility);
    }

    public boolean equals(Object that) {
        if (!(that instanceof Column)) {
            return false;
        }
        return impl.equals(((Column) that).impl);
    }

    public boolean equals(Column that) {
        return impl.equals(that.impl);
    }

    public int hashCode() {
        return impl.hashCode();
    }

    public byte[] getColumnFamily() {
        return impl.getColumnFamily();
    }

    public byte[] getColumnQualifier() {
        return impl.getColumnQualifier();
    }

    public byte[] getColumnVisibility() {
        return impl.getColumnVisibility();
    }

    public String toString() {
        return impl.toString();
    }
}
