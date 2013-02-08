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

public class ColumnUpdate {

    public final cloudbase.core.data.ColumnUpdate impl;

    public ColumnUpdate(byte[] cf, byte[] cq, byte[] cv, boolean hasts, long ts, boolean deleted, byte[] val, byte[] data, int tsOffset) {
        this.impl = new cloudbase.core.data.ColumnUpdate(cf, cq, cv, hasts, ts, deleted, val, data, tsOffset);
    }

    public ColumnUpdate(cloudbase.core.data.ColumnUpdate impl) {
        this.impl = impl;
    }

    public void setSystemTimestamp(long v) {
        impl.setSystemTimestamp(v);
    }

    public boolean hasTimestamp() {
        return impl.hasTimestamp();
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

    public long getTimestamp() {
        return impl.getTimestamp();
    }

    public boolean isDeleted() {
        return impl.isDeleted();
    }

    public byte[] getValue() {
        return impl.getValue();
    }

    public String toString() {
        return impl.toString();
    }
}
