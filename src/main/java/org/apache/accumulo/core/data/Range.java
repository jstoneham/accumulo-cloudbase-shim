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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Range implements WritableComparable<Range> {
    public final cloudbase.core.data.Range impl;

    public Range() {
        this.impl = new cloudbase.core.data.Range();
    }

    public Range(Key startKey, Key endKey) {
        this.impl = new cloudbase.core.data.Range(startKey.impl, endKey.impl);
    }

    public Range(CharSequence row) {
        this.impl = new cloudbase.core.data.Range(row);
    }

    public Range(Text row) {
        this.impl = new cloudbase.core.data.Range(row);
    }

    public Range(Text startRow, Text endRow) {
        this.impl = new cloudbase.core.data.Range(startRow, endRow);
    }

    public Range(CharSequence startRow, CharSequence endRow) {
        this.impl = new cloudbase.core.data.Range(startRow, endRow);
    }

    public Range(Text startRow, boolean startRowInclusive, Text endRow, boolean endRowInclusive) {
        this.impl = new cloudbase.core.data.Range(startRow, startRowInclusive, endRow, endRowInclusive);
    }

    public Range(CharSequence startRow, boolean startRowInclusive, CharSequence endRow, boolean endRowInclusive) {
        this.impl = new cloudbase.core.data.Range(startRow, startRowInclusive, endRow, endRowInclusive);
    }

    public Range(Key startKey, boolean startKeyInclusive, Key endKey, boolean endKeyInclusive) {
        this.impl = new cloudbase.core.data.Range(startKey.impl, startKeyInclusive, endKey.impl, endKeyInclusive);
    }

    public Range(Range range) {
        this.impl = new cloudbase.core.data.Range(range.impl);
    }

    public Range(Key start, Key stop, boolean startKeyInclusive, boolean stopKeyInclusive, boolean infiniteStartKey, boolean infiniteStopKey) {
        this.impl = new cloudbase.core.data.Range(start.impl, stop.impl, startKeyInclusive, stopKeyInclusive, infiniteStartKey, infiniteStopKey);
    }

    public Range(cloudbase.core.data.Range impl) {
        this.impl = impl;
    }

    public Key getStartKey() {
        return new Key(impl.getStartKey());
    }

    public boolean beforeStartKey(Key key) {
        return impl.beforeStartKey(key.impl);
    }

    public Key getEndKey() {
        return new Key(impl.getEndKey());
    }

    public boolean afterEndKey(Key key) {
        return impl.afterEndKey(key.impl);
    }

    public int hashCode() {
        return impl.hashCode();
    }

    public boolean equals(Object o) {
        if (!(o instanceof Range)) {
            return false;
        }
        return impl.equals(((Range) o).impl);
    }

    public boolean equals(Range otherRange) {
        return impl.equals(otherRange.impl);
    }

    public int compareTo(Range o) {
        return impl.compareTo(o.impl);
    }

    public boolean contains(Key key) {
        return impl.contains(key.impl);
    }

    public static List<Range> mergeOverlapping(Collection<Range> ranges) {
        List<cloudbase.core.data.Range> toMerge = new ArrayList<cloudbase.core.data.Range>(ranges.size());
        for (Range range : ranges) {
            toMerge.add(range.impl);
        }
        List<cloudbase.core.data.Range> merged = cloudbase.core.data.Range.mergeOverlapping(toMerge);
        List<Range> result = new ArrayList<Range>(merged.size());
        for (cloudbase.core.data.Range range : merged) {
            result.add(new Range(range));
        }
        return result;
    }

    public Range clip(Range range) {
        return new Range(impl.clip(range.impl, false));
    }

    public Range clip(Range range, boolean returnNullIfDisjoint) {
        return new Range(impl.clip(range.impl, returnNullIfDisjoint));
    }

    public Range bound(Column min, Column max) {
        return new Range(impl.bound(min.impl, max.impl));
    }

    public String toString() {
        return impl.toString();
    }

    public void readFields(DataInput in) throws IOException {
        impl.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        impl.write(out);
    }

    public boolean isStartKeyInclusive() {
        return impl.isStartKeyInclusive();
    }

    public boolean isEndKeyInclusive() {
        return impl.isEndKeyInclusive();
    }

    public boolean isInfiniteStartKey() {
        return impl.isInfiniteStartKey();
    }

    public boolean isInfiniteStopKey() {
        return impl.isInfiniteStopKey();
    }

    public static Range exact(Text row) {
        return new Range(row);
    }

    public static Range exact(Text row, Text cf) {
        Key startKey = new Key(row, cf);
        return new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM), false);
    }

    public static Range exact(Text row, Text cf, Text cq) {
        Key startKey = new Key(row, cf, cq);
        return new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL), false);
    }

    public static Range exact(Text row, Text cf, Text cq, Text cv) {
        Key startKey = new Key(row, cf, cq, cv);
        return new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS), false);
    }

    public static Range exact(Text row, Text cf, Text cq, Text cv, long ts) {
        Key startKey = new Key(row, cf, cq, cv, ts);
        return new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME), false);
    }

    public static Text followingPrefix(Text prefix) {
        byte[] prefixBytes = prefix.getBytes();

        // find the last byte in the array that is not 0xff
        int changeIndex = prefix.getLength() - 1;
        while (changeIndex >= 0 && prefixBytes[changeIndex] == (byte) 0xff)
            changeIndex--;
        if (changeIndex < 0)
            return null;

        // copy prefix bytes into new array
        byte[] newBytes = new byte[changeIndex + 1];
        System.arraycopy(prefixBytes, 0, newBytes, 0, changeIndex + 1);

        // increment the selected byte
        newBytes[changeIndex]++;
        return new Text(newBytes);
    }

    public static Range prefix(Text rowPrefix) {
        Text fp = followingPrefix(rowPrefix);
        return new Range(new Key(rowPrefix), true, fp == null ? null : new Key(fp), false);
    }

    public static Range prefix(Text row, Text cfPrefix) {
        Text fp = followingPrefix(cfPrefix);
        return new Range(new Key(row, cfPrefix), true, fp == null ? new Key(row).followingKey(PartialKey.ROW) : new Key(row, fp), false);
    }

    public static Range prefix(Text row, Text cf, Text cqPrefix) {
        Text fp = followingPrefix(cqPrefix);
        return new Range(new Key(row, cf, cqPrefix), true, fp == null ? new Key(row, cf).followingKey(PartialKey.ROW_COLFAM) : new Key(row, cf, fp), false);
    }

    public static Range prefix(Text row, Text cf, Text cq, Text cvPrefix) {
        Text fp = followingPrefix(cvPrefix);
        return new Range(new Key(row, cf, cq, cvPrefix), true, fp == null ? new Key(row, cf, cq).followingKey(PartialKey.ROW_COLFAM_COLQUAL) : new Key(row, cf, cq,
                fp), false);
    }

    public static Range exact(CharSequence row) {
        return Range.exact(new Text(row.toString()));
    }

    public static Range exact(CharSequence row, CharSequence cf) {
        return Range.exact(new Text(row.toString()), new Text(cf.toString()));
    }

    public static Range exact(CharSequence row, CharSequence cf, CharSequence cq) {
        return Range.exact(new Text(row.toString()), new Text(cf.toString()), new Text(cq.toString()));
    }

    public static Range exact(CharSequence row, CharSequence cf, CharSequence cq, CharSequence cv) {
        return Range.exact(new Text(row.toString()), new Text(cf.toString()), new Text(cq.toString()), new Text(cv.toString()));
    }

    public static Range exact(CharSequence row, CharSequence cf, CharSequence cq, CharSequence cv, long ts) {
        return Range.exact(new Text(row.toString()), new Text(cf.toString()), new Text(cq.toString()), new Text(cv.toString()), ts);
    }

    public static Range prefix(CharSequence rowPrefix) {
        return Range.prefix(new Text(rowPrefix.toString()));
    }

    public static Range prefix(CharSequence row, CharSequence cfPrefix) {
        return Range.prefix(new Text(row.toString()), new Text(cfPrefix.toString()));
    }

    public static Range prefix(CharSequence row, CharSequence cf, CharSequence cqPrefix) {
        return Range.prefix(new Text(row.toString()), new Text(cf.toString()), new Text(cqPrefix.toString()));
    }

    public static Range prefix(CharSequence row, CharSequence cf, CharSequence cq, CharSequence cvPrefix) {
        return Range.prefix(new Text(row.toString()), new Text(cf.toString()), new Text(cq.toString()), new Text(cvPrefix.toString()));
    }
}
