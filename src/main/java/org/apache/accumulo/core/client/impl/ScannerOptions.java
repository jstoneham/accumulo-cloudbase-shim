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
package org.apache.accumulo.core.client.impl;

import cloudbase.core.client.impl.ScannerOptionsShim;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public class ScannerOptions implements ScannerBase {

    private final cloudbase.core.client.impl.ScannerOptions impl;

    public ScannerOptions() {
        impl = new cloudbase.core.client.impl.ScannerOptionsShim();
    }

    public ScannerOptions(ScannerOptions so) {
        this();
        ScannerOptionsShim.setOptions(impl, so.impl);
    }

    public synchronized void addScanIterator(IteratorSetting si) {
        impl.setScanIterators(si.getPriority(), si.getIteratorClass(), si.getName());
        for (Map.Entry<String, String> option : si.getOptions().entrySet()) {
            impl.setScanIteratorOption(si.getName(), option.getKey(), option.getValue());
        }
    }

    public synchronized void removeScanIterator(String iteratorName) {
        throw new UnsupportedOperationException();
    }

    public void setScanIterators(int priority, String iteratorClass, String iteratorName) {
        impl.setScanIterators(priority, iteratorClass, iteratorName);
    }

    public synchronized void setScanIteratorOption(String iteratorName, String key, String value) {
        impl.setScanIteratorOption(iteratorName, key, value);
    }

    public synchronized void updateScanIteratorOption(String iteratorName, String key, String value) {
        impl.setScanIteratorOption(iteratorName, key, value);
    }

    public synchronized void setupRegex(String iteratorName, int iteratorPriority) throws IOException {
        impl.setupRegex(iteratorName, iteratorPriority);
    }

    public synchronized void setRowRegex(String regex) {
        impl.setRowRegex(regex);
    }

    public synchronized void setColumnFamilyRegex(String regex) {
        impl.setColumnFamilyRegex(regex);
    }

    public synchronized void setColumnQualifierRegex(String regex) {
        impl.setColumnQualifierRegex(regex);
    }

    public synchronized void setValueRegex(String regex) {
        impl.setValueRegex(regex);
    }

    public synchronized void fetchColumnFamily(Text col) {
        impl.fetchColumnFamily(col);
    }

    public synchronized void fetchColumn(Text colFam, Text colQual) {
        impl.fetchColumn(colFam, colQual);
    }

    public synchronized void fetchColumn(Column column) {
        impl.fetchColumn(column.impl);
    }

    public synchronized void clearColumns() {
        impl.clearColumns();
    }

    public synchronized SortedSet<Column> getFetchedColumns() {
        SortedSet<Column> result = new TreeSet<Column>();
        SortedSet<cloudbase.core.data.Column> originals = impl.getFetchedColumns();
        for (cloudbase.core.data.Column column : originals) {
            result.add(new Column(column));
        }
        return result;
    }

    public synchronized void clearScanIterators() {
        impl.clearScanIterators();
    }

    public Iterator<Map.Entry<Key, Value>> iterator() {
        throw new UnsupportedOperationException();
    }

    public String toString() {
        return impl.toString();
    }
}
