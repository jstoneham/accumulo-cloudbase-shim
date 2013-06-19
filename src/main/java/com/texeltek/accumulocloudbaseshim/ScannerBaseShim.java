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

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Map;

public abstract class ScannerBaseShim implements ScannerBase {
    private final cloudbase.core.client.ScannerBase baseImpl;

    public ScannerBaseShim(cloudbase.core.client.ScannerBase baseImpl) {
        this.baseImpl = baseImpl;
    }

    public void addScanIterator(IteratorSetting cfg) {
        try {
            IteratorSetting translated = IteratorTranslation.translate(cfg);
            baseImpl.setScanIterators(translated.getPriority(), translated.getIteratorClass(), translated.getName());
            for (Map.Entry<String, String> option : translated.getOptions().entrySet()) {
                baseImpl.setScanIteratorOption(translated.getName(), option.getKey(), option.getValue());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void removeScanIterator(String iteratorName) {
        throw new UnsupportedOperationException("Cloudbase 1.3 does not support removing scan iterators");
    }

    public void updateScanIteratorOption(String iteratorName, String key, String value) {
        baseImpl.setScanIteratorOption(iteratorName, key, value);
    }

    public void setScanIterators(int i, String s, String s1) throws IOException {
        baseImpl.setScanIterators(i, s, s1);
    }

    public void setScanIteratorOption(String s, String s1, String s2) {
        baseImpl.setScanIteratorOption(s, s1, s2);
    }

    public void setupRegex(String s, int i) throws IOException {
        baseImpl.setupRegex(s, i);
    }

    public void setRowRegex(String s) {
        baseImpl.setRowRegex(s);
    }

    public void setColumnFamilyRegex(String s) {
        baseImpl.setColumnFamilyRegex(s);
    }

    public void setColumnQualifierRegex(String s) {
        baseImpl.setColumnQualifierRegex(s);
    }

    public void setValueRegex(String s) {
        baseImpl.setValueRegex(s);
    }

    public void fetchColumnFamily(Text text) {
        baseImpl.fetchColumnFamily(text);
    }

    public void fetchColumn(Text text, Text text1) {
        baseImpl.fetchColumn(text, text1);
    }

    public void clearColumns() {
        baseImpl.clearColumns();
    }

    public void clearScanIterators() {
        baseImpl.clearScanIterators();
    }

    public String toString() {
        return baseImpl.toString();
    }
}
