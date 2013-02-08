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
package org.apache.accumulo.core.client;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public interface ScannerBase extends Iterable<Map.Entry<Key, Value>> {

    public void addScanIterator(IteratorSetting cfg);

    public void removeScanIterator(String iteratorName);

    public void updateScanIteratorOption(String iteratorName, String key, String value);

    public void setScanIterators(int priority, String iteratorClass, String iteratorName) throws IOException;

    public void setScanIteratorOption(String iteratorName, String key, String value);

    public void setupRegex(String iteratorName, int iteratorPriority) throws IOException;

    public void setRowRegex(String regex);

    public void setColumnFamilyRegex(String regex);

    public void setColumnQualifierRegex(String regex);

    public void setValueRegex(String regex);

    public void fetchColumnFamily(Text col);

    public void fetchColumn(Text colFam, Text colQual);

    public void clearColumns();

    public void clearScanIterators();

    public Iterator<Map.Entry<Key, Value>> iterator();
}
