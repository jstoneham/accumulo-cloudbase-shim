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

import cloudbase.core.client.BatchScanner;
import cloudbase.core.data.Key;
import cloudbase.core.data.Value;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * The intention of this iterator is the wrap the iterator that is returned by a
 * BatchScan in Accumulo in order to serve as a workaround for
 * ACCUMULO-226 (https://issues.apache.org/jira/browse/ACCUMULO-226).  The bug
 * involves subsequent calls to hasNext() on batch scan results after false has been
 * returned will return true
 * <p/>
 * A patch has been submitted and accepted in Accumulo but this wrapper can be used
 * for previous versions of Accumulo that do not yet have the patch.
 */
public class CloudbaseBatchScannerCloseableIterator implements Closeable, Iterator<Map.Entry<Key, Value>> {

    private final BatchScanner batchScanner;
    private final Iterator<Map.Entry<Key, Value>> scannerIterator;
    private Map.Entry<Key, Value> nextKeyValue = null;

    public CloudbaseBatchScannerCloseableIterator(BatchScanner scanner) {
        this.batchScanner = scanner;
        this.scannerIterator = scanner.iterator();
    }

    public boolean hasNext() {
        if (nextKeyValue == null && scannerIterator.hasNext()) {
            nextKeyValue = scannerIterator.next();
        }
        return !isTerminatingKeyValue(nextKeyValue);
    }

    private boolean isTerminatingKeyValue(Map.Entry<Key, Value> nextEntry) {
        if (nextEntry == null) {
            return true;
        }
        return !(nextEntry.getKey() != null && nextEntry.getValue() != null); //Condition taken from Accumulo's TabletServerBatchReaderIterator
    }

    public Map.Entry<Key, Value> next() {
        if (hasNext()) {
            Map.Entry<Key, Value> entry = nextKeyValue;
            nextKeyValue = null;
            return entry;
        } else {
            throw new NoSuchElementException();
        }
    }

    public void remove() {
        scannerIterator.remove();
    }

    /**
     * Close the batch scanner on which we're iterating.
     */
    public void close() {
        batchScanner.close();
    }
}
