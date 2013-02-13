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

import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import cloudbase.core.conf.Property;
import cloudbase.core.iterators.aggregation.conf.AggregatorConfiguration;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.util.BulkImportHelper;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class TableOperationsShim implements TableOperations {
    public final cloudbase.core.client.admin.TableOperations impl;
    public final cloudbase.core.client.Connector connector;

    public TableOperationsShim(cloudbase.core.client.admin.TableOperations impl,
                               cloudbase.core.client.Connector connector) {
        this.impl = impl;
        this.connector = connector;
    }

    public SortedSet<String> list() {
        return impl.list();
    }

    public boolean exists(String tableName) {
        return impl.exists(tableName);
    }

    public void create(String tableName) throws AccumuloException, AccumuloSecurityException, TableExistsException {
        try {
            impl.create(tableName);
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        } catch (cloudbase.core.client.TableExistsException e) {
            throw new TableExistsException(e);
        }
    }

    public void create(String tableName, boolean versioningIter, TimeType timeType) throws AccumuloException, AccumuloSecurityException, TableExistsException {
        try {
            impl.create(tableName, new TreeSet<Text>(), new ArrayList<AggregatorConfiguration>(), cloudbase.core.client.admin.TimeType.valueOf(timeType.name()));
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        } catch (cloudbase.core.client.TableExistsException e) {
            throw new TableExistsException(e);
        }
    }

    public void addSplits(String tableName, SortedSet<Text> partitionKeys) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
        try {
            impl.addSplits(tableName, partitionKeys);
        } catch (cloudbase.core.client.TableNotFoundException e) {
            throw new TableNotFoundException(e);
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    public Collection<Text> getSplits(String tableName) throws TableNotFoundException {
        try {
            return impl.getSplits(tableName);
        } catch (cloudbase.core.client.TableNotFoundException e) {
            throw new TableNotFoundException(e);
        }
    }

    public Collection<Text> getSplits(String tableName, int maxSplits) throws TableNotFoundException {
        try {
            return impl.getSplits(tableName, maxSplits);
        } catch (cloudbase.core.client.TableNotFoundException e) {
            throw new TableNotFoundException(e);
        }
    }

    public void compact(String tableName, Text start, Text end, boolean flush, boolean wait) throws AccumuloSecurityException, TableNotFoundException, AccumuloException {
        Date now = new Date(System.currentTimeMillis());
        SimpleDateFormat dateParser = new SimpleDateFormat("yyyyMMddHHmmssz", Locale.getDefault());
        String nowStr = dateParser.format(now);
        try {
            for (Map.Entry<String, String> prop : connector.tableOperations().getProperties(tableName)) {
                if (prop.getKey().equals(Property.TABLE_MAJC_COMPACTALL_AT.getKey())) {
                    if (dateParser.parse(prop.getValue()).after(now)) {
                        return;
                    } else {
                        break;
                    }
                }
            }

            connector.tableOperations().flush(tableName);
            connector.tableOperations().setProperty(tableName, Property.TABLE_MAJC_COMPACTALL_AT.getKey(), nowStr);
        } catch (ParseException ex) {
            throw new AccumuloException(ex);
        } catch (cloudbase.core.client.TableNotFoundException e) {
            throw new TableNotFoundException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        } catch (CBException e) {
            throw new AccumuloException(e);
        }
    }

    public void delete(String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        try {
            impl.delete(tableName);
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        } catch (cloudbase.core.client.TableNotFoundException e) {
            throw new TableNotFoundException(e);
        }
    }

    public void rename(String oldTableName, String newTableName) throws AccumuloSecurityException, TableNotFoundException, AccumuloException, TableExistsException {
        try {
            impl.rename(oldTableName, newTableName);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        } catch (cloudbase.core.client.TableNotFoundException e) {
            throw new TableNotFoundException(e);
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (cloudbase.core.client.TableExistsException e) {
            throw new TableExistsException(e);
        }
    }

    public void flush(String tableName) throws AccumuloException, AccumuloSecurityException {
        try {
            impl.flush(tableName);
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    public void setProperty(String tableName, String property, String value) throws AccumuloException, AccumuloSecurityException {
        try {
            impl.setProperty(tableName, property, value);
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    public void removeProperty(String tableName, String property) throws AccumuloException, AccumuloSecurityException {
        try {
            impl.removeProperty(tableName, property);
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    public Iterable<Map.Entry<String, String>> getProperties(String tableName) throws AccumuloException, TableNotFoundException {
        try {
            return impl.getProperties(tableName);
        } catch (cloudbase.core.client.TableNotFoundException e) {
            throw new TableNotFoundException(e);
        }
    }

    public void setLocalityGroups(String tableName, Map<String, Set<Text>> groups) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        try {
            impl.setLocalityGroups(tableName, groups);
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        } catch (cloudbase.core.client.TableNotFoundException e) {
            throw new TableNotFoundException(e);
        }
    }

    public Map<String, Set<Text>> getLocalityGroups(String tableName) throws AccumuloException, TableNotFoundException {
        try {
            return impl.getLocalityGroups(tableName);
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (cloudbase.core.client.TableNotFoundException e) {
            throw new TableNotFoundException(e);
        }
    }

    public Set<Range> splitRangeByTablets(String tableName, Range range, int maxSplits) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        try {
            Set<cloudbase.core.data.Range> splits = impl.splitRangeByTablets(tableName, range.impl, maxSplits);
            Set<Range> results = new HashSet<Range>();
            for (cloudbase.core.data.Range split : splits) {
                results.add(new Range(split));
            }
            return results;
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        } catch (cloudbase.core.client.TableNotFoundException e) {
            throw new TableNotFoundException(e);
        }
    }

    @Override
    public void importDirectory(String tableName, String dir, String failureDir, boolean setTime) throws TableNotFoundException, IOException, AccumuloException, AccumuloSecurityException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BulkImportHelper.AssignmentStats importDirectory(String tableName, String dir, String failureDir, int numThreads, int numAssignThreads, boolean disableGC) throws IOException, AccumuloException, AccumuloSecurityException {
        try {
            return new BulkImportHelper.AssignmentStats(impl.importDirectory(tableName, dir, failureDir, numThreads, numAssignThreads, disableGC));
        } catch (CBException e) {
            throw new AccumuloException(e);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        }
    }

    public void offline(String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        try {
            impl.offline(tableName);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        } catch (CBException e) {
            throw new AccumuloException(e);
        }
    }

    public void online(String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        try {
            impl.online(tableName);
        } catch (CBSecurityException e) {
            throw new AccumuloSecurityException(e);
        } catch (CBException e) {
            throw new AccumuloException(e);
        }
    }

    public void clearLocatorCache(String tableName) throws TableNotFoundException {
        try {
            impl.clearLocatorCache(tableName);
        } catch (cloudbase.core.client.TableNotFoundException e) {
            throw new TableNotFoundException(e);
        }
    }

    public Map<String, String> tableIdMap() {
        return impl.tableIdMap();
    }

    public void attachIterator(String tableName, IteratorSetting setting) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        // TODO
    }

    public void attachIterator(String tableName, IteratorSetting setting, EnumSet<IteratorUtil.IteratorScope> scopes) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        // TODO
    }

    public void removeIterator(String tableName, String name, EnumSet<IteratorUtil.IteratorScope> scopes) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        // TODO
    }

    public IteratorSetting getIteratorSetting(String tableName, String name, IteratorUtil.IteratorScope scope) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        // TODO
        return null;
    }

    public Map<String, EnumSet<IteratorUtil.IteratorScope>> listIterators(String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        // TODO
        return null;
    }

    public void checkIteratorConflicts(String tableName, IteratorSetting setting, EnumSet<IteratorUtil.IteratorScope> scopes) throws AccumuloException, TableNotFoundException {
        // TODO
    }

    public String toString() {
        return impl.toString();
    }
}
