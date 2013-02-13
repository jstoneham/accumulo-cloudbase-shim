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
package org.apache.accumulo.core.client.admin;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.util.BulkImportHelper;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.*;

public interface TableOperations {

    public SortedSet<String> list();

    public boolean exists(String tableName);

    public void create(String tableName) throws AccumuloException, AccumuloSecurityException, TableExistsException;

    public void create(String tableName, boolean versioningIter, TimeType timeType) throws AccumuloException, AccumuloSecurityException, TableExistsException;

    public void addSplits(String tableName, SortedSet<Text> partitionKeys) throws TableNotFoundException, AccumuloException, AccumuloSecurityException;

    public Collection<Text> getSplits(String tableName) throws TableNotFoundException;

    public Collection<Text> getSplits(String tableName, int maxSplits) throws TableNotFoundException;

    public void compact(String tableName, Text start, Text end, boolean flush, boolean wait) throws AccumuloSecurityException, TableNotFoundException,
            AccumuloException;

    public void delete(String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException;

    public void rename(String oldTableName, String newTableName) throws AccumuloSecurityException, TableNotFoundException, AccumuloException,
            TableExistsException;

    public void flush(String tableName) throws AccumuloException, AccumuloSecurityException;

    public void setProperty(String tableName, String property, String value) throws AccumuloException, AccumuloSecurityException;

    public void removeProperty(String tableName, String property) throws AccumuloException, AccumuloSecurityException;

    public Iterable<Map.Entry<String, String>> getProperties(String tableName) throws AccumuloException, TableNotFoundException;

    public void setLocalityGroups(String tableName, Map<String, Set<Text>> groups) throws AccumuloException, AccumuloSecurityException, TableNotFoundException;

    public Map<String, Set<Text>> getLocalityGroups(String tableName) throws AccumuloException, TableNotFoundException;

    public Set<Range> splitRangeByTablets(String tableName, Range range, int maxSplits) throws AccumuloException, AccumuloSecurityException,
            TableNotFoundException;

    public void importDirectory(String tableName, String dir, String failureDir, boolean setTime) throws TableNotFoundException, IOException, AccumuloException,
        AccumuloSecurityException;

    public BulkImportHelper.AssignmentStats importDirectory(String tableName, String dir, String failureDir, int numThreads, int numAssignThreads, boolean disableGC)
        throws IOException, AccumuloException, AccumuloSecurityException;

    public void offline(String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException;

    public void online(String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException;

    public void clearLocatorCache(String tableName) throws TableNotFoundException;

    public Map<String, String> tableIdMap();

    public void attachIterator(String tableName, IteratorSetting setting) throws AccumuloSecurityException, AccumuloException, TableNotFoundException;

    public void attachIterator(String tableName, IteratorSetting setting, EnumSet<IteratorUtil.IteratorScope> scopes) throws AccumuloSecurityException, AccumuloException,
            TableNotFoundException;

    public void removeIterator(String tableName, String name, EnumSet<IteratorUtil.IteratorScope> scopes) throws AccumuloSecurityException, AccumuloException,
            TableNotFoundException;

    public IteratorSetting getIteratorSetting(String tableName, String name, IteratorUtil.IteratorScope scope) throws AccumuloSecurityException, AccumuloException,
            TableNotFoundException;

    public Map<String, EnumSet<IteratorUtil.IteratorScope>> listIterators(String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException;

    public void checkIteratorConflicts(String tableName, IteratorSetting setting, EnumSet<IteratorUtil.IteratorScope> scopes) throws AccumuloException, TableNotFoundException;
}
