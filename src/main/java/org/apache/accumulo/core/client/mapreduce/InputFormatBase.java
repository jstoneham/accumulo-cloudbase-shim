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
package org.apache.accumulo.core.client.mapreduce;

import cloudbase.core.client.mapreduce.CloudbaseInputFormatShim;
import com.texeltek.accumulocloudbaseshim.InstanceShim;
import com.texeltek.accumulocloudbaseshim.IteratorTranslation;
import com.texeltek.accumulocloudbaseshim.JobContextShim;
import com.texeltek.accumulocloudbaseshim.TabletLocatorShim;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.SetUtils;
import org.apache.commons.collections.Transformer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.log4j.Level;

import java.io.IOException;
import java.util.*;

public abstract class InputFormatBase<K, V> extends InputFormat<K, V> {

    public final cloudbase.core.client.mapreduce.CloudbaseInputFormat impl;

    public InputFormatBase() {
        this.impl = new cloudbase.core.client.mapreduce.CloudbaseInputFormat();
    }

    public static void setIsolated(JobContext job, boolean enable) {
        CloudbaseInputFormatShim.setIsolated(job, enable);
    }

    public static void setIsolated(Configuration conf, boolean enable) {
        setIsolated(new JobContextShim(conf), enable);
    }

    public static void setInputInfo(JobContext job, String user, byte[] passwd, String table, Authorizations auths) {
        CloudbaseInputFormatShim.setInputInfo(job, user, passwd, table, auths.impl);
    }

    public static void setInputInfo(Configuration conf, String user, byte[] passwd, String table, Authorizations auths) {
        setInputInfo(new JobContextShim(conf), user, passwd, table, auths);
    }

    public static void setZooKeeperInstance(JobContext job, String instanceName, String zooKeepers) {
        CloudbaseInputFormatShim.setZooKeeperInstance(job, instanceName, zooKeepers);
    }

    public static void setZooKeeperInstance(Configuration conf, String instanceName, String zooKeepers) {
        setZooKeeperInstance(new JobContextShim(conf), instanceName, zooKeepers);
    }

    public static void setMockInstance(JobContext job, String instanceName) {
        CloudbaseInputFormatShim.setMockInstance(job, instanceName);
    }

    public static void setMockInstance(Configuration conf, String instanceName) {
        setMockInstance(new JobContextShim(conf), instanceName);
    }

    @SuppressWarnings("unchecked")
    public static void setRanges(JobContext job, Collection<Range> ranges) {
        CloudbaseInputFormatShim.setRanges(job,
                CollectionUtils.collect(ranges,
                        new Transformer() {
                            @Override
                            public Object transform(Object input) {
                                return ((Range) input).impl;
                            }
                        }));
    }

    public static void setRanges(Configuration conf, Collection<Range> ranges) {
        setRanges(new JobContextShim(conf), ranges);
    }

    public static void disableAutoAdjustRanges(JobContext job) {
        CloudbaseInputFormatShim.disableAutoAdjustRanges(job);
    }

    public static void disableAutoAdjustRanges(Configuration conf) {
        disableAutoAdjustRanges(new JobContextShim(conf));
    }

    public static enum RegexType {
        ROW, COLUMN_FAMILY, COLUMN_QUALIFIER, VALUE
    }

    public static void setRegex(JobContext job, RegexType type, String regex) {
        CloudbaseInputFormatShim.setRegex(job, CloudbaseInputFormatShim.RegexType.valueOf(type.name()), regex);
    }

    public static void setMaxVersions(Configuration conf, int maxVersions) throws IOException {
        CloudbaseInputFormatShim.setMaxVersions(new JobContextShim(conf), maxVersions);
    }

    public static void setScanOffline(Configuration conf, boolean scanOff) {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    public static void fetchColumns(JobContext job, Collection<Pair<Text, Text>> columnFamilyColumnQualifierPairs) {
        CloudbaseInputFormatShim.fetchColumns(job,
                CollectionUtils.collect(columnFamilyColumnQualifierPairs,
                        new Transformer() {
                            @Override
                            public Object transform(Object input) {
                                return ((Pair) input).impl;
                            }
                        }));
    }

    public static void fetchColumns(Configuration conf, Collection<Pair<Text, Text>> columnFamilyColumnQualifierPairs) {
        fetchColumns(new JobContextShim(conf), columnFamilyColumnQualifierPairs);
    }

    public static void setLogLevel(JobContext job, Level level) {
        CloudbaseInputFormatShim.setLogLevel(job, level);
    }

    public static void setLogLevel(Configuration conf, Level level) {
        setLogLevel(new JobContextShim(conf), level);
    }

    public static void addIterator(JobContext job, IteratorSetting cfg) {
        IteratorSetting translated = IteratorTranslation.translate(cfg);
        CloudbaseInputFormatShim.setIterator(job, translated.getPriority(),
                translated.getIteratorClass(), translated.getName());
        for (Map.Entry<String, String> option : translated.getOptions().entrySet()) {
            CloudbaseInputFormatShim.setIteratorOption(job, translated.getName(), option.getKey(), option.getValue());
        }
    }

    public static void addIterator(Configuration conf, IteratorSetting cfg) {
        addIterator(new JobContextShim(conf), cfg);
    }

    public static void setIterator(JobContext job, int priority, String iteratorClass, String iteratorName) {
        IteratorSetting translated = IteratorTranslation.translate(new IteratorSetting(priority, iteratorName, iteratorClass));
        CloudbaseInputFormatShim.setIterator(job, translated.getPriority(),
                translated.getIteratorClass(), translated.getName());
    }

    public static void setIteratorOption(JobContext job, String iteratorName, String key, String value) {
        CloudbaseInputFormatShim.setIteratorOption(job, iteratorName, key, value);
    }

    public List<InputSplit> getSplits(JobContext job) throws IOException {
        return impl.getSplits(job);
    }

    protected static boolean isIsolated(JobContext job) {
        return CloudbaseInputFormatShim.isIsolated(job);
    }

    protected static boolean isIsolated(Configuration conf) {
        return isIsolated(new JobContextShim(conf));
    }

    protected static boolean usesLocalIterators(JobContext job) {
        throw new UnsupportedOperationException();
    }

    protected static boolean usesLocalIterators(Configuration conf) {
        throw new UnsupportedOperationException();
    }

    protected static String getUsername(JobContext job) {
        return CloudbaseInputFormatShim.getUsername(job);
    }

    protected static String getUsername(Configuration conf) {
        return getUsername(new JobContextShim(conf));
    }

    protected static byte[] getPassword(JobContext job) {
        return CloudbaseInputFormatShim.getPassword(job);
    }

    protected static byte[] getPassword(Configuration conf) {
        return getPassword(new JobContextShim(conf));
    }

    protected static String getTablename(JobContext job) {
        return CloudbaseInputFormatShim.getTablename(job);
    }

    protected static String getTablename(Configuration conf) {
        return getTablename(new JobContextShim(conf));
    }

    protected static Authorizations getAuthorizations(JobContext job) {
        return new Authorizations(CloudbaseInputFormatShim.getAuthorizations(job));
    }

    protected static Authorizations getAuthorizations(Configuration conf) {
        return getAuthorizations(new JobContextShim(conf));
    }

    protected static Instance getInstance(JobContext job) {
        return new InstanceShim(CloudbaseInputFormatShim.getInstance(job));
    }

    protected static Instance getInstance(Configuration conf) {
        return getInstance(new JobContextShim(conf));
    }

    protected static TabletLocator getTabletLocator(JobContext job) throws TableNotFoundException {
        try {
            return new TabletLocatorShim(CloudbaseInputFormatShim.getTabletLocator(job));
        } catch (cloudbase.core.client.TableNotFoundException e) {
            throw new TableNotFoundException(e);
        }
    }

    protected static TabletLocator getTabletLocator(Configuration conf) throws TableNotFoundException {
        return getTabletLocator(new JobContextShim(conf));
    }

    @SuppressWarnings("unchecked")
    protected static List<Range> getRanges(JobContext job) throws IOException {
        return new ArrayList<Range>(CollectionUtils.collect(
                CloudbaseInputFormatShim.getRanges(job),
                new Transformer() {
                    @Override
                    public Object transform(Object input) {
                        return new Range(((cloudbase.core.data.Range) input));
                    }
                }));
    }

    protected static List<Range> getRanges(Configuration conf) throws IOException {
        return getRanges(new JobContextShim(conf));
    }

    @SuppressWarnings("unchecked")
    protected static Set<Pair<Text, Text>> getFetchedColumns(JobContext job) {
        return SetUtils.transformedSet(CloudbaseInputFormatShim.getFetchedColumns(job),
                new Transformer() {
                    @Override
                    public Object transform(Object input) {
                        return ((Pair) input).impl;
                    }
                });
    }

    protected static Set<Pair<Text, Text>> getFetchedColumns(Configuration conf) {
        return getFetchedColumns(new JobContextShim(conf));
    }

    protected static boolean getAutoAdjustRanges(JobContext job) {
        return CloudbaseInputFormatShim.getAutoAdjustRanges(job);
    }

    protected static boolean getAutoAdjustRanges(Configuration conf) {
        return getAutoAdjustRanges(new JobContextShim(conf));
    }

    protected static Level getLogLevel(JobContext job) {
        return CloudbaseInputFormatShim.getLogLevel(job);
    }

    protected static Level getLogLevel(Configuration conf) {
        return getLogLevel(new JobContextShim(conf));
    }

    protected static void validateOptions(JobContext job) throws IOException {
        CloudbaseInputFormatShim.validateOptions(job);
    }

    protected static void validateOptions(Configuration conf) throws IOException {
        validateOptions(new JobContextShim(conf));
    }

    protected static int getMaxVersions(JobContext job) {
        return CloudbaseInputFormatShim.getMaxVersions(job);
    }

    protected static int getMaxVersions(Configuration conf) {
        return getMaxVersions(new JobContextShim(conf));
    }

    protected static boolean isOfflineScan(Configuration conf) {
        throw new UnsupportedOperationException();
    }
}
