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
import org.apache.commons.collections.Transformer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.log4j.Level;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public abstract class InputFormatBase<K, V> extends InputFormat<K, V> {

    public final cloudbase.core.client.mapreduce.CloudbaseInputFormat impl;

    public InputFormatBase() {
        this.impl = new cloudbase.core.client.mapreduce.CloudbaseInputFormat();
    }

    public static void setIsolated(Configuration conf, boolean enable) {
        CloudbaseInputFormatShim.setIsolated(new JobContextShim(conf), enable);
    }

    public static void setInputInfo(Configuration conf, String user, byte[] passwd, String table, Authorizations auths) {
        CloudbaseInputFormatShim.setInputInfo(new JobContextShim(conf), user, passwd, table, auths.impl);
    }

    public static void setZooKeeperInstance(Configuration conf, String instanceName, String zooKeepers) {
        CloudbaseInputFormatShim.setZooKeeperInstance(new JobContextShim(conf), instanceName, zooKeepers);
    }

    public static void setMockInstance(Configuration conf, String instanceName) {
        CloudbaseInputFormatShim.setMockInstance(new JobContextShim(conf), instanceName);
    }

    public static void setRanges(Configuration conf, Collection<Range> ranges) {
        List<cloudbase.core.data.Range> convertedRanges = new ArrayList<cloudbase.core.data.Range>(ranges.size());
        for (Range r : ranges) {
            convertedRanges.add(r.impl);
        }
        CloudbaseInputFormatShim.setRanges(new JobContextShim(conf), convertedRanges);
    }

    public static void disableAutoAdjustRanges(Configuration conf) {
        CloudbaseInputFormatShim.disableAutoAdjustRanges(new JobContextShim(conf));
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

    public static void fetchColumns(Configuration conf, Collection<Pair<Text, Text>> columnFamilyColumnQualifierPairs) {
        List<cloudbase.core.util.Pair<Text, Text>> convertedPairs = new ArrayList<cloudbase.core.util.Pair<Text, Text>>(columnFamilyColumnQualifierPairs.size());
        for (Pair<Text, Text> pair : columnFamilyColumnQualifierPairs) {
            convertedPairs.add(new cloudbase.core.util.Pair<Text, Text>(pair.getFirst(), pair.getSecond()));
        }
        CloudbaseInputFormatShim.fetchColumns(new JobContextShim(conf), convertedPairs);
    }

    public static void setLogLevel(Configuration conf, Level level) {
        CloudbaseInputFormatShim.setLogLevel(new JobContextShim(conf), level);
    }

    public static void addIterator(Configuration conf, IteratorSetting cfg) {
        JobContext job = new JobContextShim(conf);
        CloudbaseInputFormatShim.setIterator(job, cfg.getPriority(), cfg.getIteratorClass(), cfg.getName());
        for (Map.Entry<String, String> option : cfg.getOptions().entrySet()) {
            CloudbaseInputFormatShim.setIteratorOption(job, cfg.getName(), option.getKey(), option.getValue());
        }
    }

    public static void setIterator(JobContext job, int priority, String iteratorClass, String iteratorName) {
        CloudbaseInputFormatShim.setIterator(job, priority, iteratorClass, iteratorName);
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
