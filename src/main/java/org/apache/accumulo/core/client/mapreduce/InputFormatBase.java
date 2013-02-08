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

import cloudbase.core.client.mapreduce.CloudbaseInputFormat;
import com.texeltek.accumulocloudbaseshim.JobContextShim;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.log4j.Level;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public abstract class InputFormatBase<K, V> extends InputFormat<K, V> {

    public static void setIsolated(Configuration conf, boolean enable) {
        CloudbaseInputFormat.setIsolated(new JobContextShim(conf), enable);
    }

    public static void setInputInfo(Configuration conf, String user, byte[] passwd, String table, Authorizations auths) {
        CloudbaseInputFormat.setInputInfo(new JobContextShim(conf), user, passwd, table, auths.impl);
    }

    public static void setZooKeeperInstance(Configuration conf, String instanceName, String zooKeepers) {
        CloudbaseInputFormat.setZooKeeperInstance(new JobContextShim(conf), instanceName, zooKeepers);
    }

    public static void setMockInstance(Configuration conf, String instanceName) {
        CloudbaseInputFormat.setMockInstance(new JobContextShim(conf), instanceName);
    }

    public static void setRanges(Configuration conf, Collection<Range> ranges) {
        List<cloudbase.core.data.Range> convertedRanges = new ArrayList<cloudbase.core.data.Range>(ranges.size());
        for (Range r : ranges) {
            convertedRanges.add(r.impl);
        }
        CloudbaseInputFormat.setRanges(new JobContextShim(conf), convertedRanges);
    }

    public static void disableAutoAdjustRanges(Configuration conf) {
        CloudbaseInputFormat.disableAutoAdjustRanges(new JobContextShim(conf));
    }

    public static enum RegexType {
        ROW, COLUMN_FAMILY, COLUMN_QUALIFIER, VALUE
    }

    public static void setRegex(JobContext job, RegexType type, String regex) {
        CloudbaseInputFormat.setRegex(job, CloudbaseInputFormat.RegexType.valueOf(type.name()), regex);
    }

    public static void setMaxVersions(Configuration conf, int maxVersions) throws IOException {
        CloudbaseInputFormat.setMaxVersions(new JobContextShim(conf), maxVersions);
    }

    public static void setScanOffline(Configuration conf, boolean scanOff) {
        throw new UnsupportedOperationException();
    }

    public static void fetchColumns(Configuration conf, Collection<Pair<Text, Text>> columnFamilyColumnQualifierPairs) {
        List<cloudbase.core.util.Pair<Text, Text>> convertedPairs = new ArrayList<cloudbase.core.util.Pair<Text, Text>>(columnFamilyColumnQualifierPairs.size());
        for (Pair<Text, Text> pair : columnFamilyColumnQualifierPairs) {
            convertedPairs.add(new cloudbase.core.util.Pair<Text, Text>(pair.getFirst(), pair.getSecond()));
        }
        CloudbaseInputFormat.fetchColumns(new JobContextShim(conf), convertedPairs);
    }

    public static void setLogLevel(Configuration conf, Level level) {
        CloudbaseInputFormat.setLogLevel(new JobContextShim(conf), level);
    }

    public static void addIterator(Configuration conf, IteratorSetting cfg) {
        JobContext job = new JobContextShim(conf);
        CloudbaseInputFormat.setIterator(job, cfg.getPriority(), cfg.getIteratorClass(), cfg.getName());
        for (Map.Entry<String, String> option : cfg.getOptions().entrySet()) {
            CloudbaseInputFormat.setIteratorOption(job, cfg.getName(), option.getKey(), option.getValue());
        }
    }

    public static void setIterator(JobContext job, int priority, String iteratorClass, String iteratorName) {
        CloudbaseInputFormat.setIterator(job, priority, iteratorClass, iteratorName);
    }

    public static void setIteratorOption(JobContext job, String iteratorName, String key, String value) {
        CloudbaseInputFormat.setIteratorOption(job, iteratorName, key, value);
    }
}
