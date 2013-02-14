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

import cloudbase.core.client.mapreduce.CloudbaseOutputFormat;
import com.texeltek.accumulocloudbaseshim.JobContextShim;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.log4j.Level;

import java.io.IOException;

public class AccumuloOutputFormat extends OutputFormat<Text, Mutation> {
    private final cloudbase.core.client.mapreduce.CloudbaseOutputFormat impl =
            new cloudbase.core.client.mapreduce.CloudbaseOutputFormat();

    public static void setOutputInfo(JobContext job, String user, byte[] passwd, boolean createTables, String defaultTable) {
        CloudbaseOutputFormat.setOutputInfo(job, user, passwd, createTables, defaultTable);
    }

    public static void setOutputInfo(Configuration conf, String user, byte[] passwd, boolean createTables, String defaultTable) {
        CloudbaseOutputFormat.setOutputInfo(new JobContextShim(conf), user, passwd, createTables, defaultTable);
    }

    public static void setZooKeeperInstance(JobContext job, String instanceName, String zooKeepers) {
        CloudbaseOutputFormat.setZooKeeperInstance(job, instanceName, zooKeepers);
    }

    public static void setZooKeeperInstance(Configuration conf, String instanceName, String zooKeepers) {
        CloudbaseOutputFormat.setZooKeeperInstance(new JobContextShim(conf), instanceName, zooKeepers);
    }

    public static void setMockInstance(JobContext job, String instanceName) {
        throw new UnsupportedOperationException();
    }

    public static void setMockInstance(Configuration conf, String instanceName) {
        throw new UnsupportedOperationException();
    }

    public static void setMaxMutationBufferSize(JobContext job, long numberOfBytes) {
        CloudbaseOutputFormat.setMaxMutationBufferSize(job, numberOfBytes);
    }

    public static void setMaxMutationBufferSize(Configuration conf, long numberOfBytes) {
        CloudbaseOutputFormat.setMaxMutationBufferSize(new JobContextShim(conf), numberOfBytes);
    }

    public static void setMaxLatency(JobContext job, int numberOfMilliseconds) {
        CloudbaseOutputFormat.setMaxLatency(job, numberOfMilliseconds);
    }

    public static void setMaxLatency(Configuration conf, int numberOfMilliseconds) {
        CloudbaseOutputFormat.setMaxLatency(new JobContextShim(conf), numberOfMilliseconds);
    }

    public static void setMaxWriteThreads(JobContext job, int numberOfThreads) {
        CloudbaseOutputFormat.setMaxWriteThreads(job, numberOfThreads);
    }

    public static void setMaxWriteThreads(Configuration conf, int numberOfThreads) {
        CloudbaseOutputFormat.setMaxWriteThreads(new JobContextShim(conf), numberOfThreads);
    }

    public static void setLogLevel(JobContext job, Level level) {
        CloudbaseOutputFormat.setLogLevel(job, level);
    }

    public static void setLogLevel(Configuration conf, Level level) {
        CloudbaseOutputFormat.setLogLevel(new JobContextShim(conf), level);
    }

    public static void setSimulationMode(JobContext job) {
        CloudbaseOutputFormat.setSimulationMode(job);
    }

    public static void setSimulationMode(Configuration conf) {
        CloudbaseOutputFormat.setSimulationMode(new JobContextShim(conf));
    }

    public void checkOutputSpecs(JobContext job) throws IOException {
        impl.checkOutputSpecs(job);
    }

    public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
        return impl.getOutputCommitter(context);
    }

    public RecordWriter<Text, Mutation> getRecordWriter(TaskAttemptContext attempt) throws IOException {
        final RecordWriter<Text, cloudbase.core.data.Mutation> implWriter = impl.getRecordWriter(attempt);
        return new RecordWriter<Text, Mutation>() {
            public void write(Text text, Mutation mutation) throws IOException, InterruptedException {
                implWriter.write(text, mutation.impl);
            }

            public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
                implWriter.close(taskAttemptContext);
            }
        };
    }

    public String toString() {
        return impl.toString();
    }
}
