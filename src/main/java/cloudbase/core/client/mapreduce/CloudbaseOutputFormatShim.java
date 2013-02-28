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
package cloudbase.core.client.mapreduce;

import cloudbase.core.client.*;
import cloudbase.core.client.mock.MockInstance;
import cloudbase.core.client.mock.MockMultiTableBatchWriter;
import cloudbase.core.data.ColumnUpdate;
import cloudbase.core.data.KeyExtent;
import cloudbase.core.data.Mutation;
import cloudbase.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

/**
 * Add mock output support to the {@link CloudbaseOutputFormat}
 */
public class CloudbaseOutputFormatShim extends CloudbaseOutputFormat {

    private static final Logger log = Logger.getLogger(CloudbaseOutputFormatShim.class);
    private static final String PREFIX = CloudbaseOutputFormat.class.getSimpleName();
    private static final String MOCK = PREFIX + ".useMockInstance";
    private static final String INSTANCE_HAS_BEEN_SET = PREFIX + ".instanceConfigured";
    private static final String INSTANCE_NAME = PREFIX + ".instanceName";

    protected static Instance getInstance(JobContext job) {
        Configuration conf = job.getConfiguration();
        if (isMock(conf)) {
            return new MockInstance(conf.get(INSTANCE_NAME));
        }
        return CloudbaseOutputFormat.getInstance(job);
    }

    protected static boolean isMock(Configuration conf) {
        return conf.getBoolean(MOCK, false);
    }

    public static void setMockInstance(JobContext job, String instanceName) {
        Configuration conf = job.getConfiguration();
        conf.setBoolean(INSTANCE_HAS_BEEN_SET, true);
        conf.setBoolean(MOCK, true);
        conf.set(INSTANCE_NAME, instanceName);
    }

    @Override
    public RecordWriter<Text, Mutation> getRecordWriter(TaskAttemptContext attempt)
            throws IOException {
        if (isMock(attempt.getConfiguration())) {
            try {
                return new MockCloudbaseRecordWriter(attempt);
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
        return super.getRecordWriter(attempt);
    }

    @Override
    public void checkOutputSpecs(JobContext job) throws IOException {
        if (isMock(job.getConfiguration())) {
            try {
                Connector c = CloudbaseOutputFormatShim.getInstance(job).getConnector(getUsername(job), getPassword(job));
                if (!c.securityOperations().authenticateUser(getUsername(job), getPassword(job)))
                    throw new IOException("Unable to authenticate user");
            } catch (CBException e) {
                throw new IOException(e);
            } catch (CBSecurityException e) {
                throw new IOException(e);
            }
        } else {
            super.checkOutputSpecs(job);
        }
    }

    protected static class MockCloudbaseRecordWriter extends RecordWriter<Text, Mutation> {

        private MultiTableBatchWriter mtbw;
        private HashMap bws;
        private Text defaultTableName;
        private boolean simulate, createTables;
        private long mutCount, valCount;
        private Connector conn;

        public void write(Text table, Mutation mutation)
                throws IOException {
            if (table == null || table.toString().isEmpty())
                table = defaultTableName;
            if (!simulate && table == null)
                throw new IOException("No table or default table specified. Try simulation mode next ti" +
                        "me"
                );
            mutCount++;
            valCount += mutation.size();
            printMutation(table, mutation);
            if (simulate)
                return;
            if (!bws.containsKey(table))
                try {
                    List l = null;
                    addTable(table, l);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new IOException(e);
                }
            try {
                ((BatchWriter) bws.get(table)).addMutation(mutation);
            } catch (MutationsRejectedException e) {
                throw new IOException(e);
            }
        }

        public void addTable(Text key, List aggregators)
                throws CBException, CBSecurityException {
            if (simulate) {
                log.info((new StringBuilder()).append("Simulating adding table: ").append(key).toString());
                return;
            }
            log.debug((new StringBuilder()).append("Adding table: ").append(key).toString());
            BatchWriter bw = null;
            String table = key.toString();
            if (createTables && !conn.tableOperations().exists(table))
                try {
                    if (aggregators == null)
                        conn.tableOperations().create(table);
                    else
                        conn.tableOperations().create(table, aggregators);
                } catch (CBSecurityException e) {
                    log.error((new StringBuilder()).append("Cloudbase security violation creating ").append(table).toString(), e);
                    throw e;
                } catch (TableExistsException e) {
                }
            try {
                bw = mtbw.getBatchWriter(table);
            } catch (TableNotFoundException e) {
                log.error((new StringBuilder()).append("Cloudbase table ").append(table).append(" doesn't exist and cannot be created.").toString(), e);
                throw new CBException(e);
            } catch (CBException e) {
                throw e;
            } catch (CBSecurityException e) {
                throw e;
            }
            if (bw != null)
                bws.put(key, bw);
        }

        private int printMutation(Text table, Mutation m) {
            if (log.isTraceEnabled()) {
                log.trace(String.format("Table %s row key: %s", new Object[]{
                        table, hexDump(m.getRow())
                }));
                ColumnUpdate cu;
                for (Iterator i$ = m.getUpdates().iterator(); i$.hasNext(); log.trace(String.format("Table %s value: %s", new Object[]{
                        table, hexDump(cu.getValue())
                }))) {
                    cu = (ColumnUpdate) i$.next();
                    log.trace(String.format("Table %s column: %s:%s", new Object[]{
                            table, hexDump(cu.getColumnFamily()), hexDump(cu.getColumnQualifier())
                    }));
                    log.trace(String.format("Table %s security: %s", new Object[]{
                            table, (new ColumnVisibility(cu.getColumnVisibility())).toString()
                    }));
                }

            }
            return m.getUpdates().size();
        }

        private String hexDump(byte ba[]) {
            StringBuilder sb = new StringBuilder();
            byte arr$[] = ba;
            int len$ = arr$.length;
            for (int i$ = 0; i$ < len$; i$++) {
                byte b = arr$[i$];
                if (b > 32 && b < 126)
                    sb.append((char) b);
                else
                    sb.append(String.format("x%02x", new Object[]{
                            Byte.valueOf(b)
                    }));
            }

            return sb.toString();
        }

        public void close(TaskAttemptContext attempt)
                throws IOException, InterruptedException {
            log.debug((new StringBuilder()).append("mutations written: ").append(mutCount).append(", values written: ").append(valCount).toString());
            if (simulate)
                return;
            try {
                mtbw.close();
            } catch (MutationsRejectedException e) {
                if (e.getAuthorizationFailures().size() >= 0) {
                    HashSet tables = new HashSet();
                    KeyExtent ke;
                    for (Iterator i$ = e.getAuthorizationFailures().iterator(); i$.hasNext(); tables.add(ke.getTableId().toString()))
                        ke = (KeyExtent) i$.next();

                    log.error((new StringBuilder()).append("Not authorized to write to tables : ").append(tables).toString());
                }
                if (e.getConstraintViolationSummaries().size() > 0)
                    log.error((new StringBuilder()).append("Constraint violations : ").append(e.getConstraintViolationSummaries().size()).toString());
            }
        }

        MockCloudbaseRecordWriter(TaskAttemptContext attempt)
                throws CBException, CBSecurityException {
            mtbw = null;
            bws = null;
            defaultTableName = null;
            simulate = false;
            createTables = false;
            mutCount = 0L;
            valCount = 0L;
            Level l = CloudbaseOutputFormat.getLogLevel(attempt);
            if (l != null)
                log.setLevel(CloudbaseOutputFormat.getLogLevel(attempt));
            simulate = CloudbaseOutputFormat.getSimulationMode(attempt);
            createTables = CloudbaseOutputFormat.canCreateTables(attempt);
            if (simulate)
                log.info("Simulating output only. No writes to tables will occur");
            bws = new HashMap();
            String tname = CloudbaseOutputFormat.getDefaultTableName(attempt);
            defaultTableName = tname != null ? new Text(tname) : null;
            if (!simulate) {
                conn = CloudbaseOutputFormatShim.getInstance(attempt).getConnector(CloudbaseOutputFormat.getUsername(attempt), CloudbaseOutputFormat.getPassword(attempt));
                mtbw = new MockMultiTableBatchWriter(conn, CloudbaseOutputFormat.getMaxMutationBufferSize(attempt), CloudbaseOutputFormat.getMaxLatency(attempt), CloudbaseOutputFormat.getMaxWriteThreads(attempt));
            }
        }
    }
}
