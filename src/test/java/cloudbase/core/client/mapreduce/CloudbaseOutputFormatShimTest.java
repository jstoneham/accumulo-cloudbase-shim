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

import cloudbase.core.client.BatchWriter;
import cloudbase.core.client.Connector;
import cloudbase.core.client.Scanner;
import cloudbase.core.client.mock.MockInstance;
import cloudbase.core.data.Key;
import cloudbase.core.data.Mutation;
import cloudbase.core.data.Value;
import cloudbase.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 */
public class CloudbaseOutputFormatShimTest {

    static class TestMapper extends Mapper<Key, Value, Text, Mutation> {
        Key key = null;
        int count = 0;

        @Override
        protected void map(Key k, Value v, Context context) throws IOException, InterruptedException {
            if (key != null)
                assertEquals(key.getRow().toString(), new String(v.get()));
            assertEquals(k.getRow(), new Text(String.format("%09x", count + 1)));
            assertEquals(new String(v.get()), String.format("%09x", count));
            key = new Key(k);
            count++;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            Mutation m = new Mutation("total");
            m.put("", "", Integer.toString(count));
            try {
                context.write(new Text(), m);
            } catch (NullPointerException e) {
            }
        }
    }

    @Test
    public void testMR() throws Exception {
        MockInstance mockInstance = new MockInstance("testmrinstance");
        Connector c = mockInstance.getConnector("root", new byte[]{});
        c.tableOperations().create("testtable1");
        c.tableOperations().create("testtable2");
        BatchWriter bw = c.createBatchWriter("testtable1", 10000L, 1000L, 4);
        for (int i = 0; i < 100; i++) {
            Mutation m = new Mutation(new Text(String.format("%09x", i + 1)));
            m.put(new Text(), new Text(), new Value(String.format("%09x", i).getBytes()));
            bw.addMutation(m);
        }
        bw.close();

        Job job = new Job();
        job.setInputFormatClass(CloudbaseInputFormat.class);
        job.setMapperClass(TestMapper.class);
        job.setOutputFormatClass(CloudbaseOutputFormatShim.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Mutation.class);
        job.setNumReduceTasks(0);
        Configuration conf = job.getConfiguration();
        CloudbaseInputFormat.setInputInfo(job, "root", "".getBytes(), "testtable1", new Authorizations());
        CloudbaseInputFormat.setMockInstance(job, "testmrinstance");
        CloudbaseOutputFormatShim.setOutputInfo(job, "root", "".getBytes(), false, "testtable2");
        CloudbaseOutputFormatShim.setMockInstance(job, "testmrinstance");

        CloudbaseInputFormat input = new CloudbaseInputFormat();
        List<InputSplit> splits = input.getSplits(job);
        assertEquals(splits.size(), 1);

        CloudbaseOutputFormatShim output = new CloudbaseOutputFormatShim();

        TestMapper mapper = (TestMapper) job.getMapperClass().newInstance();
        for (InputSplit split : splits) {
            TaskAttemptID id = new TaskAttemptID();
            TaskAttemptContext tac = new TaskAttemptContext(job.getConfiguration(), id);
            RecordReader<Key, Value> reader = input.createRecordReader(split, tac);
            RecordWriter<Text, Mutation> writer = output.getRecordWriter(tac);
            Mapper<Key, Value, Text, Mutation>.Context context = mapper.new Context(job.getConfiguration(), id, reader, writer, null, null, split);
            reader.initialize(split, context);
            mapper.run(context);
            writer.close(context);
        }

        Scanner scanner = c.createScanner("testtable2", new Authorizations());
        Iterator<Map.Entry<Key, Value>> iter = scanner.iterator();
        assertTrue(iter.hasNext());
        Map.Entry<Key, Value> entry = iter.next();
        assertEquals(Integer.parseInt(new String(entry.getValue().get())), 100);
        assertFalse(iter.hasNext());
    }
}
