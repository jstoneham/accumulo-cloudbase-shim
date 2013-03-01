package org.apache.accumulo.core.client.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.junit.Test;

public class AccumuloOutputFormatTest {

    static class TestMapper extends Mapper<Key,Value,Text,Mutation> {
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
            } catch (NullPointerException e) {}
        }
    }

    @Test
    public void testMR() throws Exception {
        MockInstance mockInstance = new MockInstance("testmrinstance");
        Connector c = mockInstance.getConnector("root", new byte[] {});
        c.tableOperations().create("testtable1");
        c.tableOperations().create("testtable2");
        c.securityOperations().grantTablePermission("root", "testtable1", TablePermission.READ);
        c.securityOperations().grantTablePermission("root", "testtable2", TablePermission.READ);
        BatchWriter bw = c.createBatchWriter("testtable1", 10000L, 1000L, 4);
        for (int i = 0; i < 100; i++) {
            Mutation m = new Mutation(new Text(String.format("%09x", i + 1)));
            m.put(new Text(), new Text(), new Value(String.format("%09x", i).getBytes()));
            bw.addMutation(m);
        }
        bw.close();

        Job job = new Job();
        job.setInputFormatClass(AccumuloInputFormat.class);
        job.setMapperClass(TestMapper.class);
        job.setOutputFormatClass(AccumuloOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Mutation.class);
        job.setNumReduceTasks(0);
        Configuration conf = job.getConfiguration();
        AccumuloInputFormat.setInputInfo(conf, "root", "".getBytes(), "testtable1", new Authorizations());
        AccumuloInputFormat.setMockInstance(conf, "testmrinstance");
        AccumuloOutputFormat.setOutputInfo(conf, "root", "".getBytes(), false, "testtable2");
        AccumuloOutputFormat.setMockInstance(conf, "testmrinstance");

        AccumuloInputFormat input = new AccumuloInputFormat();
        List<InputSplit> splits = input.getSplits(job);
        assertEquals(splits.size(), 1);

        AccumuloOutputFormat output = new AccumuloOutputFormat();

        TestMapper mapper = (TestMapper) job.getMapperClass().newInstance();
        for (InputSplit split : splits) {
            TaskAttemptID id = new TaskAttemptID();
            TaskAttemptContext tac = new TaskAttemptContext(job.getConfiguration(), id);
            RecordReader<Key,Value> reader = input.createRecordReader(split, tac);
            RecordWriter<Text,Mutation> writer = output.getRecordWriter(tac);
            Mapper<Key,Value,Text,Mutation>.Context context = mapper.new Context(job.getConfiguration(), id, reader, writer, null, null, split);
            reader.initialize(split, context);
            mapper.run(context);
            writer.close(context);
        }

        Scanner scanner = c.createScanner("testtable2", new Authorizations());
        Iterator<Entry<Key,Value>> iter = scanner.iterator();
        assertTrue(iter.hasNext());
        Entry<Key,Value> entry = iter.next();
        assertEquals(Integer.parseInt(new String(entry.getValue().get())), 100);
        assertFalse(iter.hasNext());
    }
}
