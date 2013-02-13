package org.apache.accumulo.core.client.mapreduce.lib.partition;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

public class RangePartitioner extends Partitioner<Text,Writable> implements Configurable {

    public final cloudbase.core.client.mapreduce.lib.partition.RangePartitioner impl;

    public RangePartitioner(cloudbase.core.client.mapreduce.lib.partition.RangePartitioner impl) {
        this.impl = impl;
    }

    public void setConf(Configuration entries) {
        impl.setConf(entries);
    }

    public Configuration getConf() {
        return impl.getConf();
    }

    @Override
    public int getPartition(Text text, Writable writable, int i) {
        return impl.getPartition(text, writable, i);
    }
}
