package org.apache.accumulo.core.client.mapreduce.lib.partition;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

public class KeyRangePartitioner extends Partitioner<Key,Writable> implements Configurable {

    public final cloudbase.core.client.mapreduce.lib.partition.KeyRangePartitioner impl;

    public KeyRangePartitioner(cloudbase.core.client.mapreduce.lib.partition.KeyRangePartitioner impl) {
        this.impl = impl;
    }

    public void setConf(Configuration entries) {
        impl.setConf(entries);
    }

    public Configuration getConf() {
        return impl.getConf();
    }

    @Override
    public int getPartition(Key key, Writable writable, int i) {
        return impl.getPartition(key.impl, writable, i);
    }
}
