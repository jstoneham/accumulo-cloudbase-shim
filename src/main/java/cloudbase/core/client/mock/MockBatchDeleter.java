package cloudbase.core.client.mock;

import cloudbase.core.client.BatchDeleter;
import cloudbase.core.client.BatchWriter;
import cloudbase.core.client.MutationsRejectedException;
import cloudbase.core.client.TableNotFoundException;
import cloudbase.core.data.Key;
import cloudbase.core.data.Mutation;
import cloudbase.core.data.Value;
import cloudbase.core.security.Authorizations;
import cloudbase.core.security.ColumnVisibility;

import java.util.Map.Entry;

public class MockBatchDeleter extends MockBatchScanner implements BatchDeleter {

    private final MockCloudbase cb;
    private final String tableName;

    public MockBatchDeleter(MockCloudbase cb, String tableName, Authorizations auths) {
        super(cb.tables.get(tableName), auths);
        this.cb = cb;
        this.tableName = tableName;
    }

    @Override
    public void delete() throws MutationsRejectedException, TableNotFoundException {
//        BatchWriter writer = new MockBatchWriter(cb, tableName);
//        try {
//            for (Entry<Key, Value> next : this) {
//                Key k = next.getKey();
//                Mutation m = new Mutation(k.getRow());
//                m.putDelete(k.getColumnFamily(), k.getColumnQualifier(), new ColumnVisibility(k.getColumnVisibility()), k.getTimestamp());
//                writer.addMutation(m);
//            }
//        } finally {
//            writer.close();
//        }
    }
}
