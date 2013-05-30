package org.apache.accumulo.core.iterators.user;

import com.texeltek.accumulocloudbaseshim.SortedKeyValueIteratorShim;
import org.apache.accumulo.core.data.Value;

public class RowDeletingIterator extends SortedKeyValueIteratorShim {

    public static final Value DELETE_ROW_VALUE = new Value(cloudbase.core.iterators.RowDeletingIterator.DELETE_ROW_VALUE);

    public RowDeletingIterator(cloudbase.core.iterators.RowDeletingIterator impl) {
        super(impl);
    }
}
