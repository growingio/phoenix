package org.apache.phoenix.expression.aggregator.gio;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.aggregator.BaseAggregator;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;

/**
 * Created by Spirit on 17/6/15.
 */


public class StringMergeAggregator extends BaseAggregator {
    private StringBuffer mergeResult = new StringBuffer();

    public StringMergeAggregator(SortOrder sortOrder) {
        super(sortOrder);
    }

    public StringMergeAggregator(SortOrder sortOrder, ImmutableBytesWritable ptr) {
        this(sortOrder);
        if (ptr != null) {
            mergeResult = new StringBuffer(PVarchar.INSTANCE.toObject(ptr).toString());
            mergeResult.trimToSize();
        }
    }

    @Override
    public void aggregate(Tuple tuple, ImmutableBytesWritable ptr) {
        String value = PVarchar.INSTANCE.toObject(ptr).toString();
        mergeResult.append(value).append(" ");

    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        ptr.set(PVarchar.INSTANCE.toBytes(mergeResult.toString()));
        return true;
    }


    @Override
    public PDataType getDataType() {
        return PVarchar.INSTANCE;
    }
}