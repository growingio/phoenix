package org.apache.phoenix.expression.aggregator.gio;

import io.growing.bitmap.BitMap;
import io.growing.bitmap.BucketBitMap;
import io.growing.bitmap.CBitMap;
import io.growing.bitmap.RoaringBitmap;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.BaseAggregator;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarbinary;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CBitMapMergeAggregator3 extends BaseAggregator {
    private List<Expression> children;
    private Map<Short, CBitMap> rid2Cbm = new HashMap<>();
    private int ridCount = 0;

    public CBitMapMergeAggregator3(SortOrder sortOrder, ImmutableBytesWritable ptr, List<Expression> children) {
        super(sortOrder);
        this.children = children;
        if (ptr != null) {
            throw new RuntimeException("gio unexpected exception");
        }
    }

    @Override
    public List<Expression> getChildren() {
        return children;
    }

    @Override
    public void aggregate(Tuple tuple, ImmutableBytesWritable ptr) {
        mergeValue(tuple, ptr);
    }


    private void mergeValue(Tuple tuple, ImmutableBytesWritable ptr) {
        try {
            Expression expr2 = getChildren().get(2);
            ridCount = (int) ((LiteralExpression) expr2).getValue();

            Expression expr0 = getChildren().get(0);
            if (!expr0.evaluate(tuple, ptr)) return;
            CBitMap cbm = new CBitMap(ptr.copyBytes());

            Expression expr1 = getChildren().get(1);
            if (!expr1.evaluate(tuple, ptr)) return;
            short rid = ((Integer) PInteger.INSTANCE.toObject(ptr, expr1.getSortOrder())).shortValue();

            if (!rid2Cbm.containsKey(rid)) {
                rid2Cbm.put(rid, cbm);
            } else {
                rid2Cbm.get(rid).or(cbm);
            }
        } catch (Exception e) {
            throw new RuntimeException("gio unexpected exception", e);
        }
    }


    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        try {
            byte[] bmBytes = evaluateValue().getBytes();
            ptr.set(bmBytes, 0, bmBytes.length);
            return true;
        } catch (IOException e) {
            throw new RuntimeException("gio unexpected exception", e);
        }
    }


    private BitMap evaluateValue() {
        CBitMap result = new CBitMap();
        for (Map.Entry<Short, CBitMap> rid2CbmEntry : rid2Cbm.entrySet()) {
            short rid = rid2CbmEntry.getKey();
            CBitMap cbm = rid2CbmEntry.getValue();

            Map<Integer, BucketBitMap> newPos2Bbm = new HashMap<>();
            for (Map.Entry<Integer, BucketBitMap> cBit2BbmEntry : cbm.getContainer().entrySet()) {
                int pos = cBit2BbmEntry.getKey();
                BucketBitMap bbm = cBit2BbmEntry.getValue();

                Map<Short, RoaringBitmap> newBucketId2Rbm = new HashMap<>();
                for (Map.Entry<Short, RoaringBitmap> bucketId2RbmEntry: bbm.getContainer().entrySet()) {
                    short bucketId = bucketId2RbmEntry.getKey();
                    short newBucketId = BitMapAggregator.mergeRid3(rid, bucketId, ridCount);
                    newBucketId2Rbm.put(newBucketId, bucketId2RbmEntry.getValue());
                }
                newPos2Bbm.put(pos, new BucketBitMap(newBucketId2Rbm, false));
            }
            result.or(new CBitMap(newPos2Bbm, false));
        }
        return result;
    }


    @Override
    public final PDataType getDataType() {
        return PVarbinary.INSTANCE;
    }

    @Override
    public void reset() {
        rid2Cbm.clear();
        super.reset();
    }

    @Override
    public String toString() {
        return "BITMAP MERGE [rid2Cbm.size()=" + rid2Cbm.size() + "]";
    }

}
