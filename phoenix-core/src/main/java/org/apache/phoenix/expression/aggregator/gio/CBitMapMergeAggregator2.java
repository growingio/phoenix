/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.expression.aggregator.gio;

import io.growing.bitmap.BitMap;
import io.growing.bitmap.BucketBitMap;
import io.growing.bitmap.CBitMap;
import io.growing.bitmap.RoaringBitmap;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
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


/**
 * Aggregator2 that merge bitmap values
 *
 * @since GIO-1.2
 */
public class CBitMapMergeAggregator2 extends BaseAggregator {
    private List<Expression> children;
    private Map<Short, CBitMap> rid2Cbm = new HashMap<>();

    public CBitMapMergeAggregator2(SortOrder sortOrder, ImmutableBytesWritable ptr, List<Expression> children) {
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
                    short newBucketId = BitMapAggregator.mergeRid(rid, bucketId);
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
