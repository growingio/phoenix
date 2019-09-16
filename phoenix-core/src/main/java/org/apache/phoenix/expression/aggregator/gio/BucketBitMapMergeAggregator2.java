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
public class BucketBitMapMergeAggregator2 extends BaseAggregator {
    private List<Expression> children;
    private Map<Short, BucketBitMap> rid2Bbm = new HashMap<>();

    public BucketBitMapMergeAggregator2(SortOrder sortOrder, ImmutableBytesWritable ptr, List<Expression> children) {
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
            BucketBitMap bbm = new BucketBitMap(ptr.copyBytes());

            Expression expr1 = getChildren().get(1);
            if (!expr1.evaluate(tuple, ptr)) return;
            short rid = ((Integer) PInteger.INSTANCE.toObject(ptr, expr1.getSortOrder())).shortValue();

            if (!rid2Bbm.containsKey(rid)) {
                rid2Bbm.put(rid, bbm);
            } else {
                rid2Bbm.get(rid).or(bbm);
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
        } catch (IOException e) {
            throw new RuntimeException("gio unexpected exception", e);
        }
        return true;
    }

    private BitMap evaluateValue() {
        Map<Short, RoaringBitmap> newContainer = new HashMap<>();
        for (Map.Entry<Short, BucketBitMap> rid2BbmEntry : rid2Bbm.entrySet()) {
            short rid = rid2BbmEntry.getKey();
            BucketBitMap bbm = rid2BbmEntry.getValue();
            for (Map.Entry<Short, RoaringBitmap> container : bbm.getContainer().entrySet()) {
                short bucketId = container.getKey();
                short newBucketId = BitMapAggregator.mergeRid(rid, bucketId);
                newContainer.put(newBucketId, container.getValue());
            }
        }
        return new BucketBitMap(newContainer, false);
    }


    @Override
    public final PDataType getDataType() {
        return PVarbinary.INSTANCE;
    }

    @Override
    public void reset() {
        rid2Bbm.clear();
        super.reset();
    }

    @Override
    public String toString() {
        return "BITMAP MERGE [rid2Bbm.size()=" + rid2Bbm.size() + "]";
    }
}
