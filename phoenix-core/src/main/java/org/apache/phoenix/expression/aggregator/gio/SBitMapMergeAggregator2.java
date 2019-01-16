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

import io.growing.bitmap.*;
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
public class SBitMapMergeAggregator2 extends BaseAggregator {
    private List<Expression> children;
    private Map<Short, SBitMap> rid2Sbm = new HashMap<>();
    // isFirst 避免只有一条数据时 仍然进行序列化和反序列的开销
    private boolean isFirst = true;
    private byte[] data = null;
    private short ruleId = -1;

    public SBitMapMergeAggregator2(SortOrder sortOrder, ImmutableBytesWritable ptr, List<Expression> children) {
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
            if (isFirst) {
                if (!getChildren().get(0).evaluate(tuple, ptr)) return;
                data = ptr.copyBytes();
                Expression expr1 = getChildren().get(1);
                if (!expr1.evaluate(tuple, ptr)) return;
                ruleId = ((Integer) PInteger.INSTANCE.toObject(ptr, expr1.getSortOrder())).shortValue();
                isFirst = false;
            } else {
                if (data != null) {
                    rid2Sbm.put(ruleId, new SBitMap(data));
                    data = null;
                }

                Expression expr0 = getChildren().get(0);
                if (!expr0.evaluate(tuple, ptr)) return;
                SBitMap sbm = new SBitMap(ptr.copyBytes());

                Expression expr1 = getChildren().get(1);
                if (!expr1.evaluate(tuple, ptr)) return;
                short rid = ((Integer) PInteger.INSTANCE.toObject(ptr, expr1.getSortOrder())).shortValue();

                if (!rid2Sbm.containsKey(rid)) {
                    rid2Sbm.put(rid, sbm);
                } else {
                    rid2Sbm.get(rid).or(sbm);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("gio unexpected exception", e);
        }
    }


    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        try {
            if (data != null) {
                ptr.set(data, 0, data.length);
            } else {
                byte[] bmBytes = evaluateValue().getBytes();
                ptr.set(bmBytes, 0, bmBytes.length);
            }
            return true;
        } catch (IOException e) {
            throw new RuntimeException("gio unexpected exception", e);
        }
    }


    private BitMap evaluateValue() {
        SBitMap result = new SBitMap();
        for (Map.Entry<Short, SBitMap> rid2SbmEntry : rid2Sbm.entrySet()) {
            short rid = rid2SbmEntry.getKey();
            SBitMap sbm = rid2SbmEntry.getValue();

            Map<Integer, BucketBitMap> newEid2Bbm = new HashMap<>();
            for (Map.Entry<Integer, BucketBitMap> eid2BbmEntry : sbm.getContainer().entrySet()) {
                short eid = eid2BbmEntry.getKey().shortValue();
                BucketBitMap bbm = eid2BbmEntry.getValue();
                short newEid = BitMapAggregator.mergeRid(rid, eid);
                newEid2Bbm.put((int) newEid, bbm);
            }
            result.or(new SBitMap(newEid2Bbm, false));
        }
        return result;
    }


    @Override
    public final PDataType getDataType() {
        return PVarbinary.INSTANCE;
    }

    @Override
    public void reset() {
        rid2Sbm.clear();
        isFirst = true;
        data = null;
        ruleId = -1;
        super.reset();
    }

    @Override
    public String toString() {
        return "BITMAP MERGE [rid2Sbm.size()=" + rid2Sbm.size() + "]";
    }

}
