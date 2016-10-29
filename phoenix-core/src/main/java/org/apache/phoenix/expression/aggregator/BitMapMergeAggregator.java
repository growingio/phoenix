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
package org.apache.phoenix.expression.aggregator;

import io.growing.roaringbitmap.RoaringBitmap;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.util.SizedUtil;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.math.BigDecimal;


/**
 * 
 * Aggregator that merge bitmap values
 *
 * 
 * @since 0.1
 */
public class BitMapMergeAggregator extends BaseAggregator {
    private RoaringBitmap rb = new RoaringBitmap();

    public BitMapMergeAggregator(SortOrder sortOrder, ImmutableBytesWritable ptr) {
        super(sortOrder);
        if (ptr != null) {
            mergeValue(ptr);
        }
    }

    private void mergeValue(ImmutableBytesWritable ptr) {
        RoaringBitmap value = new RoaringBitmap();
        try {
            value.deserialize(new DataInputStream(new ByteArrayInputStream(ptr.copyBytes())));
        } catch (Exception e) {
        }
        rb.or(value);
    }

    @Override
    public void aggregate(Tuple tuple, ImmutableBytesWritable ptr) {
        mergeValue(ptr);
    }
    
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
//        if (rb.isEmpty()) {
//            return false;
//        }
        byte[] bmBytes = rb.getBytes();
        ptr.set(bmBytes, 0, bmBytes.length);
        return true;
    }
    
    @Override
    public final PDataType getDataType() {
        return PVarbinary.INSTANCE;
    }
    
    @Override
    public void reset() {
        rb = new RoaringBitmap();
        super.reset();
    }

    @Override
    public String toString() {
        return "BITMAP MERGE [cardinality=" + rb.getCardinality() + "]";
    }

    @Override
    public int getSize() {
        return super.getSize() + rb.getSizeInBytes() + SizedUtil.ARRAY_SIZE;
    }
}
