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

import io.growing.bitmap.BucketBitMap;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.aggregator.BaseAggregator;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.util.SizedUtil;

import java.io.IOException;


/**
 * Aggregator that merge bitmap values
 *
 * @since GIO-1.2
 */
public class BucketBitMapMergeAggregator extends BaseAggregator {
    private BucketBitMap bucketBm = new BucketBitMap();
    private byte[] data = null;
    private boolean isFirst = true;

    public BucketBitMapMergeAggregator(SortOrder sortOrder, ImmutableBytesWritable ptr) {
        super(sortOrder);
        if (ptr != null) {
            mergeValue(ptr);
        }
    }

    private void mergeValue(ImmutableBytesWritable ptr) {
        try {
            if (isFirst) {
                data = ptr.copyBytes();
                isFirst = false;
            } else {
                if (data != null) {
                    bucketBm = new BucketBitMap(data);
                    data = null;
                }
                BucketBitMap value = new BucketBitMap(ptr.copyBytes());
                bucketBm.or(value);
            }
        } catch (Exception e) {
            throw new RuntimeException("Unexpected exception", e);
        }
    }

    @Override
    public void aggregate(Tuple tuple, ImmutableBytesWritable ptr) {
        mergeValue(ptr);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        try {
            if (data != null) {
                ptr.set(data, 0, data.length);
            } else {
                byte[] bmBytes = bucketBm.getBytes();
                ptr.set(bmBytes, 0, bmBytes.length);
            }
        } catch (IOException e) {
            throw new RuntimeException("Unexpected exception", e);
        }
        return true;
    }

    @Override
    public final PDataType getDataType() {
        return PVarbinary.INSTANCE;
    }

    @Override
    public void reset() {
        bucketBm = new BucketBitMap();
        data = null;
        isFirst = true;
        super.reset();
    }

    @Override
    public String toString() {
        return "BITMAP MERGE [cardinality=" + bucketBm.getCount() + "]";
    }

    @Override
    public int getSize() {
        try {
            int dataSize = 0;
            if (data != null) {
                dataSize = data.length;
            } else {
                dataSize = bucketBm.getSizeInBytes();
            }
            return super.getSize() + dataSize + SizedUtil.ARRAY_SIZE;
        } catch (IOException e) {
            throw new RuntimeException("Unexpected exception", e);
        }
    }
}
