package org.apache.phoenix.expression.aggregator.gio;

public class BitMapAggregator {
    public static short mergeRid(short rid, short bucketId) {
        int high8Bit = (rid & 0x00FF) << 8;
        int low8Bit = (bucketId & 0x00FF);
        return new Integer(high8Bit | low8Bit).shortValue();
    }

}
