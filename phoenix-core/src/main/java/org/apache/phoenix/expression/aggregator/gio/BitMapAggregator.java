package org.apache.phoenix.expression.aggregator.gio;

public class BitMapAggregator {

    static final int MAXIMUM_CAPACITY = 1 << 30;
    static final int eightBit = 0x00ff;
    static final int nineBit = 0x01ff;

    public static short mergeRid(short rid, short bucketId) {
        int high8Bit = (rid & 0x00FF) << 8;
        int low8Bit = (bucketId & 0x00FF);
        return new Integer(high8Bit | low8Bit).shortValue();
    }

    // 在rule_id不足128的时候，给lowBit9位，在rule_id超过128的时候，都是8位
    public static short mergeRid3(short rid, short bucketId, int ridCount) {
        boolean isSmallMerged = ((int) (Math.log(tableSizeFor(ridCount)) / Math.log(2.0)) < 8);
        int bitCount = isSmallMerged ? 9: 8;
        int basicBit = isSmallMerged ? nineBit: eightBit;
        int high8Bit = (rid & basicBit) << bitCount;
        int low8Bit = (bucketId & basicBit);
        return new Integer(high8Bit | low8Bit).shortValue();
    }

    static int tableSizeFor(int cap) {
        int n = cap;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
    }


}
