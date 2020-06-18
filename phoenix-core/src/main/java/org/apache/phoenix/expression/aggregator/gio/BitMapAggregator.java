package org.apache.phoenix.expression.aggregator.gio;

public class BitMapAggregator {

    static final int MAXIMUM_CAPACITY = 1 << 30;
    static final int eightBit = 0x00FF;
    static final int maxBit = 0xFFFF;


    public static short mergeRid(short rid, short bucketId) {
        int high8Bit = (rid & 0x00FF) << 8;
        int low8Bit = (bucketId & 0x00FF);
        return new Integer(high8Bit | low8Bit).shortValue();
    }

    /**
     * 动态调整rid 和 bucketId需要的位
     * leftShiftBit 是rid的左移位，根据ridCount, 只给rid的index留够正好的位就可以了
     * 比如，传入5个rid, 那么tableSizeFor(5) = 8,8的二进制就是1000, 是2的三次方
     * rid一定小于8是 0 - 7的值，最大111,用三位就可以保存，所以 16 - 3 = 13，rid的数据左移13位，剩下的位给bucketId使用
     * @param rid rule_id的index
     * @param bucketId
     * @param ridCount 合并指标中rule_id的个数
     * @return 新编号
     */
    public static short mergeRid3(short rid, short bucketId, int ridCount) {
        int leftShiftBit = (int) Math.max(16 - (Math.log(tableSizeFor(ridCount)) / Math.log(2)), 8);
        int bucketBaseBit = Math.max((maxBit / tableSizeFor(ridCount)) - 1, eightBit);
        int ridBaseBit = Math.min(tableSizeFor(ridCount) - 1, eightBit);
        int highBit = (rid & ridBaseBit) << leftShiftBit;
        int lowBit = (bucketId & bucketBaseBit);
        return new Integer(highBit | lowBit).shortValue();
    }

    // 返回大于输入参数且最近的2的整数次幂的数, hashMap里的tableSizeFor方法
    static int tableSizeFor(int cap) {
        int n = cap - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
    }


}
