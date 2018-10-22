package org.apache.phoenix.expression.function.gio;

import io.growing.bitmap.RoaringBitmap;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.parse.FunctionParseNode.*;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarbinary;

import java.util.List;

/**
 * Created by qifu on 2017/12/6.
 */
@BuiltInFunction(name = RBitMapSampleFunction.NAME,
        args = {@Argument(allowedTypes = {PVarbinary.class}),
                @Argument(allowedTypes = {PInteger.class})})
public class RBitMapSampleFunction extends ScalarFunction {
    public static final String NAME = "RBITMAP_SAMPLE";

    public RBitMapSampleFunction() {
    }

    public RBitMapSampleFunction(List<Expression> children) {
        super(children);
    }

    @Override
    public PDataType getDataType() {
        return PVarbinary.INSTANCE;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression sampleExpr = children.get(1);
        if (!sampleExpr.evaluate(tuple, ptr))
            return false;
        int sampleRatio = (Integer) PInteger.INSTANCE.toObject(ptr, sampleExpr.getSortOrder());

        if (!children.get(0).evaluate(tuple, ptr))
            return false;
        RoaringBitmap original = new RoaringBitmap(ptr.copyBytes());
        RoaringBitmap sampled = original.sample(sampleRatio);
        ptr.set(sampled.getBytes());
        return true;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
