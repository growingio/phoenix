package org.apache.phoenix.expression.function.gio;

import io.growing.bitmap.CBitMap;
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
@BuiltInFunction(name = CBitMapSampleFunction.NAME,
        args = {@Argument(allowedTypes = {PVarbinary.class}),
                @Argument(allowedTypes = {PInteger.class})})
public class CBitMapSampleFunction extends ScalarFunction {
    public static final String NAME = "CBITMAP_SAMPLE";

    public CBitMapSampleFunction() {
    }

    public CBitMapSampleFunction(List<Expression> children) {
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
        try {
            CBitMap original = new CBitMap(ptr.copyBytes());
            CBitMap sampled = original.sample(sampleRatio);
            ptr.set(sampled.getBytes());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public String getName() {
        return NAME;
    }
}
