package org.apache.phoenix.expression.function.gio;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.gio.SBitMapMergeAggregator;
import org.apache.phoenix.expression.aggregator.gio.SBitMapMergeAggregator2;
import org.apache.phoenix.expression.aggregator.gio.SBitMapMergeAggregator3;
import org.apache.phoenix.expression.function.CountAggregateFunction;
import org.apache.phoenix.expression.function.DelegateConstantToCountAggregateFunction;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.gio.SBitMapMergeAggregateParseNode2;
import org.apache.phoenix.parse.gio.SBitMapMergeAggregateParseNode3;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarbinary;

import java.util.List;

/**
 * Built-in function for BITMAP merge function2.
 *
 * @since 0.1
 */
@FunctionParseNode.BuiltInFunction(name = SBitMapMergeFunction3.NAME,
        nodeClass = SBitMapMergeAggregateParseNode3.class,
        args = {@FunctionParseNode.Argument(allowedTypes = {PVarbinary.class}),
                @FunctionParseNode.Argument(allowedTypes = {PInteger.class})})
public class SBitMapMergeFunction3 extends DelegateConstantToCountAggregateFunction {
    public static final String NAME = "SBITMAP_MERGE3";

    public SBitMapMergeFunction3() {
    }

    // TODO: remove when not required at built-in func register time
    public SBitMapMergeFunction3(List<Expression> childExpressions) {
        super(childExpressions, null);
    }

    public SBitMapMergeFunction3(List<Expression> childExpressions,
                                 CountAggregateFunction delegate) {
        super(childExpressions, delegate);
    }


    @Override
    public Aggregator newClientAggregator() {
        return new SBitMapMergeAggregator(SortOrder.getDefault(), null);
    }

    @Override
    public Aggregator newServerAggregator(Configuration conf) {
        return newServerAggregator(conf, null);
    }

    @Override
    public Aggregator newServerAggregator(Configuration conf, ImmutableBytesWritable ptr) {
        Expression bmExpr = children.get(0);
        return new SBitMapMergeAggregator3(bmExpr.getSortOrder(), ptr, children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (!super.evaluate(tuple, ptr)) {
            return false;
        }
        if (isConstantExpression()) {
            PDataType type = getDataType();
            Object constantValue = ((LiteralExpression) children.get(0)).getValue();
            ptr.set(type.toBytes(constantValue));
        }
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PVarbinary.INSTANCE;
    }

    @Override
    public String getName() {
        return NAME;
    }
}