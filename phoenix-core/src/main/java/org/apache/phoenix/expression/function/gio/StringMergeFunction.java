package org.apache.phoenix.expression.function.gio;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.gio.StringMergeAggregator;
import org.apache.phoenix.expression.function.CountAggregateFunction;
import org.apache.phoenix.expression.function.DelegateConstantToCountAggregateFunction;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.parse.gio.StringMergeParseNode;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;

import java.util.List;

/**
 * Created by Spirit on 17/6/15.
// */
@BuiltInFunction(name = StringMergeFunction.NAME, nodeClass = StringMergeParseNode.class, args= {@Argument(allowedTypes={PVarchar.class})})
public class StringMergeFunction extends DelegateConstantToCountAggregateFunction {
    public static final String NAME = "STRING_AGG";

    public StringMergeFunction() {

    }

    public StringMergeFunction(List<Expression> childExpressions){
        super(childExpressions, null);
    }

    @Override
    public String getName() {
        return NAME;
    }

    public StringMergeFunction(List<Expression> childExpressions, CountAggregateFunction delegate){
        super(childExpressions, delegate);
    }

    private Aggregator newAggregator(final PDataType type, SortOrder sortOrder, ImmutableBytesWritable ptr) {
        if (type == PVarchar.INSTANCE) {
            return new StringMergeAggregator(sortOrder, ptr);
        } else {
            return null;
        }
    }

    private Expression getStringExpression() {
        return children.get(0);
    }

    @Override
    public Aggregator newClientAggregator() {
        return newAggregator(getDataType(), SortOrder.getDefault(), null);
    }

    @Override
    public Aggregator newServerAggregator(Configuration conf) {
        Expression child = getAggregatorExpression();
        return newAggregator(child.getDataType(), child.getSortOrder(), null);
    }

    @Override
    public Aggregator newServerAggregator(Configuration conf, ImmutableBytesWritable ptr) {
        Expression child = getAggregatorExpression();
        return newAggregator(child.getDataType(), child.getSortOrder(), ptr);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (!super.evaluate(tuple, ptr)) {
            return false;
        }
        List values = Lists.newArrayListWithExpectedSize(children.size());
        for (int i = 0; i < children.size(); i++) {
            Expression dataExpr = children.get(i);
            if (!dataExpr.evaluate(tuple, ptr)) return false;

            String value = (String) PVarchar.INSTANCE.toObject(ptr, children.get(0).getSortOrder());

            values.add(i, value.trim());
        }
        ptr.set(PVarchar.INSTANCE.toBytes(mkString(values)));
        return true;
    }

    private String mkString(List values) {
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < values.size() - 1; i++) {
            buffer.append(values.get(i).toString()).append(" ");
        }
        buffer.append(values.get(values.size() - 1).toString());
        return buffer.toString();
    }

}