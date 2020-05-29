package org.apache.phoenix.parse.gio;

import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.FunctionExpression;
import org.apache.phoenix.expression.function.gio.BucketBitMapMergeFunction3;
import org.apache.phoenix.parse.DelegateConstantToCountParseNode;
import org.apache.phoenix.parse.ParseNode;

import java.sql.SQLException;
import java.util.List;

public class BucketBitMapMergeAggregateParseNode3 extends DelegateConstantToCountParseNode {

    public BucketBitMapMergeAggregateParseNode3(String name, List<ParseNode> children,
                                                BuiltInFunctionInfo info) {
        super(name, children, info);
    }

    @Override
    public FunctionExpression create(List<Expression> children, StatementContext context)
            throws SQLException {
        return new BucketBitMapMergeFunction3(children, getDelegateFunction(children, context));
    }
}
