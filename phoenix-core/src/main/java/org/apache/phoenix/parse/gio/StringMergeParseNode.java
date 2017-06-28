package org.apache.phoenix.parse.gio;

import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.FunctionExpression;
import org.apache.phoenix.expression.function.gio.StringMergeFunction;
import org.apache.phoenix.parse.DelegateConstantToCountParseNode;
import org.apache.phoenix.parse.ParseNode;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by Spirit on 17/6/16.
 */
public class StringMergeParseNode extends DelegateConstantToCountParseNode {

    public StringMergeParseNode(String name, List<ParseNode> children, BuiltInFunctionInfo info) {
        super(name, children, info);
    }

    @Override
    public FunctionExpression create(List<Expression> children, StatementContext context) throws SQLException {
        return new StringMergeFunction(children, getDelegateFunction(children,context));
    }
}
