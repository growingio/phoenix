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
package org.apache.phoenix.expression.function.gio;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.gio.RBitMapMergeAggregator;
import org.apache.phoenix.expression.function.CountAggregateFunction;
import org.apache.phoenix.expression.function.DelegateConstantToCountAggregateFunction;
import org.apache.phoenix.parse.gio.CBitMapMergeAggregateParseNode;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarbinary;

import java.util.List;


/**
 * Built-in function for BITMAP merge function.
 *
 * @since 0.1
 */
@BuiltInFunction(name = RBitMapMergeFunction.NAME, nodeClass = CBitMapMergeAggregateParseNode.class,
        args = {@Argument(allowedTypes = {PVarbinary.class})})
public class RBitMapMergeFunction extends DelegateConstantToCountAggregateFunction {
    public static final String NAME = "RBITMAP_MERGE";

    public RBitMapMergeFunction() {
    }

    // TODO: remove when not required at built-in func register time
    public RBitMapMergeFunction(List<Expression> childExpressions) {
        super(childExpressions, null);
    }

    public RBitMapMergeFunction(List<Expression> childExpressions, CountAggregateFunction delegate) {
        super(childExpressions, delegate);
    }

    private Aggregator newAggregator(final PDataType type, SortOrder sortOrder, ImmutableBytesWritable ptr) {
        assert (type == PVarbinary.INSTANCE);
        return new RBitMapMergeAggregator(sortOrder, ptr);
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
