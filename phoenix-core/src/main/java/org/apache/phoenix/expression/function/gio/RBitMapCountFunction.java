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

import io.growing.bitmap.RoaringBitmap;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarbinary;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

@BuiltInFunction(name = RBitMapCountFunction.NAME,
        args = { @Argument(allowedTypes = { PVarbinary.class }) })
public class RBitMapCountFunction extends ScalarFunction {

    public static final String NAME = "RBITMAP_COUNT";

    public RBitMapCountFunction() {
    }

    public RBitMapCountFunction(List<Expression> children) throws SQLException {
        super(children);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        try {
            byte[] lengthBuf = new byte[PLong.INSTANCE.getByteSize()];
            RoaringBitmap bitmap = new RoaringBitmap();
            if (children.get(0).evaluate(tuple, ptr)) {
                bitmap.deserialize(new DataInputStream(new ByteArrayInputStream(ptr.copyBytes())));
                PLong.INSTANCE.getCodec().encodeLong(bitmap.getLongCardinality(), lengthBuf, 0);
            } else {
                PLong.INSTANCE.getCodec().encodeLong(0L, lengthBuf, 0);
            }
            ptr.set(lengthBuf);
            return true;
        } catch (IOException e) {
            throw new RuntimeException("unexpected exception", e);
        }
    }

    @Override
    public PDataType getDataType() {
        return PLong.INSTANCE;
    }
}
