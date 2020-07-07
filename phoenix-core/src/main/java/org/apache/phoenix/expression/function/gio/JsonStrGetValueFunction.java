package org.apache.phoenix.expression.function.gio;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.*;
import org.apache.phoenix.util.ByteUtil;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

@FunctionParseNode.BuiltInFunction(name= org.apache.phoenix.expression.function.gio.JsonStrGetValueFunction.NAME, args={
        @FunctionParseNode.Argument(allowedTypes={ PVarchar.class }),
        @FunctionParseNode.Argument(allowedTypes={ PVarchar.class })} )
public class JsonStrGetValueFunction extends ScalarFunction {
    public static final String NAME = "JSON_GET_VALUE";

    public JsonStrGetValueFunction() { }

    public JsonStrGetValueFunction(List<Expression> children) throws SQLException {
        super(children);
    }

    private List<Expression> getStringExpression() {
        return children;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        List<Expression> childs = getStringExpression();
        String sourceStr = null;
        String key = null;
        for (int i = 0; i < childs.size(); i++) {
            if (!childs.get(i).evaluate(tuple, ptr)) {
                return false;
            }
            if (ptr.getLength() == 0) {
                ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
                return true;
            }
            switch (i) {
                case 0 :
                    sourceStr = (String) PVarchar.INSTANCE.toObject(ptr, getStringExpression().get(0).getSortOrder());
                case 1 :
                    key = (String) PVarchar.INSTANCE.toObject(ptr, getStringExpression().get(0).getSortOrder());
            }
        }
        try {
            Map<String, String> jsonNode = new ObjectMapper().readValue(sourceStr, new TypeReference<Map<String, String>>() {});
            byte[] orDefault = jsonNode.get(key) != null ? jsonNode.get(key).getBytes() : ByteUtil.EMPTY_BYTE_ARRAY;
            ptr.set(orDefault);
        } catch (IOException e) {
            return false;
        }
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PVarchar.INSTANCE;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
