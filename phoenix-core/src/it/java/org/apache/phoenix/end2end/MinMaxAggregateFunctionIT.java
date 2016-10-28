/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.phoenix.end2end;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.sql.*;
import java.util.Properties;

import io.growing.roaringbitmap.RoaringBitmap;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

public class MinMaxAggregateFunctionIT extends BaseHBaseManagedTimeIT {

    @Test
    public void testMinMaxAggregateFunctions() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            conn.prepareStatement(
                "create table TT("
                        + "VAL1 integer not null, "
                        + "VAL2 char(2), "
                        + "VAL3 varchar, "
                        + "VAL4 varchar "
                        + "constraint PK primary key (VAL1))").execute();
            conn.commit();

            conn.prepareStatement("upsert into TT values (0, '00', '00', '0')").execute();
            conn.prepareStatement("upsert into TT values (1, '01', '01', '1')").execute();
            conn.prepareStatement("upsert into TT values (2, '02', '02', '2')").execute();
            conn.commit();

            ResultSet rs = conn.prepareStatement("select min(VAL2) from TT").executeQuery();
            assertTrue(rs.next());
            assertEquals("00", rs.getString(1));

            rs = conn.prepareStatement("select min(VAL3) from TT").executeQuery();
            assertTrue(rs.next());
            assertEquals("00", rs.getString(1));

            rs = conn.prepareStatement("select max(VAL2)from TT").executeQuery();
            assertTrue(rs.next());
            assertEquals("02", rs.getString(1));

            rs = conn.prepareStatement("select max(VAL3)from TT").executeQuery();
            assertTrue(rs.next());
            assertEquals("02", rs.getString(1));

            rs =
                    conn.prepareStatement(
                        "select min(VAL1), min(VAL2), min(VAL3), min(VAL4) from TT").executeQuery();
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
            assertEquals("00", rs.getString(2));
            assertEquals("00", rs.getString(3));
            assertEquals("0", rs.getString(4));

            rs =
                    conn.prepareStatement(
                        "select max(VAL1), max(VAL2), max(VAL3), max(VAL4) from TT").executeQuery();

            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals("02", rs.getString(2));
            assertEquals("02", rs.getString(3));
            assertEquals("2", rs.getString(4));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testBitMap() throws Exception {

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        String create = "CREATE TABLE test_bm (PK INTEGER PRIMARY KEY, c1 VARBINARY)";
        String upsert = "upsert into test_bm values (?,?)";
        String query = "select bitmap_merge(c1) from test_bm";
//        String query = "select bitmap_and(b1.c1, b2.c1) from test_bm b1 join test_bm b2 on b1.pk=b2.pk";
        try {
            Statement statement = conn.createStatement();
            statement.execute(create);
            conn.commit();
            for(int i=0; i<3; i++) {
                PreparedStatement stmt = conn.prepareStatement(upsert);
                stmt.setInt(1, i);
                if (i == 2) {
                    stmt.setBytes(2, null);
                } else {
                    RoaringBitmap rb = new RoaringBitmap();
                    rb.add(i);
                    stmt.setBytes(2, rb.getBytes());
                }
                stmt.execute();
                conn.commit();
            }
            Statement stmtf = conn.createStatement();
            ResultSet rs = stmtf.executeQuery(query);
            assertTrue(rs.next());

            RoaringBitmap rst = new RoaringBitmap();
            rst.deserialize(new DataInputStream(new ByteArrayInputStream(rs.getBytes(1))));
            assertEquals(rst.getCardinality(), 2);
        } finally {
            conn.close();
        }

    }
}
