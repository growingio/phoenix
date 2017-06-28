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

import org.apache.phoenix.util.PropertiesUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.*;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StringAggMergeFunctionIT extends BaseHBaseManagedTimeIT {

    private Connection conn = null;

    @Before
    public void beforeAll() throws SQLException, IOException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        createTable();
    }

    @After
    public void afterAll() throws SQLException {
        conn.close();
    }

    private void createTable() throws SQLException, IOException {
        Statement stmt = conn.createStatement();

        // create table
        String create1 = "CREATE TABLE test_s1 (id INTEGER PRIMARY KEY, rid VARCHAR, vv VARCHAR)";
        stmt.addBatch(create1);
        stmt.executeBatch();
        stmt.close();

        // insert data
        String upsert1 = "upsert into test_s1 values (?,?,?)";
        PreparedStatement prepareStmt1 = conn.prepareStatement(upsert1);
        prepareStmt1.setInt(1, 1);
        prepareStmt1.setString(2, "a");
        prepareStmt1.setString(3, "aaa");
        prepareStmt1.addBatch();
        prepareStmt1.executeBatch();
        prepareStmt1.close();


        String upsert2 = "upsert into test_s1 values (?,?,?)";
        PreparedStatement prepareStmt2 = conn.prepareStatement(upsert2);
        prepareStmt2.setInt(1, 2);
        prepareStmt2.setString(2, "a");
        prepareStmt2.setString(3, "bbb");
        prepareStmt2.addBatch();
        prepareStmt2.executeBatch();
        prepareStmt2.close();


        String upsert3 = "upsert into test_s1 values (?,?,?)";
        PreparedStatement prepareStmt3 = conn.prepareStatement(upsert3);
        prepareStmt3.setInt(1, 3);
        prepareStmt3.setString(2, "a");
        prepareStmt3.setString(3, "abc");
        prepareStmt3.addBatch();
        prepareStmt3.executeBatch();
        prepareStmt3.close();

        conn.commit();
    }


    ///////////////////////////////////////////
    // STRING_AGG tests                      //
    ///////////////////////////////////////////

    @Test
    public void testStringAgg() throws SQLException {
        String query = "select rid, string_agg(vv) " +
                "from test_s1 group by rid";

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);

        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertEquals("aaa bbb abc", rs.getString(2));
    }

}
