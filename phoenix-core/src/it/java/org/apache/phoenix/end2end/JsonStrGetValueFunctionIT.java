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

import io.growing.bitmap.BucketBitMap;
import io.growing.bitmap.CBitMap;
import io.growing.bitmap.RoaringBitmap;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.*;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class JsonStrGetValueFunctionIT extends BaseHBaseManagedTimeIT {

    private Connection conn = null;

    @Before
    public void beforeAll() throws SQLException, IOException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        createRbmTable();
    }

    @After
    public void afterAll() throws SQLException {
        conn.close();
    }

    private void createRbmTable() throws SQLException, IOException {
        Statement stmt = conn.createStatement();

        // create table
        String create1 = "CREATE TABLE test_rbm1 (id INTEGER PRIMARY KEY, utm_var VARCHAR)";
        stmt.addBatch(create1);
        stmt.executeBatch();
        stmt.close();

        // insert data
        String upsert1 = "upsert into test_rbm1 values (?,?)";
        PreparedStatement prepareStmt1 = conn.prepareStatement(upsert1);
        prepareStmt1.setInt(1, 1);
        String jsonStr = "{\"test_guanggao_weidu\" : \"ios_adsvars_2020022905\", \"ads_link\" : \"acavasg\"}";
        prepareStmt1.setString(2, jsonStr);
        prepareStmt1.addBatch();
        prepareStmt1.executeBatch();
        prepareStmt1.close();
        conn.commit();
    }

    @Test
    public void testJSONGet() throws SQLException {
        String query = "select json_get_value(utm_var, 'test_guanggao_weidu'), json_get_value(utm_var, 'ads_link') " +
                "from test_rbm1";

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        while(rs.next()) {
            String json1 = rs.getString(1);
            String json2 = rs.getString(2);
            assertEquals("ios_adsvars_2020022905", json1);
            assertEquals("acavasg", json2);
        }
    }

}
