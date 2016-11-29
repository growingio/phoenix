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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BitMapMergeFunctionIT extends BaseHBaseManagedTimeIT {

    private Connection conn = null;

    @Before
    public void beforeAll() throws SQLException, IOException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        createRbmTable();
        createBucketBmTable();
        createCbmTable();
    }

    @After
    public void afterAll() throws SQLException {
        conn.close();
    }

    private void createRbmTable() throws SQLException, IOException {
        Statement stmt = conn.createStatement();

        // create table
        String create1 = "CREATE TABLE test_rbm1 (id INTEGER PRIMARY KEY, bm VARBINARY)";
        String create2 = "CREATE TABLE test_rbm2 (id INTEGER PRIMARY KEY, bm VARBINARY)";
        stmt.addBatch(create1);
        stmt.addBatch(create2);
        stmt.executeBatch();
        stmt.close();

        // insert data
        String upsert1 = "upsert into test_rbm1 values (?,?)";
        PreparedStatement prepareStmt1 = conn.prepareStatement(upsert1);
        prepareStmt1.setInt(1, 1);
        RoaringBitmap cbm1 = new RoaringBitmap();
        cbm1.add(1);
        cbm1.add(2);
        prepareStmt1.setBytes(2, cbm1.getBytes());
        prepareStmt1.addBatch();
        prepareStmt1.executeBatch();
        prepareStmt1.close();

        String upsert2 = "upsert into test_rbm2 values (?,?)";
        PreparedStatement prepareStmt2 = conn.prepareStatement(upsert2);
        prepareStmt2.setInt(1, 1);
        RoaringBitmap cbm2 = new RoaringBitmap();
        cbm2.add(1);
        cbm2.add(4);
        prepareStmt2.setBytes(2, cbm2.getBytes());
        prepareStmt2.addBatch();
        prepareStmt2.executeBatch();
        prepareStmt2.close();

        conn.commit();
    }

    private void createBucketBmTable() throws SQLException, IOException {
        Statement stmt = conn.createStatement();

        // create table
        String create1 = "CREATE TABLE test_bucket_bm1 (id INTEGER PRIMARY KEY, bm VARBINARY)";
        String create2 = "CREATE TABLE test_bucket_bm2 (id INTEGER PRIMARY KEY, bm VARBINARY)";
        stmt.addBatch(create1);
        stmt.addBatch(create2);
        stmt.executeBatch();
        stmt.close();

        // insert data
        String upsert1 = "upsert into test_bucket_bm1 values (?,?)";
        PreparedStatement prepareStmt1 = conn.prepareStatement(upsert1);
        prepareStmt1.setInt(1, 1);
        BucketBitMap bucketBm1 = new BucketBitMap();
        bucketBm1.add((short) 1, 2);
        bucketBm1.add((short) 3, 4);
        prepareStmt1.setBytes(2, bucketBm1.getBytes());
        prepareStmt1.addBatch();
        prepareStmt1.executeBatch();
        prepareStmt1.close();

        String upsert2 = "upsert into test_bucket_bm2 values (?,?)";
        PreparedStatement prepareStmt2 = conn.prepareStatement(upsert2);
        prepareStmt2.setInt(1, 1);
        BucketBitMap bucketBm2 = new BucketBitMap();
        bucketBm2.add((short) 1, 2);
        bucketBm2.add((short) 4, 8);
        prepareStmt2.setBytes(2, bucketBm2.getBytes());
        prepareStmt2.addBatch();
        prepareStmt2.executeBatch();
        prepareStmt2.close();

        conn.commit();
    }

    private void createCbmTable() throws SQLException, IOException {
        Statement stmt = conn.createStatement();

        // create table
        String create1 = "CREATE TABLE test_cbm1 (id INTEGER PRIMARY KEY, bm VARBINARY)";
        String create2 = "CREATE TABLE test_cbm2 (id INTEGER PRIMARY KEY, bm VARBINARY)";
        stmt.addBatch(create1);
        stmt.addBatch(create2);
        stmt.executeBatch();
        stmt.close();

        // insert data
        String upsert1 = "upsert into test_cbm1 values (?,?)";
        PreparedStatement prepareStmt1 = conn.prepareStatement(upsert1);
        prepareStmt1.setInt(1, 1);
        CBitMap cbm1 = new CBitMap();
        cbm1.add((short) 1, 2, 3);
        cbm1.add((short) 0, 1, 2);
        prepareStmt1.setBytes(2, cbm1.getBytes());
        prepareStmt1.addBatch();
        prepareStmt1.executeBatch();
        prepareStmt1.close();

        String upsert2 = "upsert into test_cbm2 values (?,?)";
        PreparedStatement prepareStmt2 = conn.prepareStatement(upsert2);
        prepareStmt2.setInt(1, 1);
        CBitMap cbm2 = new CBitMap();
        cbm2.add((short) 2, 7, 3);
        cbm2.add((short) 4, 8, 2);
        prepareStmt2.setBytes(2, cbm2.getBytes());
        prepareStmt2.addBatch();
        prepareStmt2.executeBatch();
        prepareStmt2.close();

        conn.commit();
    }

    ///////////////////////////////////////////
    // RBitMap tests                         //
    ///////////////////////////////////////////

    @Test
    public void testRBitMapAnd() throws SQLException {
        String query = "select rbitmap_count(rbitmap_and(b1.bm, b2.bm)) " +
                "from test_rbm1 b1, test_rbm2 b2";

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        assertTrue(rs.next());
        assertEquals(1, rs.getLong(1));
    }

    @Test
    public void testRBitMapAndNot() throws SQLException {
        String query = "select rbitmap_count(rbitmap_andnot(b1.bm, b2.bm)) " +
                "from test_rbm1 b1, test_rbm2 b2";

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        assertTrue(rs.next());
        assertEquals(1, rs.getLong(1));
    }

    @Test
    public void testRBitMapOr() throws SQLException {
        String query = "select rbitmap_count(rbitmap_or(b1.bm, b2.bm)) " +
                "from test_rbm1 b1, test_rbm2 b2";

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        assertTrue(rs.next());
        assertEquals(3, rs.getLong(1));
    }

    @Test
    public void testRBitMapMerge() throws SQLException {
        String query = "select rbitmap_count(rbitmap_merge(bm)) " +
                "from " +
                "(select bm from test_rbm1 " +
                "union all " +
                "select bm from test_rbm2)";

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        assertTrue(rs.next());
        assertEquals(3, rs.getLong(1));
    }

    ///////////////////////////////////////////
    // BucketBitMap tests                    //
    ///////////////////////////////////////////

    @Test
    public void testBucketBitMapAnd() throws SQLException {
        String query = "select bucket_bitmap_count(bucket_bitmap_and(b1.bm, b2.bm)) " +
                "from test_bucket_bm1 b1, test_bucket_bm2 b2";

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        assertTrue(rs.next());
        assertEquals(1, rs.getLong(1));
    }

    @Test
    public void testBucketBitMapAndNot() throws SQLException {
        String query = "select bucket_bitmap_count(bucket_bitmap_andnot(b1.bm, b2.bm)) " +
                "from test_bucket_bm1 b1, test_bucket_bm2 b2";

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        assertTrue(rs.next());
        assertEquals(1, rs.getLong(1));
    }

    @Test
    public void testBucketBitMapOr() throws SQLException {
        String query = "select bucket_bitmap_count(bucket_bitmap_or(b1.bm, b2.bm)) " +
                "from test_bucket_bm1 b1, test_bucket_bm2 b2";

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        assertTrue(rs.next());
        assertEquals(3, rs.getLong(1));
    }

    @Test
    public void testBucketBitMapMerge() throws SQLException {
        String query = "select bucket_bitmap_count(bucket_bitmap_merge(bm)) " +
                "from " +
                "(select bm from test_bucket_bm1 " +
                "union all " +
                "select bm from test_bucket_bm2)";

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        assertTrue(rs.next());
        assertEquals(3, rs.getLong(1));
    }

    ///////////////////////////////////////////
    // CBitMap tests                         //
    ///////////////////////////////////////////

    @Test
    public void testCBitMapAnd() throws SQLException {
        String query = "select cbitmap_count(cbitmap_and(b1.bm, b2.bm)) " +
                "from test_cbm1 b1, test_bucket_bm1 b2";

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        assertTrue(rs.next());
        assertEquals(3, rs.getLong(1));
    }

    @Test
    public void testCBitMapAndNot() throws SQLException {
        String query = "select cbitmap_count(cbitmap_andnot(b1.bm, b2.bm)) " +
                "from test_cbm1 b1, test_bucket_bm1 b2";

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        assertTrue(rs.next());
        assertEquals(2, rs.getLong(1));
    }

    @Test
    public void testCBitMapOr() throws SQLException {
        String query = "select cbitmap_count(cbitmap_or(b1.bm, b2.bm)) " +
                "from test_cbm1 b1, test_cbm2 b2";

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        assertTrue(rs.next());
        assertEquals(10, rs.getLong(1));
    }

    @Test
    public void testCBitMapMerge() throws SQLException {
        String query = "select cbitmap_count(cbitmap_merge(bm)) " +
                "from " +
                "(select bm from test_cbm1 " +
                "union all " +
                "select bm from test_cbm2)";

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        assertTrue(rs.next());
        assertEquals(10, rs.getLong(1));
    }
}
