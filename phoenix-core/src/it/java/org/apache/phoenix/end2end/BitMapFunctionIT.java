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

public class BitMapFunctionIT extends BaseHBaseManagedTimeIT {

    private Connection conn = null;

    @Before
    public void beforeAll() throws SQLException, IOException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

//        createRbmTable();
//        createBucketBmTable();
        createBucketBmTableForMerge();
//        createCbmTable();
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
        String create3 = "CREATE TABLE test_rbm3 (id INTEGER PRIMARY KEY, bm VARBINARY)";
        String create4 = "CREATE TABLE test_rbm4 (id INTEGER PRIMARY KEY, bm VARBINARY)";
        stmt.addBatch(create1);
        stmt.addBatch(create2);
        stmt.addBatch(create3);
        stmt.addBatch(create4);
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

        String upsert3 = "upsert into test_rbm3 values (?,?)";
        PreparedStatement prepareStmt3 = conn.prepareStatement(upsert3);
        prepareStmt3.setInt(1, 1);
        prepareStmt3.setBytes(2, null);
        prepareStmt3.addBatch();
        prepareStmt3.executeBatch();
        prepareStmt3.close();

        String upsert4 = "upsert into test_rbm4 values (?,?)";
        PreparedStatement prepareStmt4 = conn.prepareStatement(upsert4);
        RoaringBitmap sample1 = new RoaringBitmap();
        RoaringBitmap sample2 = new RoaringBitmap();
        for (int i = 1; i < 256; i++) {
            sample1.add(i);
            if (i % 3 == 0)
                sample2.add(i);
        }
        prepareStmt4.setInt(1, 1);
        prepareStmt4.setBytes(2, sample1.getBytes());
        prepareStmt4.execute();
        prepareStmt4.setInt(1, 2);
        prepareStmt4.setBytes(2, sample2.getBytes());
        prepareStmt4.execute();

        conn.commit();
    }

    private void createBucketBmTable() throws SQLException, IOException {
        Statement stmt = conn.createStatement();

        // create table
        String create1 = "CREATE TABLE test_bucket_bm1 (id INTEGER PRIMARY KEY, bm VARBINARY)";
        String create2 = "CREATE TABLE test_bucket_bm2 (id INTEGER PRIMARY KEY, bm VARBINARY)";
        String create3 = "CREATE TABLE test_bucket_bm3 (id INTEGER PRIMARY KEY, bm VARBINARY)";
        stmt.addBatch(create1);
        stmt.addBatch(create2);
        stmt.addBatch(create3);
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

        String upsert3 = "upsert into test_bucket_bm3 values (?,?)";
        PreparedStatement prepareStmt3 = conn.prepareStatement(upsert3);
        prepareStmt3.setInt(1, 1);
        prepareStmt3.setBytes(2, null);
        prepareStmt3.addBatch();
        prepareStmt3.executeBatch();
        prepareStmt3.close();

        conn.commit();
    }

    private void createBucketBmTableForMerge() throws SQLException, IOException {
        Statement stmt = conn.createStatement();

        // create table
        String create1 = "CREATE TABLE test_merge_bm1 (id INTEGER PRIMARY KEY, bm VARBINARY)";
        String create2 = "CREATE TABLE test_merge_bm2 (id INTEGER PRIMARY KEY, bm VARBINARY)";
        stmt.addBatch(create1);
        stmt.addBatch(create2);
        stmt.executeBatch();
        stmt.close();

        // insert data
        String upsert1 = "upsert into test_merge_bm1 values (?,?)";
        PreparedStatement prepareStmt1 = conn.prepareStatement(upsert1);
        prepareStmt1.setInt(1, 1);
        BucketBitMap bucketBm1 = new BucketBitMap();
        bucketBm1.add((short) 0, 1);
        bucketBm1.add((short) 1, 2);
        bucketBm1.add((short) 256, 3);
        prepareStmt1.setBytes(2, bucketBm1.getBytes());
        prepareStmt1.addBatch();
        prepareStmt1.executeBatch();
        prepareStmt1.close();

        String upsert2 = "upsert into test_merge_bm2 values (?,?)";
        PreparedStatement prepareStmt2 = conn.prepareStatement(upsert2);
        prepareStmt2.setInt(1, 1);
        BucketBitMap bucketBm2 = new BucketBitMap();
        bucketBm2.add((short) 0, 1);
        bucketBm2.add((short) 1, 2);
        bucketBm2.add((short) 256, 3);
        bucketBm2.add((short) 511, 4);
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
        String create3 = "CREATE TABLE test_cbm3 (id INTEGER PRIMARY KEY, bm VARBINARY)";
        stmt.addBatch(create1);
        stmt.addBatch(create2);
        stmt.addBatch(create3);
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

        String upsert3 = "upsert into test_cbm3 values (?,?)";
        PreparedStatement prepareStmt3 = conn.prepareStatement(upsert3);
        prepareStmt3.setInt(1, 1);
        prepareStmt3.setBytes(2, null);
        prepareStmt3.addBatch();
        prepareStmt3.executeBatch();
        prepareStmt3.close();

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

    @Test
    public void testRBitMapCount() throws SQLException {
        String query = "select rbitmap_count(bm) from test_rbm3";
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        assertTrue(rs.next());
        assertEquals(0, rs.getLong(1));
    }

    @Test
    public void testRBitMapSample() throws SQLException {
        int sampleRatio = 4;
        String query = "select id, bm, rbitmap_sample(bm, " + sampleRatio + ") from test_rbm4";
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        while (rs.next()) {
            int id = rs.getInt(1);
            RoaringBitmap original = new RoaringBitmap(rs.getBytes(2));
            RoaringBitmap rbm = new RoaringBitmap(rs.getBytes(3));
            assertEquals(original.sample(sampleRatio).getCardinality(), rbm.getCardinality());
        }
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
        assertEquals(1.0D, rs.getDouble(1), 0D);
    }

    @Test
    public void testBucketBitMapAndNot() throws SQLException {
        String query = "select bucket_bitmap_count(bucket_bitmap_andnot(b1.bm, b2.bm)) " +
                "from test_bucket_bm1 b1, test_bucket_bm2 b2";

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        assertTrue(rs.next());
        assertEquals(1.0D, rs.getDouble(1), 0D);
    }

    @Test
    public void testBucketBitMapOr() throws SQLException {
        String query = "select bucket_bitmap_count(bucket_bitmap_or(b1.bm, b2.bm)) " +
                "from test_bucket_bm1 b1, test_bucket_bm2 b2";

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        assertTrue(rs.next());
        assertEquals(3.0D, rs.getDouble(1), 0D);
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
        assertEquals(3.0D, rs.getDouble(1), 0D);
    }

    @Test
    public void testBucketBitMapMerge2() throws Exception {
        String query = "select bucket_bitmap_merge2(bm, rid) " +
                "from " +
                "(select bm,0 rid from test_bucket_bm1 " +
                "union all " +
                "select bm,1 rid from test_bucket_bm2)";
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        while (rs.next()) {
            BucketBitMap bbm = new BucketBitMap(rs.getBytes(1));
            assertEquals(bbm.getCount(), 3, 0);
        }

        // 一条数据时，server aggregator 不进行序列化和反序列
        String query2 = "select bucket_bitmap_merge2(bm, 0) from test_bucket_bm1";
        Statement stmt2 = conn.createStatement();
        ResultSet rs2 = stmt2.executeQuery(query2);
        while (rs2.next()) {
            BucketBitMap bbm = new BucketBitMap(rs2.getBytes(1));
            assertEquals(bbm.getCount(), 2, 0);
        }
    }

    @Test
    public void testBucketBitMapMerge3() throws Exception {
        String query = "select bucket_bitmap_merge3(bm, rid, 2) " +
                "from " +
                "(select bm,0 rid from test_merge_bm1 " +
                "union all " +
                "select bm,1 rid from test_merge_bm2)";
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        while (rs.next()) {
            BucketBitMap bbm = new BucketBitMap(rs.getBytes(1));
            assertEquals(bbm.getContainer().keySet().toArray().length, 7, 0);
            assertEquals(bbm.getCount(), 4, 0);
        }

        // 一条数据时，server aggregator 不进行序列化和反序列
        String query2 = "select bucket_bitmap_merge3(bm, 0, 1) from test_merge_bm1";
        Statement stmt2 = conn.createStatement();
        ResultSet rs2 = stmt2.executeQuery(query2);
        while (rs2.next()) {
            BucketBitMap bbm = new BucketBitMap(rs2.getBytes(1));
            assertEquals(bbm.getCount(), 3, 0);
        }
    }

    @Test
    public void testBucketBitMapMerge3more128() throws Exception {
        String query = "select bucket_bitmap_merge3(bm, rid, 128) " +
                "from " +
                "(select bm,0 rid from test_merge_bm1 " +
                "union all " +
                "select bm,1 rid from test_merge_bm2)";
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        while (rs.next()) {
            BucketBitMap bbm = new BucketBitMap(rs.getBytes(1));
            assertEquals(bbm.getContainer().keySet().toArray().length, 7, 0);
            assertEquals(bbm.getCount(), 4, 0);
        }

        // 一条数据时，server aggregator 不进行序列化和反序列
        String query2 = "select bucket_bitmap_merge3(bm, 0, 128) from test_merge_bm1";
        Statement stmt2 = conn.createStatement();
        ResultSet rs2 = stmt2.executeQuery(query2);
        while (rs2.next()) {
            BucketBitMap bbm = new BucketBitMap(rs2.getBytes(1));
            assertEquals(bbm.getCount(), 3, 0);
        }
    }

    @Test
    public void testCBitMapMerge2() throws Exception {
        String query = "select cbitmap_merge2(bm, rid) " +
                "from " +
                "(select bm,0 rid from test_cbm1 " +
                "union all " +
                "select bm,1 rid from test_cbm2)";
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        while (rs.next()) {
            CBitMap cbm = new CBitMap(rs.getBytes(1));
            assertEquals(cbm.getCount(), 10, 0);
        }

        // 一条数据时，server aggregator 不进行序列化和反序列
        String query2 = "select cbitmap_merge2(bm, 0) from test_cbm1";
        Statement stmt2 = conn.createStatement();
        ResultSet rs2 = stmt2.executeQuery(query2);
        while (rs2.next()) {
            CBitMap cbm = new CBitMap(rs2.getBytes(1));
            assertEquals(cbm.getCount(), 5, 0);
        }
    }

    @Test
    public void testCBitMapMerge3() throws Exception {
        String query = "select cbitmap_merge3(bm, rid, 2) " +
                "from " +
                "(select bm,0 rid from test_cbm1 " +
                "union all " +
                "select bm,1 rid from test_cbm2)";
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        while (rs.next()) {
            CBitMap cbm = new CBitMap(rs.getBytes(1));
            assertEquals(cbm.getCount(), 10, 0);
        }

        String query2 = "select cbitmap_merge3(bm, 0, 1) from test_cbm1";
        Statement stmt2 = conn.createStatement();
        ResultSet rs2 = stmt2.executeQuery(query2);
        while (rs2.next()) {
            CBitMap cbm = new CBitMap(rs2.getBytes(1));
            assertEquals(cbm.getCount(), 5, 0);
        }
    }

    @Test
    public void testBucketBitMapCount() throws SQLException {
        String query = "select bucket_bitmap_count(bm) from test_bucket_bm3";
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        assertTrue(rs.next());
        assertEquals(0D, rs.getDouble(1), 0D);
    }

    @Test
    public void testBucketBitMapSample() throws SQLException, IOException, ClassNotFoundException {
        String query = "select id, bm, bucket_bitmap_sample(bm, 2) from test_bucket_bm1" +
                " union all " +
                "select id, bm, bucket_bitmap_sample(bm, 2) from test_bucket_bm2" +
                " union all " +
                "select id, bm, bucket_bitmap_sample(bm, 2) from test_bucket_bm3";
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        while (rs.next()) {
            BucketBitMap original = new BucketBitMap(rs.getBytes(2));
            BucketBitMap sampled = new BucketBitMap(rs.getBytes(3));
            assertEquals(original.sample(2).getUniqueCardinality(), sampled.getUniqueCardinality());
        }
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
        assertEquals(3D, rs.getDouble(1), 0D);
    }

    @Test
    public void testCBitMapAndNot() throws SQLException {
        String query = "select cbitmap_count(cbitmap_andnot(b1.bm, b2.bm)) " +
                "from test_cbm1 b1, test_bucket_bm1 b2";

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        assertTrue(rs.next());
        assertEquals(2D, rs.getDouble(1), 0D);
    }

    @Test
    public void testCBitMapOr() throws SQLException {
        String query = "select cbitmap_count(cbitmap_or(b1.bm, b2.bm)) " +
                "from test_cbm1 b1, test_cbm2 b2";

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        assertTrue(rs.next());
        assertEquals(10D, rs.getDouble(1), 0D);
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
        assertEquals(10D, rs.getDouble(1), 0D);
    }

    @Test
    public void testCBitMapCount() throws SQLException {
        String query = "select cbitmap_count(bm) from test_cbm3";
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        assertTrue(rs.next());
        assertEquals(0, rs.getDouble(1), 0D);
    }

    @Test
    public void testCBitMapSample() throws SQLException, IOException, ClassNotFoundException {
        String query = "select id, bm, cbitmap_sample(bm, 2) from test_cbm1" +
                " union all " +
                "select id, bm, cbitmap_sample(bm, 2) from test_cbm2" +
                " union all " +
                "select id, bm, cbitmap_sample(bm, 2) from test_cbm3";
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        while (rs.next()) {
            CBitMap original = new CBitMap(rs.getBytes(2));
            CBitMap sampled = new CBitMap(rs.getBytes(3));
            assertEquals((long)original.sample(2).getCount(), (long)sampled.getCount());
        }
    }
}
