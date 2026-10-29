// Copyright (c) YugabyteDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//

package org.yb.pgsql;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.yb.YBTestRunner;
import org.yb.util.RequiresReleaseBuild;

import java.sql.ResultSet;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.yb.AssertionWrappers.*;

@RunWith(value=YBTestRunner.class)
public class TestPgRegressHashInQueries extends BasePgRegressTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestPgRegressHashInQueries.class);

  @Override
  protected Integer getYsqlRequestLimit() {
    // This is so number of roundtrips equals the number of request operators created.
    return 1;
  }

  @Test
  public void testInQueryBatchingOnHashKey() throws Exception {

    String createTable = "CREATE TABLE t1 (a int PRIMARY KEY, b int) SPLIT INTO 3 TABLETS";
    String insertTable = "INSERT INTO t1  SELECT i, i FROM (SELECT generate_series(1, 1024) i) t";

    try (Statement statement = connection.createStatement()) {
      statement.execute(createTable);
      statement.execute(insertTable);
    }

    // Generate select query required to run batched IN.
    // SELECT * FROM t1 WHERE a IN (1, 2, 3, .... 511, 512);
    int num_rows = 512;
    String query = "SELECT * FROM t1 WHERE a IN (";
    for(int i = 1; i < num_rows; ++i) {
      query += i + ", ";
    }
    query += num_rows + ")";
    Set<Row> expectedRows = new HashSet<>();
    for (int i = 1; i <= num_rows; i++) {
      expectedRows.add(new Row(i, i));
    }

    try (Statement statement = connection.createStatement()) {
      statement.execute("SET yb_push_hash_column_lists_to_docdb = false");
      statement.execute("SET yb_enable_hash_batch_in = false");
      long noBatchingNumRequests = getNumStorageRoundtrips(statement, query);
      assertEquals(512, noBatchingNumRequests);
      assertRowSet(statement, query, expectedRows);

      statement.execute("SET yb_enable_hash_batch_in = true");
      long batchingNumRequests = getNumStorageRoundtrips(statement, query);
      assertRowSet(statement, query, expectedRows);
      assertEquals(1, batchingNumRequests);
    }
  }

  @Test
  public void testInQueryBatchingOnMixedKey() throws Exception {

    String createTable =
        "CREATE TABLE t1 (a int, b int, PRIMARY KEY(a hash, b asc)) SPLIT INTO 3 TABLETS";
    String insertTable1 =
        "INSERT INTO t1  SELECT i, i FROM (SELECT generate_series(1, 1024) i) t";
    String insertTable2 =
        "INSERT INTO t1  SELECT i, i+1 FROM (SELECT generate_series(1, 1024) i) t";

    try (Statement statement = connection.createStatement()) {
      statement.execute(createTable);
      statement.execute(insertTable1);
      statement.execute(insertTable2);
    }

    // Generate select query required to run batched IN.
    // SELECT * FROM t1 WHERE a IN (1, 2, 3, .... 511, 512);
    int upper_limit = 512;
    String query = "SELECT * FROM t1 WHERE a IN (";
    for(int i = 1; i < upper_limit; ++i) {
      query += i + ", ";
    }
    query += upper_limit + ") AND b IN (";

    for(int i = 1; i < upper_limit; ++i) {
      if ((i % 2) == 0) {
        query += i + ", ";
      }
    }
    query += upper_limit + ")";

    Set<Row> expectedRows = new HashSet<>();
    for (int i = 1; i <= upper_limit; i++) {
      if ((i % 2) == 1) {
        expectedRows.add(new Row(i, i+1));
      } else {
        expectedRows.add(new Row(i, i));
      }
    }

    try (Statement statement = connection.createStatement()) {
      statement.execute("SET yb_push_hash_column_lists_to_docdb = false");
      statement.execute("SET yb_enable_hash_batch_in = false");
      long noBatchingNumRequests = getNumStorageRoundtrips(statement, query);
      assertEquals(512, noBatchingNumRequests);
      assertRowSet(statement, query, expectedRows);

      statement.execute("SET yb_enable_hash_batch_in = true");
      long batchingNumRequests = getNumStorageRoundtrips(statement, query);
      assertRowSet(statement, query, expectedRows);
      // We send three requests as the number of tablets created are three.
      assertEquals(1, batchingNumRequests);
    }
  }

  @Test
  public void testInQueryBatchingNestLoopHashKey() throws Exception {
    String createTable1 = "CREATE TABLE x (a int PRIMARY KEY, b int) SPLIT INTO 3 TABLETS";
    String insertTable1 = "INSERT INTO x  SELECT i*2, i FROM (SELECT generate_series(1, 4096) i) t";
    String createTable2 = "CREATE TABLE y (a int PRIMARY KEY, b int) SPLIT INTO 3 TABLETS";
    String insertTable2 = "INSERT INTO y  SELECT i*5, i FROM (SELECT generate_series(1, 4096) i) t";

    try (Statement statement = connection.createStatement()) {
      statement.execute(createTable1);
      statement.execute(insertTable1);
      statement.execute(createTable2);
      statement.execute(insertTable2);
    }

    // Generate NL Join query and enable NL Join batching in it with different batch sizes.
    // These get automatically converted to batched IN queries. We should expect the best
    // performance when we enable IN batching.
    String query = "SELECT * FROM x t1 JOIN y t2 ON t1.a = t2.a";

    Set<Row> expectedRows = new HashSet<>();
    for (int i = 1; i <= 819; i++) {
      expectedRows.add(new Row(i*10, i*5, i*10, i*2));
    }

    try (Statement statement = connection.createStatement()) {
      // Enabling NL Join batching
      statement.execute("SET enable_hashjoin = off");
      statement.execute("SET enable_mergejoin = off");
      statement.execute("SET enable_seqscan = off");
      statement.execute("SET enable_material = off");

      statement.execute("SET yb_bnl_batch_size = 3;");
      statement.execute("SET yb_push_hash_column_lists_to_docdb = false");
      statement.execute("SET yb_enable_hash_batch_in = false");
      long noBatchingSmallBatchSizeNumRPCs = getNumStorageRoundtrips(statement, query);
      assertEquals(4098, noBatchingSmallBatchSizeNumRPCs);
      assertRowSet(statement, query, expectedRows);

      statement.execute("SET yb_bnl_batch_size = 1024;");
      statement.execute("SET yb_enable_hash_batch_in = false");
      long noBatchingLargeBatchSizeNumRPCs = getNumStorageRoundtrips(statement, query);
      assertEquals(4098, noBatchingLargeBatchSizeNumRPCs);
      assertRowSet(statement, query, expectedRows);

      statement.execute("SET yb_bnl_batch_size = 1024;");
      statement.execute("SET yb_enable_hash_batch_in = true");
      long batchingLargeBatchSizeNumRPCs = getNumStorageRoundtrips(statement, query);
      assertEquals(6, batchingLargeBatchSizeNumRPCs);
      assertRowSet(statement, query, expectedRows);
    }
  }

  // 140 tablets overloads ASAN/fastdebug Spark workers (master-leader timeout / SIGABRT).
  @Test
  @RequiresReleaseBuild
  public void testInQueryBatchingManyTablets() throws Exception {
    // #31565: composite HASH PK with independent BNL INs on a high-tablet table must stay one
    // batched read at default work_mem, not one storage request per hash permutation.
    final int tablets = 140;
    final int numRows = 1024 * 1024;
    final int inValues = 256;
    final String table = "t_many_tablets";

    Set<Row> expectedRows = new HashSet<>();
    for (int i = 1; i <= inValues; ++i) {
      expectedRows.add(new Row(i, i, i, i));
    }

    try (Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TABLE " + table +
          " (h1 int, h2 int, v1 int, v2 int, PRIMARY KEY ((h1, h2) HASH)) " +
          "SPLIT INTO " + tablets + " TABLETS");
      statement.execute(
          "INSERT INTO " + table + " SELECT i, i, i, i FROM generate_series(1, " + numRows +
          ") AS i");
      statement.execute("SET work_mem = '4MB'");
      statement.execute("SET yb_enable_hash_batch_in = true");
      statement.execute("SET yb_bnl_batch_size = 1024");
      statement.execute("SET enable_hashjoin = off");
      statement.execute("SET enable_mergejoin = off");
      statement.execute("SET enable_seqscan = off");
      statement.execute("SET enable_material = off");

      String query = "SELECT * FROM " + table +
          " WHERE h1 IN (SELECT generate_series(1, " + inValues + "))" +
          " AND h2 IN (SELECT generate_series(1, " + inValues + "))";
      String explain = getExplainAnalyzeOutput(statement, query);
      LOG.info("many tablets, independent BNL IN plan:\n{}", explain);
      assertTrue("expected YB Batched Nested Loop, plan was:\n" + explain,
          explain.contains("YB Batched Nested Loop"));
      long storageTableReadRequests = getNumStorageRoundtrips(statement, query);
      logReadOpsAndRowsScanned("many tablets, independent BNL IN", statement, query);
      assertEquals(1, storageTableReadRequests);
      assertRowSet(statement, query, expectedRows);
    }
  }

  @Test
  @RequiresReleaseBuild
  public void testHashColumnListsIndependentIn() throws Exception {
    // Same schema and row count as testInQueryBatchingManyTablets. Literal IN lists, so this is
    // not a batched nested loop. The permutation path flushes each tablet into several ops, so
    // compact column_lists scans the same rows and sends fewer read ops and storage read requests.
    final int tablets = 32;
    final int numRows = 1024 * 1024;
    final int inValues = 256;
    final String table = "t_hash_column_lists";

    StringBuilder inList = new StringBuilder();
    Set<Row> expectedRows = new HashSet<>();
    for (int i = 1; i <= inValues; ++i) {
      if (i > 1) {
        inList.append(", ");
      }
      inList.append(i);
      expectedRows.add(new Row(i, i, i, i));
    }
    String query = "SELECT * FROM " + table +
        " WHERE h1 IN (" + inList + ") AND h2 IN (" + inList + ")";

    try (Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TABLE " + table +
          " (h1 int, h2 int, v1 int, v2 int, PRIMARY KEY ((h1, h2) HASH)) " +
          "SPLIT INTO " + tablets + " TABLETS");
      statement.execute(
          "INSERT INTO " + table + " SELECT i, i, i, i FROM generate_series(1, " + numRows +
          ") AS i");
      statement.execute("SET yb_enable_hash_batch_in = true");

      statement.execute("SET yb_push_hash_column_lists_to_docdb = off");
      LOG.info("hash column lists off:\n{}", getExplainAnalyzeOutput(statement, query));
      statement.execute(query);
      long requestsOff = getNumStorageRoundtrips(statement, query);
      long readOpsOff = getNumStorageReadOps(statement, query);
      long rowsScannedOff = getNumStorageRowsScanned(statement, query);
      logReadOpsAndRowsScanned("hash column lists off", statement, query);
      assertRowSet(statement, query, expectedRows);

      statement.execute("SET yb_push_hash_column_lists_to_docdb = on");
      statement.execute(query);
      long requestsOn = getNumStorageRoundtrips(statement, query);
      long readOpsOn = getNumStorageReadOps(statement, query);
      long rowsScannedOn = getNumStorageRowsScanned(statement, query);
      logReadOpsAndRowsScanned("hash column lists on", statement, query);
      assertRowSet(statement, query, expectedRows);

      assertEquals(rowsScannedOff, rowsScannedOn);
      assertTrue(
          "flag on storage read ops " + readOpsOn + " not less than flag off " + readOpsOff,
          readOpsOn < readOpsOff);
      assertTrue(
          "flag on storage read requests " + requestsOn +
          " not less than flag off " + requestsOff,
          requestsOn < requestsOff);
    }
  }

  // An IN list on one tablet, with work_mem below the bound keys. The off path binds each key as a
  // permutation tuple and flushes once they pass work_mem. The on path never binds them, so one
  // tablet stays one read op and one storage read request.
  @Test
  public void testHashColumnListsWorkMemFlush() throws Exception {
    final int tablets = 100;
    final int keyLen = 8192;
    final int inValues = 100;
    final String table = "t_hash_column_lists_flush";

    try (Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TABLE " + table +
          " (h text, v int, PRIMARY KEY (h HASH)) SPLIT INTO " + tablets + " TABLETS");
      int[] range = firstTabletHashRange(statement, table);
      List<String> inKeys = new ArrayList<>();
      List<String> otherKeys = new ArrayList<>();
      collectKeys(statement, keyLen, range[0], range[1], inValues, inKeys, otherKeys);
      assertFalse(otherKeys.isEmpty());

      int v = 1;
      for (String key : inKeys) {
        insertKey(statement, table, key, v);
        v++;
      }
      for (String key : otherKeys) {
        insertKey(statement, table, key, -1);
      }

      String query = "SELECT v FROM " + table + " WHERE h IN (" + quotedList(inKeys) + ")";
      Set<Row> expectedRows = new HashSet<>();
      for (int i = 1; i <= inValues; ++i) {
        expectedRows.add(new Row(i));
      }
      statement.execute("SET work_mem = '64kB'");
      statement.execute("SET yb_enable_hash_batch_in = true");

      statement.execute("SET yb_push_hash_column_lists_to_docdb = off");
      long requestsOff = getNumStorageRoundtrips(statement, query);
      long readOpsOff = getNumStorageReadOps(statement, query);
      logReadOpsAndRowsScanned("hash column lists flush off", statement, query);
      assertRowSet(statement, query, expectedRows);

      statement.execute("SET yb_push_hash_column_lists_to_docdb = on");
      long requestsOn = getNumStorageRoundtrips(statement, query);
      long readOpsOn = getNumStorageReadOps(statement, query);
      logReadOpsAndRowsScanned("hash column lists flush on", statement, query);
      assertRowSet(statement, query, expectedRows);

      assertTrue("flag off read ops " + readOpsOff + " not above 1", readOpsOff > 1);
      assertTrue("flag off storage read requests " + requestsOff + " not above 1", requestsOff > 1);
      assertEquals(1L, readOpsOn);
      assertEquals(1L, requestsOn);
    }
  }

  // Batched nested loop of two hash primary keys. The inner side is one hash IN batch.
  // Column lists send that batch as one storage read, so the join is the outer scan plus that
  // one read. Wide keys make the permutation path flush the same batch into many more reads.
  @Test
  public void testHashColumnListsBnlJoin() throws Exception {
    final int tablets = 3;
    final int keyLen = 8192;
    final int numRows = 100;
    final String outer = "t_bnl_outer";
    final String inner = "t_bnl_inner";
    // One parallel outer scan, one inner batch.
    final long optimalRequests = 2;

    try (Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TABLE " + outer +
          " (h text, v int, PRIMARY KEY (h HASH)) SPLIT INTO " + tablets + " TABLETS");
      statement.execute(
          "CREATE TABLE " + inner +
          " (h text, v int, PRIMARY KEY (h HASH)) SPLIT INTO " + tablets + " TABLETS");
      insertWideRows(statement, outer, keyLen, numRows);
      insertWideRows(statement, inner, keyLen, numRows);

      statement.execute("SET yb_bnl_batch_size = 1024");
      statement.execute("SET yb_enable_hash_batch_in = true");
      statement.execute("SET yb_fetch_row_limit = 10000");
      statement.execute("SET work_mem = '64kB'");
      statement.execute("SET enable_hashjoin = off");
      statement.execute("SET enable_mergejoin = off");
      statement.execute("SET enable_seqscan = off");
      statement.execute("SET enable_material = off");

      String query =
          "SELECT o.v, i.v FROM " + outer + " o JOIN " + inner + " i ON o.h = i.h";
      String explain = getExplainAnalyzeOutput(statement, query);
      LOG.info("hash column lists bnl plan:\n{}", explain);
      assertTrue("expected YB Batched Nested Loop, plan was:\n" + explain,
          explain.contains("YB Batched Nested Loop"));

      Set<Row> expectedRows = new HashSet<>();
      for (int i = 1; i <= numRows; ++i) {
        expectedRows.add(new Row(i, i));
      }

      statement.execute("SET yb_push_hash_column_lists_to_docdb = off");
      long requestsOff = getNumStorageRoundtrips(statement, query);
      logReadOpsAndRowsScanned("hash column lists bnl off", statement, query);
      LOG.info("hash column lists bnl off: Storage Read Requests={}", requestsOff);
      assertRowSet(statement, query, expectedRows);

      statement.execute("SET yb_push_hash_column_lists_to_docdb = on");
      long requestsOn = getNumStorageRoundtrips(statement, query);
      logReadOpsAndRowsScanned("hash column lists bnl on", statement, query);
      LOG.info("hash column lists bnl on: Storage Read Requests={}", requestsOn);
      assertRowSet(statement, query, expectedRows);

      assertEquals("flag on storage read requests", optimalRequests, requestsOn);
      assertTrue(
          "flag off storage read requests " + requestsOff +
          " not many more than flag on " + requestsOn,
          requestsOff >= requestsOn + 8);
    }
  }

  // Inclusive hash-code bounds of the tablet that starts at hash 0.
  private int[] firstTabletHashRange(Statement statement, String table) throws Exception {
    int end = -1;
    ResultSet rs = statement.executeQuery(
        "SELECT encode(partition_key_start, 'hex'), encode(partition_key_end, 'hex') " +
        "FROM yb_local_tablets WHERE table_name = '" + table + "'");
    while (rs.next()) {
      String startHex = rs.getString(1);
      if (startHex != null && !startHex.isEmpty()) {
        continue;
      }
      String endHex = rs.getString(2);
      end = Integer.parseInt(endHex, 16);
      break;
    }
    rs.close();
    assertTrue("no tablet starts at hash 0", end > 0);
    return new int[] {0, end - 1};
  }

  // inKeys all hash into [lo, hi]. otherKeys are a few that do not, so a scan of every tablet
  // would return them.
  private void collectKeys(
      Statement statement, int keyLen, int lo, int hi, int needed,
      List<String> inKeys, List<String> otherKeys) throws Exception {
    final int batch = 40;
    int probe = 0;
    while (inKeys.size() < needed) {
      List<String> candidates = new ArrayList<>();
      for (int i = 0; i < batch; ++i, ++probe) {
        candidates.add(wideKey(keyLen, probe));
      }
      ResultSet rs = statement.executeQuery(
          "SELECT yb_hash_code(k) FROM unnest(ARRAY[" + quotedList(candidates) + "]) k");
      int i = 0;
      while (rs.next()) {
        int hash = rs.getInt(1);
        String key = candidates.get(i++);
        if (hash >= lo && hash <= hi) {
          if (inKeys.size() < needed) {
            inKeys.add(key);
          }
        } else if (otherKeys.size() < 16) {
          otherKeys.add(key);
        }
      }
      rs.close();
      assertTrue("ran out of probes looking for keys in [" + lo + ", " + hi + "]", probe < 100000);
    }
  }

  private static String wideKey(int keyLen, int probe) {
    char[] chars = new char[keyLen];
    Arrays.fill(chars, 'k');
    char[] suffix = Integer.toHexString(probe).toCharArray();
    System.arraycopy(suffix, 0, chars, keyLen - suffix.length, suffix.length);
    return new String(chars);
  }

  private static String quotedList(List<String> values) {
    StringBuilder sql = new StringBuilder();
    for (int i = 0; i < values.size(); ++i) {
      if (i > 0) {
        sql.append(", ");
      }
      sql.append('\'').append(values.get(i)).append('\'');
    }
    return sql.toString();
  }

  private static void insertKey(Statement statement, String table, String key, int v)
      throws Exception {
    statement.execute("INSERT INTO " + table + " VALUES ('" + key + "', " + v + ")");
  }

  private static void insertWideRows(Statement statement, String table, int keyLen, int numRows)
      throws Exception {
    StringBuilder keys = new StringBuilder();
    StringBuilder vals = new StringBuilder();
    for (int i = 1; i <= numRows; ++i) {
      if (i > 1) {
        keys.append(", ");
        vals.append(", ");
      }
      keys.append('\'').append(wideKey(keyLen, i)).append('\'');
      vals.append(i);
    }
    statement.execute(
        "INSERT INTO " + table + " SELECT k, v FROM unnest(ARRAY[" + keys +
        "]::text[], ARRAY[" + vals + "]::int[]) AS u(k, v)");
  }

  private void logReadOpsAndRowsScanned(String label, Statement statement, String query)
      throws Exception {
    LOG.info("{}: Storage Read Ops={}, Storage Rows Scanned={}",
        label,
        getNumStorageReadOps(statement, query),
        getNumStorageRowsScanned(statement, query));
  }

  @Test
  public void schedule() throws Exception {
    runPgRegressTest("yb_hash_in_schedule");
  }
}
