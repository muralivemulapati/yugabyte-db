// Copyright (c) YugabyteDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under
// the License.
//
// YSQL client for a batched nested-loop join of two hash-PK tables.
// One batch of --rows outer keys (default 1024, matching yb_bnl_batch_size)
// joined to the same keys on the inner table. --key-size is the varchar
// length in bytes. On the baseline hash-batch path, wide keys make pggate
// flush ROW(yb_hash_code, h) tuples against work_mem.
//
//   bnl_client create-schema --key-size 32768 --tablets 8 --rows 1024
//   bnl_client explain --host <ysql-host>
//   bnl_client query --iterations 10

#include <libpq-fe.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

namespace {

constexpr int kBatchSize = 1024;
constexpr size_t kMinKeySize = 16;
constexpr size_t kMaxKeySize = 262144;

uint64_t SplitMix64(uint64_t x) {
  x += 0x9e3779b97f4a7c15ULL;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
  return x ^ (x >> 31);
}

std::string ToHex(uint64_t v) {
  static const char kHex[] = "0123456789abcdef";
  std::string out(16, '0');
  for (size_t i = 0; i < 16; ++i) {
    out[15 - i] = kHex[(v >> (i * 4)) & 0xf];
  }
  return out;
}

// Unique key of exactly len bytes. The first 16 bytes are a hex row id.
std::string MakeKey(uint64_t index, size_t len) {
  std::string out = ToHex(SplitMix64(index + 1));
  if (len < out.size()) {
    out.resize(len);
    return out;
  }
  out.resize(len, 'a');
  return out;
}

void Die(const std::string& msg) {
  std::cerr << "error: " << msg << std::endl;
  std::exit(1);
}

void Check(PGconn* conn, PGresult* res, const char* what) {
  const ExecStatusType st = PQresultStatus(res);
  if (st == PGRES_COMMAND_OK || st == PGRES_TUPLES_OK || st == PGRES_COPY_IN) {
    return;
  }
  std::ostringstream oss;
  oss << what << ": " << PQerrorMessage(conn);
  PQclear(res);
  Die(oss.str());
}

void Exec(PGconn* conn, const std::string& sql, const char* what) {
  PGresult* res = PQexec(conn, sql.c_str());
  Check(conn, res, what);
  PQclear(res);
}

std::string QuoteIdent(const std::string& s) {
  std::string out = "\"";
  for (char c : s) {
    if (c == '"') {
      out += "\"\"";
    } else {
      out += c;
    }
  }
  out += '"';
  return out;
}

struct Options {
  std::string mode;
  std::string host = "35.161.151.104,34.216.247.21,35.91.196.110";
  std::string port = "5433";
  std::string user = "yugabyte";
  std::string dbname = "yugabyte";
  std::string password;
  std::string sslmode = "require";
  std::string outer_table = "bnl_outer";
  std::string inner_table = "bnl_inner";
  int tablets = 8;
  uint64_t rows = kBatchSize;
  size_t key_size = 0;
  int iterations = 10;
  int warmup = 1;
  std::string explain_options = "ANALYZE, DIST, COSTS OFF, TIMING OFF";
};

void Usage(const char* argv0) {
  std::cerr
      << "Usage:\n"
      << "  " << argv0 << " create-schema --key-size N [options]\n"
      << "  " << argv0 << " query --key-size N [options]\n"
      << "  " << argv0 << " explain --key-size N [options]\n"
      << "\n"
      << "create-schema  DROP+CREATE two varchar HASH-PK tables and COPY --rows\n"
      << "               identical keys of --key-size bytes into both.\n"
      << "query          SELECT count(*) of the equijoin, timed. Settings force\n"
      << "               one YB Batched Nested Loop batch (yb_bnl_batch_size="
      << kBatchSize << ").\n"
      << "explain        Same SQL wrapped in EXPLAIN (ANALYZE, DIST).\n"
      << "\n"
      << "Connection:\n"
      << "  --host HOSTS          comma-separated YSQL hosts\n"
      << "  --port PORT           default 5433\n"
      << "  --user USER           default yugabyte\n"
      << "  --dbname DB           default yugabyte\n"
      << "  --password PASS       also read from PGPASSWORD / YSQL_PASSWORD\n"
      << "  --sslmode MODE        default require\n"
      << "\n"
      << "  --key-size N          varchar key length in bytes (required, "
      << kMinKeySize << ".." << kMaxKeySize << ")\n"
      << "  --outer-table NAME    default bnl_outer\n"
      << "  --inner-table NAME    default bnl_inner\n"
      << "  --tablets N           HASH tablets on each table (default 8)\n"
      << "  --rows N              rows in each table (default " << kBatchSize
      << ", one BNL batch)\n"
      << "  --iterations N        timed runs (query default 10, explain default 1)\n"
      << "  --warmup N            untimed runs first (query default 1, explain 0)\n"
      << "  --explain-options S   explain mode only\n";
}

int64_t ParseI64(const std::string& s, const char* flag) {
  try {
    return std::stoll(s);
  } catch (...) {
    Die(std::string("invalid integer for ") + flag + ": " + s);
  }
  return 0;
}

Options ParseArgs(int argc, char** argv) {
  if (argc < 2) {
    Usage(argv[0]);
    std::exit(2);
  }
  Options o;
  o.mode = argv[1];
  if (o.mode == "-h" || o.mode == "--help") {
    Usage(argv[0]);
    std::exit(0);
  }
  if (o.mode == "create" || o.mode == "schema") {
    o.mode = "create-schema";
  }
  if (o.mode != "create-schema" && o.mode != "query" && o.mode != "explain") {
    Usage(argv[0]);
    Die("mode must be create-schema, query, or explain");
  }

  if (const char* p = std::getenv("PGPASSWORD")) {
    o.password = p;
  } else if (const char* p = std::getenv("YSQL_PASSWORD")) {
    o.password = p;
  }

  auto need = [&](int i, const char* flag) {
    if (i + 1 >= argc) {
      Die(std::string(flag) + " requires a value");
    }
    return std::string(argv[i + 1]);
  };

  bool iterations_set = false;
  bool warmup_set = false;
  bool key_size_set = false;
  for (int i = 2; i < argc; ++i) {
    const std::string a = argv[i];
    if (a == "-h" || a == "--help") {
      Usage(argv[0]);
      std::exit(0);
    } else if (a == "--host") {
      o.host = need(i++, a.c_str());
    } else if (a == "--port") {
      o.port = need(i++, a.c_str());
    } else if (a == "--user") {
      o.user = need(i++, a.c_str());
    } else if (a == "--dbname") {
      o.dbname = need(i++, a.c_str());
    } else if (a == "--password") {
      o.password = need(i++, a.c_str());
    } else if (a == "--sslmode") {
      o.sslmode = need(i++, a.c_str());
    } else if (a == "--outer-table") {
      o.outer_table = need(i++, a.c_str());
    } else if (a == "--inner-table") {
      o.inner_table = need(i++, a.c_str());
    } else if (a == "--tablets") {
      o.tablets = static_cast<int>(ParseI64(need(i++, a.c_str()), "--tablets"));
    } else if (a == "--rows") {
      o.rows = static_cast<uint64_t>(ParseI64(need(i++, a.c_str()), "--rows"));
    } else if (a == "--key-size") {
      o.key_size = static_cast<size_t>(ParseI64(need(i++, a.c_str()), "--key-size"));
      key_size_set = true;
    } else if (a == "--iterations") {
      o.iterations = static_cast<int>(ParseI64(need(i++, a.c_str()), "--iterations"));
      iterations_set = true;
    } else if (a == "--warmup") {
      o.warmup = static_cast<int>(ParseI64(need(i++, a.c_str()), "--warmup"));
      warmup_set = true;
    } else if (a == "--explain-options") {
      o.explain_options = need(i++, a.c_str());
    } else {
      Die("unknown flag: " + a);
    }
  }

  if (o.mode == "explain") {
    if (!iterations_set) o.iterations = 1;
    if (!warmup_set) o.warmup = 0;
  }
  if (!key_size_set) Die("--key-size is required");
  if (o.key_size < kMinKeySize || o.key_size > kMaxKeySize) {
    Die("--key-size must be in [" + std::to_string(kMinKeySize) + ", " +
        std::to_string(kMaxKeySize) + "]");
  }
  if (o.tablets < 1) Die("--tablets must be >= 1");
  if (o.rows < 1) Die("--rows must be >= 1");
  if (o.iterations < 1) Die("--iterations must be >= 1");
  if (o.warmup < 0) Die("--warmup must be >= 0");
  if (o.explain_options.empty()) Die("--explain-options must not be empty");
  if (o.outer_table.empty() || o.inner_table.empty()) Die("table name must not be empty");
  return o;
}

PGconn* Connect(const Options& o) {
  std::ostringstream cs;
  cs << "host=" << o.host << " port=" << o.port << " user=" << o.user << " dbname=" << o.dbname
     << " sslmode=" << o.sslmode << " connect_timeout=15 application_name=bnl_client";
  if (!o.password.empty()) {
    cs << " password=" << o.password;
  }
  PGconn* conn = PQconnectdb(cs.str().c_str());
  if (PQstatus(conn) != CONNECTION_OK) {
    const std::string err = PQerrorMessage(conn);
    PQfinish(conn);
    Die("connect: " + err);
  }
  return conn;
}

void CopyKeys(PGconn* conn, const std::string& table, const Options& o) {
  const std::string sql = "COPY " + QuoteIdent(table) + " (h, v) FROM STDIN";
  PGresult* copy_res = PQexec(conn, sql.c_str());
  Check(conn, copy_res, "COPY FROM STDIN");
  PQclear(copy_res);

  std::string buf;
  buf.reserve(std::min<size_t>(o.rows * (o.key_size + 16), 8 << 20));
  for (uint64_t i = 0; i < o.rows; ++i) {
    buf += MakeKey(i, o.key_size);
    buf += '\t';
    buf += std::to_string(i);
    buf += '\n';
    if (buf.size() >= (4 << 20)) {
      if (PQputCopyData(conn, buf.data(), static_cast<int>(buf.size())) != 1) {
        Die(std::string("COPY put: ") + PQerrorMessage(conn));
      }
      buf.clear();
    }
  }
  if (!buf.empty() && PQputCopyData(conn, buf.data(), static_cast<int>(buf.size())) != 1) {
    Die(std::string("COPY put: ") + PQerrorMessage(conn));
  }
  if (PQputCopyEnd(conn, nullptr) != 1) {
    Die(std::string("COPY end: ") + PQerrorMessage(conn));
  }
  PGresult* end_res = PQgetResult(conn);
  Check(conn, end_res, "COPY complete");
  PQclear(end_res);
}

void CreateOneTable(PGconn* conn, const std::string& table, const Options& o) {
  const std::string qtable = QuoteIdent(table);
  Exec(conn, "DROP TABLE IF EXISTS " + qtable, "DROP TABLE");
  const std::string ddl = "CREATE TABLE " + qtable +
                           " (h varchar PRIMARY KEY, v int) SPLIT INTO " +
                           std::to_string(o.tablets) + " TABLETS";
  std::cout << "DDL: " << ddl << ";\n";
  Exec(conn, ddl, "CREATE TABLE");
  const auto t0 = std::chrono::steady_clock::now();
  CopyKeys(conn, table, o);
  const auto t1 = std::chrono::steady_clock::now();
  const double sec = std::chrono::duration<double>(t1 - t0).count();
  std::cout << "inserted " << o.rows << " rows into " << table << " in " << std::fixed
            << std::setprecision(3) << sec << " s\n";
  Exec(conn, "ANALYZE " + qtable, "ANALYZE");
}

void CreateSchema(PGconn* conn, const Options& o) {
  CreateOneTable(conn, o.outer_table, o);
  CreateOneTable(conn, o.inner_table, o);
  std::cout << "schema ready: outer=" << o.outer_table << "  inner=" << o.inner_table
            << "  tablets=" << o.tablets << "  rows=" << o.rows
            << "  key_size=" << o.key_size << std::endl;
}

std::string JoinSql(const Options& o) {
  return "SELECT count(*) FROM " + QuoteIdent(o.outer_table) + " o JOIN " +
         QuoteIdent(o.inner_table) + " i ON o.h = i.h";
}

void ApplyJoinSettings(PGconn* conn, const Options& o) {
  // One inner index probe per batch. Fetch limit stays above the join
  // cardinality so a full batch is not split by paging.
  const std::string fetch = std::to_string(std::max<uint64_t>(o.rows, 100000) + 1);
  const std::vector<std::string> settings = {
      "SET yb_bnl_batch_size = " + std::to_string(kBatchSize),
      "SET yb_enable_batchednl = on",
      "SET yb_prefer_bnl = on",
      "SET yb_enable_hash_batch_in = on",
      "SET yb_fetch_row_limit = " + fetch,
      "SET enable_hashjoin = off",
      "SET enable_mergejoin = off",
      "SET enable_material = off",
      "SET enable_seqscan = off",
      "SET enable_nestloop = on",
  };
  for (const auto& sql : settings) {
    std::cout << sql << ";\n";
    Exec(conn, sql, "SET");
  }
  std::cout << "key_size=" << o.key_size << "  rows=" << o.rows
            << "  batch=" << kBatchSize << "  work_mem=session default\n";
}

double Percentile(std::vector<double> xs, double p) {
  if (xs.empty()) return 0;
  std::sort(xs.begin(), xs.end());
  const double idx = p * static_cast<double>(xs.size() - 1);
  const size_t lo = static_cast<size_t>(idx);
  const size_t hi = std::min(lo + 1, xs.size() - 1);
  const double frac = idx - static_cast<double>(lo);
  return xs[lo] * (1.0 - frac) + xs[hi] * frac;
}

bool TakePrefixedLong(const std::string& line, const char* key, std::optional<int64_t>& dest) {
  if (dest) return false;
  const std::string needle = std::string(key) + ":";
  const auto pos = line.find(needle);
  if (pos == std::string::npos) return false;
  const char* p = line.c_str() + pos + needle.size();
  while (*p == ' ' || *p == '\t') ++p;
  char* end = nullptr;
  const int64_t v = std::strtoll(p, &end, 10);
  if (end == p) return false;
  dest = v;
  return true;
}

bool TakePrefixedDouble(const std::string& line, const char* key, std::optional<double>& dest) {
  if (dest) return false;
  const std::string needle = std::string(key) + ":";
  const auto pos = line.find(needle);
  if (pos == std::string::npos) return false;
  const char* p = line.c_str() + pos + needle.size();
  while (*p == ' ' || *p == '\t') ++p;
  char* end = nullptr;
  const double v = std::strtod(p, &end);
  if (end == p) return false;
  dest = v;
  return true;
}

struct DistMetrics {
  std::optional<int64_t> table_read_requests;
  std::optional<int64_t> read_requests;
  std::optional<int64_t> table_read_ops;
  std::optional<int64_t> read_ops;
  std::optional<int64_t> table_rows_scanned;
  std::optional<int64_t> rows_scanned;
  std::optional<double> exec_ms;

  int64_t Requests() const {
    if (table_read_requests) return *table_read_requests;
    if (read_requests) return *read_requests;
    return -1;
  }
  int64_t Ops() const {
    if (table_read_ops) return *table_read_ops;
    if (read_ops) return *read_ops;
    return -1;
  }
  int64_t RowsScanned() const {
    if (table_rows_scanned) return *table_rows_scanned;
    if (rows_scanned) return *rows_scanned;
    return -1;
  }
};

DistMetrics ParseDistMetrics(PGresult* res) {
  DistMetrics m;
  const int n = PQntuples(res);
  for (int r = 0; r < n; ++r) {
    const std::string line = PQgetvalue(res, r, 0);
    TakePrefixedLong(line, "Storage Table Read Requests", m.table_read_requests);
    TakePrefixedLong(line, "Storage Read Requests", m.read_requests);
    TakePrefixedLong(line, "Storage Table Read Ops", m.table_read_ops);
    TakePrefixedLong(line, "Storage Read Ops", m.read_ops);
    TakePrefixedLong(line, "Storage Table Rows Scanned", m.table_rows_scanned);
    TakePrefixedLong(line, "Storage Rows Scanned", m.rows_scanned);
    TakePrefixedDouble(line, "Execution Time", m.exec_ms);
  }
  return m;
}

std::string FmtMetric(int64_t v) { return v < 0 ? std::string("-") : std::to_string(v); }

bool PlanHasBnl(PGresult* res) {
  const int n = PQntuples(res);
  for (int r = 0; r < n; ++r) {
    const std::string line = PQgetvalue(res, r, 0);
    if (line.find("YB Batched Nested Loop") != std::string::npos) return true;
  }
  return false;
}

void PrintPlan(PGresult* res) {
  const int n = PQntuples(res);
  for (int r = 0; r < n; ++r) {
    std::cout << PQgetvalue(res, r, 0) << "\n";
  }
}

void RunQuery(PGconn* conn, const Options& o) {
  ApplyJoinSettings(conn, o);
  const std::string sql = JoinSql(o);
  std::cout << "SQL: " << sql << "\n";
  std::vector<double> lat_ms;
  lat_ms.reserve(static_cast<size_t>(o.iterations));
  const int total = o.warmup + o.iterations;
  for (int i = 0; i < total; ++i) {
    const auto t0 = std::chrono::steady_clock::now();
    PGresult* res = PQexec(conn, sql.c_str());
    const auto t1 = std::chrono::steady_clock::now();
    Check(conn, res, "SELECT");
    const char* count = PQntuples(res) == 1 ? PQgetvalue(res, 0, 0) : "-";
    PQclear(res);
    const double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    const bool timed = i >= o.warmup;
    if (timed) lat_ms.push_back(ms);
    std::cout << (timed ? "query " : "warmup ") << (timed ? (i - o.warmup + 1) : (i + 1))
              << std::fixed << std::setprecision(3) << "  " << ms << " ms  count=" << count
              << std::endl;
  }
  const double sum = std::accumulate(lat_ms.begin(), lat_ms.end(), 0.0);
  const double avg = sum / static_cast<double>(lat_ms.size());
  std::cout << std::fixed << std::setprecision(3) << "latency_ms  n=" << lat_ms.size()
            << "  min=" << *std::min_element(lat_ms.begin(), lat_ms.end())
            << "  p50=" << Percentile(lat_ms, 0.50) << "  p95=" << Percentile(lat_ms, 0.95)
            << "  p99=" << Percentile(lat_ms, 0.99) << "  avg=" << avg
            << "  max=" << *std::max_element(lat_ms.begin(), lat_ms.end()) << std::endl;
}

void RunExplain(PGconn* conn, const Options& o) {
  ApplyJoinSettings(conn, o);
  const std::string sql = JoinSql(o);
  std::cout << "SQL: " << sql << "\n";
  const std::string explain_sql = "EXPLAIN (" + o.explain_options + ") " + sql;
  std::vector<double> requests;
  std::vector<double> ops;
  std::vector<double> exec_ms;
  bool printed_plan = false;
  const int total = o.warmup + o.iterations;
  for (int i = 0; i < total; ++i) {
    const auto t0 = std::chrono::steady_clock::now();
    PGresult* res = PQexec(conn, explain_sql.c_str());
    const auto t1 = std::chrono::steady_clock::now();
    Check(conn, res, "EXPLAIN");
    const DistMetrics m = ParseDistMetrics(res);
    const bool timed = i >= o.warmup;
    if (timed && !printed_plan) {
      std::cout << "bnl=" << (PlanHasBnl(res) ? "yes" : "no") << "\nplan:\n";
      PrintPlan(res);
      printed_plan = true;
    }
    PQclear(res);
    const double wall_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    if (timed) {
      if (m.Requests() >= 0) requests.push_back(static_cast<double>(m.Requests()));
      if (m.Ops() >= 0) ops.push_back(static_cast<double>(m.Ops()));
      if (m.exec_ms) exec_ms.push_back(*m.exec_ms);
    }
    std::cout << (timed ? "explain " : "warmup ") << (timed ? (i - o.warmup + 1) : (i + 1))
              << "  requests=" << FmtMetric(m.Requests()) << "  ops=" << FmtMetric(m.Ops())
              << "  rows_scanned=" << FmtMetric(m.RowsScanned()) << "  exec_ms=";
    if (m.exec_ms) {
      std::cout << std::fixed << std::setprecision(3) << *m.exec_ms;
    } else {
      std::cout << "-";
    }
    std::cout << std::fixed << std::setprecision(3) << "  wall_ms=" << wall_ms << std::endl;
  }

  auto summarize = [](const char* name, const std::vector<double>& xs) {
    if (xs.empty()) {
      std::cout << name << "  n=0\n";
      return;
    }
    const double sum = std::accumulate(xs.begin(), xs.end(), 0.0);
    std::cout << std::fixed << std::setprecision(3) << name << "  n=" << xs.size()
              << "  min=" << *std::min_element(xs.begin(), xs.end())
              << "  p50=" << Percentile(xs, 0.50)
              << "  avg=" << (sum / static_cast<double>(xs.size()))
              << "  max=" << *std::max_element(xs.begin(), xs.end()) << std::endl;
  };
  summarize("requests", requests);
  summarize("ops", ops);
  summarize("exec_ms", exec_ms);
}

}  // namespace

int main(int argc, char** argv) {
  const Options o = ParseArgs(argc, argv);
  PGconn* conn = Connect(o);
  std::cout << "connected to " << PQhost(conn) << ":" << PQport(conn) << " db=" << PQdb(conn)
            << " user=" << PQuser(conn) << " ssl=" << (PQsslInUse(conn) ? "yes" : "no")
            << std::endl;
  try {
    if (o.mode == "create-schema") {
      CreateSchema(conn, o);
    } else if (o.mode == "explain") {
      RunExplain(conn, o);
    } else {
      RunQuery(conn, o);
    }
  } catch (const std::exception& e) {
    PQfinish(conn);
    Die(e.what());
  }
  PQfinish(conn);
  return 0;
}
