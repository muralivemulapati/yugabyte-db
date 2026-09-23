// YSQL client for HASH-sharded varchar IN-list latency tests.
// Default connection is the portal.dev universe murali-d58130 (client-to-node TLS).
//
//   in_list_client create-schema --tablets 8 --rows 100000 --subkeys 1
//   in_list_client query --in-list-size 64 --iterations 20 --rows 100000 --subkeys 1
//   in_list_client explain --in-list-size 64 --subkeys 2 --independent-in

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
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <vector>

namespace {

// SplitMix64: deterministic unique-looking keys from a row index.
uint64_t SplitMix64(uint64_t x) {
  x += 0x9e3779b97f4a7c15ULL;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
  return x ^ (x >> 31);
}

std::string ToHex(uint64_t v, size_t len) {
  static const char kHex[] = "0123456789abcdef";
  std::string out(len, '0');
  for (size_t i = 0; i < len; ++i) {
    out[len - 1 - i] = kHex[(v >> ((i % 16) * 4)) & 0xf];
    if (i == 15) {
      v = SplitMix64(v);
    }
  }
  return out;
}

// Key space used by both create-schema inserts and query IN lists, so queries
// look up values that exist unless --miss-rate is set.
std::string MakeKey(uint64_t index, size_t len) { return ToHex(SplitMix64(index + 1), len); }

// Subkey j of row index. --subkeys 1 matches the historical single-column MakeKey
// space so existing in_list_bench tables keep working.
std::string MakeSubKey(uint64_t index, int subkey, int nsub, uint64_t seed, size_t len) {
  if (nsub == 1) {
    return MakeKey(index ^ seed, len);
  }
  return ToHex(
      SplitMix64(((index ^ seed) + 1) * 0x10007ULL + static_cast<uint64_t>(subkey + 1)), len);
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

std::string QuoteIdent(const std::string& ident) {
  std::string out = "\"";
  for (char c : ident) {
    if (c == '"') {
      out += "\"\"";
    } else {
      out += c;
    }
  }
  out += '"';
  return out;
}

std::string QuoteLiteral(const std::string& s) {
  std::string out = "'";
  for (char c : s) {
    if (c == '\'') {
      out += "''";
    } else {
      out += c;
    }
  }
  out += "'";
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
  std::string table = "in_list_bench";
  int tablets = 8;
  int subkeys = 1;
  uint64_t rows = 10000;
  uint64_t batch_size = 1024;
  int in_list_size = 64;
  int iterations = 10;
  int warmup = 1;
  size_t value_len = 16;
  double miss_rate = 0.0;
  uint64_t seed = 1;
  bool independent_in = false;
  std::string explain_options = "ANALYZE, DIST, COSTS OFF, TIMING OFF";
};

std::string ColName(int i, int nsub) {
  if (nsub == 1) {
    return "k";
  }
  return "k" + std::to_string(i + 1);
}

// quoted column list: k  or  k1, k2, ...
std::string HashColsSql(const Options& o) {
  std::ostringstream oss;
  for (int i = 0; i < o.subkeys; ++i) {
    if (i) oss << ", ";
    oss << QuoteIdent(ColName(i, o.subkeys));
  }
  return oss.str();
}

std::vector<std::string> MakeRowKeys(uint64_t index, const Options& o) {
  std::vector<std::string> keys;
  keys.reserve(static_cast<size_t>(o.subkeys));
  for (int j = 0; j < o.subkeys; ++j) {
    keys.push_back(MakeSubKey(index, j, o.subkeys, o.seed, o.value_len));
  }
  return keys;
}

std::string FormatInValue(const std::vector<std::string>& keys) {
  if (keys.size() == 1) {
    return QuoteLiteral(keys[0]);
  }
  std::ostringstream oss;
  oss << "(";
  for (size_t i = 0; i < keys.size(); ++i) {
    if (i) oss << ", ";
    oss << QuoteLiteral(keys[i]);
  }
  oss << ")";
  return oss.str();
}

void Usage(const char* argv0) {
  std::cerr
      << "Usage:\n"
      << "  " << argv0 << " create-schema [options]\n"
      << "  " << argv0 << " query [options]\n"
      << "  " << argv0 << " explain [options]\n"
      << "\n"
      << "create-schema  DROP+CREATE a varchar HASH-PK table with --tablets tablets\n"
      << "               and --subkeys HASH columns, then COPY in --rows keys.\n"
      << "query          SELECT hash cols with an IN-list of --in-list-size keys;\n"
      << "               print per-query and summary latency.\n"
      << "explain        Same SQL as query, wrapped in EXPLAIN (ANALYZE, DIST);\n"
      << "               print the plan once and Storage Read Requests / Ops.\n"
      << "\n"
      << "Connection (defaults: portal universe murali-d58130, TLS required):\n"
      << "  --host HOSTS          comma-separated YSQL hosts\n"
      << "  --port PORT           default 5433\n"
      << "  --user USER           default yugabyte\n"
      << "  --dbname DB           default yugabyte\n"
      << "  --password PASS       also read from PGPASSWORD / YSQL_PASSWORD\n"
      << "  --sslmode MODE        default require\n"
      << "  --table NAME          default in_list_bench\n"
      << "  --subkeys N           HASH PK columns (default 1; must match create-schema)\n"
      << "\n"
      << "create-schema:\n"
      << "  --tablets N           HASH tablets (default 8)\n"
      << "  --rows N              rows to insert (default 10000)\n"
      << "  --batch-size N        COPY flush size (default 1024)\n"
      << "  --value-len N         varchar key length (default 16)\n"
      << "  --seed N              key-space seed (default 1)\n"
      << "\n"
      << "query / explain:\n"
      << "  --in-list-size N      IN-list length per list (default 64)\n"
      << "  --independent-in      WHERE k1 IN (...) AND k2 IN (...), not ROW IN.\n"
      << "                       No-op with --subkeys 1. Cartesian product is\n"
      << "                       in-list-size ^ subkeys permutations in pggate.\n"
      << "  --iterations N        timed queries (query default 10, explain default 1)\n"
      << "  --warmup N            untimed queries first (query default 1, explain 0)\n"
      << "  --rows N              key-space size, must match create-schema (default 10000)\n"
      << "  --value-len N         must match create-schema (default 16)\n"
      << "  --miss-rate F         fraction of IN values that are non-existent (default 0)\n"
      << "  --seed N              key-space seed (default 1)\n"
      << "  --explain-options S   EXPLAIN options (default ANALYZE, DIST, COSTS OFF,\n"
      << "                       TIMING OFF); explain mode only\n";
}

int64_t ParseI64(const std::string& s, const char* flag) {
  try {
    return std::stoll(s);
  } catch (...) {
    Die(std::string("invalid integer for ") + flag + ": " + s);
  }
  return 0;
}

double ParseDouble(const std::string& s, const char* flag) {
  try {
    return std::stod(s);
  } catch (...) {
    Die(std::string("invalid number for ") + flag + ": " + s);
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
  } else if (o.mode == "query-mode") {
    o.mode = "query";
  } else if (o.mode == "explain-mode") {
    o.mode = "explain";
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
    } else if (a == "--table") {
      o.table = need(i++, a.c_str());
    } else if (a == "--tablets") {
      o.tablets = static_cast<int>(ParseI64(need(i++, a.c_str()), "--tablets"));
    } else if (a == "--subkeys") {
      o.subkeys = static_cast<int>(ParseI64(need(i++, a.c_str()), "--subkeys"));
    } else if (a == "--rows") {
      o.rows = static_cast<uint64_t>(ParseI64(need(i++, a.c_str()), "--rows"));
    } else if (a == "--batch-size") {
      o.batch_size = static_cast<uint64_t>(ParseI64(need(i++, a.c_str()), "--batch-size"));
    } else if (a == "--in-list-size") {
      o.in_list_size = static_cast<int>(ParseI64(need(i++, a.c_str()), "--in-list-size"));
    } else if (a == "--iterations") {
      o.iterations = static_cast<int>(ParseI64(need(i++, a.c_str()), "--iterations"));
      iterations_set = true;
    } else if (a == "--warmup") {
      o.warmup = static_cast<int>(ParseI64(need(i++, a.c_str()), "--warmup"));
      warmup_set = true;
    } else if (a == "--value-len") {
      o.value_len = static_cast<size_t>(ParseI64(need(i++, a.c_str()), "--value-len"));
    } else if (a == "--miss-rate") {
      o.miss_rate = ParseDouble(need(i++, a.c_str()), "--miss-rate");
    } else if (a == "--seed") {
      o.seed = static_cast<uint64_t>(ParseI64(need(i++, a.c_str()), "--seed"));
    } else if (a == "--independent-in") {
      o.independent_in = true;
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
  if (o.tablets < 1) Die("--tablets must be >= 1");
  if (o.subkeys < 1 || o.subkeys > 16) Die("--subkeys must be in [1, 16]");
  if (o.rows < 1) Die("--rows must be >= 1");
  if (o.batch_size < 1) Die("--batch-size must be >= 1");
  if (o.in_list_size < 1) Die("--in-list-size must be >= 1");
  if (o.iterations < 1) Die("--iterations must be >= 1");
  if (o.warmup < 0) Die("--warmup must be >= 0");
  if (o.value_len < 1 || o.value_len > 64) Die("--value-len must be in [1, 64]");
  if (o.miss_rate < 0.0 || o.miss_rate > 1.0) Die("--miss-rate must be in [0, 1]");
  if (o.explain_options.empty()) Die("--explain-options must not be empty");
  return o;
}

PGconn* Connect(const Options& o) {
  std::ostringstream cs;
  cs << "host=" << o.host << " port=" << o.port << " user=" << o.user << " dbname=" << o.dbname
     << " sslmode=" << o.sslmode << " connect_timeout=15 application_name=in_list_client";
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

void CreateSchema(PGconn* conn, const Options& o) {
  const std::string qtable = QuoteIdent(o.table);
  const std::string cols = HashColsSql(o);
  Exec(conn, "DROP TABLE IF EXISTS " + qtable, "DROP TABLE");

  std::ostringstream ddl;
  ddl << "CREATE TABLE " << qtable << " (\n";
  for (int i = 0; i < o.subkeys; ++i) {
    ddl << "  " << QuoteIdent(ColName(i, o.subkeys)) << " VARCHAR(" << o.value_len
        << ") NOT NULL,\n";
  }
  if (o.subkeys == 1) {
    ddl << "  PRIMARY KEY (" << cols << " HASH)\n";
  } else {
    ddl << "  PRIMARY KEY ((" << cols << ") HASH)\n";
  }
  ddl << ") SPLIT INTO " << o.tablets << " TABLETS";
  const std::string ddl_sql = ddl.str();
  std::cout << "DDL:\n" << ddl_sql << ";\n";
  Exec(conn, ddl_sql, "CREATE TABLE");

  const auto t0 = std::chrono::steady_clock::now();
  PGresult* copy_res = PQexec(conn, ("COPY " + qtable + " (" + cols + ") FROM STDIN").c_str());
  Check(conn, copy_res, "COPY FROM STDIN");
  PQclear(copy_res);

  std::string buf;
  buf.reserve(static_cast<size_t>(o.batch_size) * (o.value_len + 1) *
              static_cast<size_t>(o.subkeys));
  uint64_t sent = 0;
  for (uint64_t i = 0; i < o.rows; ++i) {
    const std::vector<std::string> keys = MakeRowKeys(i, o);
    for (size_t j = 0; j < keys.size(); ++j) {
      if (j) buf += '\t';
      buf += keys[j];
    }
    buf += '\n';
    ++sent;
    if (sent % o.batch_size == 0) {
      if (PQputCopyData(conn, buf.data(), static_cast<int>(buf.size())) != 1) {
        Die(std::string("COPY put: ") + PQerrorMessage(conn));
      }
      buf.clear();
      if (sent % (o.batch_size * 16) == 0) {
        std::cout << "  inserted " << sent << " / " << o.rows << std::endl;
      }
    }
  }
  if (!buf.empty()) {
    if (PQputCopyData(conn, buf.data(), static_cast<int>(buf.size())) != 1) {
      Die(std::string("COPY put: ") + PQerrorMessage(conn));
    }
  }
  if (PQputCopyEnd(conn, nullptr) != 1) {
    Die(std::string("COPY end: ") + PQerrorMessage(conn));
  }
  PGresult* end_res = PQgetResult(conn);
  Check(conn, end_res, "COPY complete");
  PQclear(end_res);
  const auto t1 = std::chrono::steady_clock::now();
  const double sec = std::chrono::duration<double>(t1 - t0).count();
  std::cout << "inserted " << o.rows << " rows in " << std::fixed << std::setprecision(3) << sec
            << " s (" << (sec > 0 ? o.rows / sec : 0.0) << " rows/s)\n";

  Exec(conn, "ANALYZE " + qtable, "ANALYZE");
  std::cout << "schema ready: " << o.table << "  tablets=" << o.tablets
            << "  subkeys=" << o.subkeys << "  rows=" << o.rows << std::endl;
}

std::string BuildInQuery(const Options& o, std::mt19937_64& rng) {
  const int n_miss = static_cast<int>(o.in_list_size * o.miss_rate + 0.5);
  int n_hit = o.in_list_size - n_miss;
  if (static_cast<uint64_t>(n_hit) > o.rows) {
    n_hit = static_cast<int>(o.rows);
  }

  std::unordered_set<uint64_t> seen;
  seen.reserve(static_cast<size_t>(o.in_list_size) * 2);
  std::vector<std::vector<std::string>> vals;
  vals.reserve(static_cast<size_t>(o.in_list_size));

  std::uniform_int_distribution<uint64_t> hit_dist(0, o.rows - 1);
  while (static_cast<int>(vals.size()) < n_hit) {
    const uint64_t idx = hit_dist(rng);
    if (seen.insert(idx).second) {
      vals.push_back(MakeRowKeys(idx, o));
    }
  }
  // Misses sit outside the inserted index space.
  uint64_t miss_base = o.rows;
  while (static_cast<int>(vals.size()) < o.in_list_size) {
    const uint64_t idx = miss_base++;
    vals.push_back(MakeRowKeys(idx, o));
  }
  std::shuffle(vals.begin(), vals.end(), rng);

  const std::string cols = HashColsSql(o);
  std::ostringstream sql;
  sql << "SELECT " << cols << " FROM " << QuoteIdent(o.table) << " WHERE ";
  if (o.independent_in && o.subkeys > 1) {
    for (int j = 0; j < o.subkeys; ++j) {
      if (j) sql << " AND ";
      sql << QuoteIdent(ColName(j, o.subkeys)) << " IN (";
      for (size_t i = 0; i < vals.size(); ++i) {
        if (i) sql << ", ";
        sql << QuoteLiteral(vals[i][static_cast<size_t>(j)]);
      }
      sql << ")";
    }
    return sql.str();
  }
  const std::string in_lhs = o.subkeys == 1 ? cols : ("(" + cols + ")");
  sql << in_lhs << " IN (";
  for (size_t i = 0; i < vals.size(); ++i) {
    if (i) sql << ", ";
    sql << FormatInValue(vals[i]);
  }
  sql << ")";
  return sql.str();
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

void PrintInStyle(const Options& o) {
  if (o.independent_in && o.subkeys > 1) {
    std::cout << "in_style=independent AND  subkeys=" << o.subkeys
              << "  in_list_size=" << o.in_list_size << "  permutations="
              << o.in_list_size << "^" << o.subkeys << std::endl;
  } else if (o.independent_in) {
    std::cout << "in_style=independent AND (same as single-column IN)  subkeys=1"
              << "  in_list_size=" << o.in_list_size << std::endl;
  } else {
    std::cout << "in_style=row IN  subkeys=" << o.subkeys
              << "  in_list_size=" << o.in_list_size << std::endl;
  }
}

bool TakePrefixedLong(const std::string& line, const char* key, std::optional<long>& dest) {
  if (dest) return false;
  const std::string needle = std::string(key) + ":";
  const auto pos = line.find(needle);
  if (pos == std::string::npos) return false;
  const char* p = line.c_str() + pos + needle.size();
  while (*p == ' ' || *p == '\t') {
    ++p;
  }
  char* end = nullptr;
  const long v = std::strtol(p, &end, 10);
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
  while (*p == ' ' || *p == '\t') {
    ++p;
  }
  char* end = nullptr;
  const double v = std::strtod(p, &end);
  if (end == p) return false;
  dest = v;
  return true;
}

struct DistMetrics {
  std::optional<long> table_read_requests;
  std::optional<long> read_requests;
  std::optional<long> table_read_ops;
  std::optional<long> read_ops;
  std::optional<long> table_rows_scanned;
  std::optional<long> rows_scanned;
  std::optional<double> exec_ms;

  long Requests() const {
    if (table_read_requests) return *table_read_requests;
    if (read_requests) return *read_requests;
    return -1;
  }
  long Ops() const {
    if (table_read_ops) return *table_read_ops;
    if (read_ops) return *read_ops;
    return -1;
  }
  long RowsScanned() const {
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

std::string FmtMetric(long v) { return v < 0 ? std::string("-") : std::to_string(v); }

void PrintPlan(PGresult* res) {
  const int n = PQntuples(res);
  for (int r = 0; r < n; ++r) {
    std::cout << PQgetvalue(res, r, 0) << "\n";
  }
}

void RunQuery(PGconn* conn, const Options& o) {
  PrintInStyle(o);
  std::mt19937_64 rng(o.seed ^ 0x9e3779b97f4a7c15ULL);
  std::vector<double> lat_ms;
  lat_ms.reserve(static_cast<size_t>(o.iterations));

  const int total = o.warmup + o.iterations;
  for (int i = 0; i < total; ++i) {
    const std::string sql = BuildInQuery(o, rng);
    if (i == 0) {
      const std::string preview =
          sql.size() > 240 ? sql.substr(0, 240) + " ..." : sql;
      std::cout << "sample SQL: " << preview << "\n";
    }

    const auto t0 = std::chrono::steady_clock::now();
    PGresult* res = PQexec(conn, sql.c_str());
    const auto t1 = std::chrono::steady_clock::now();
    Check(conn, res, "SELECT");
    const int ntuples = PQntuples(res);
    PQclear(res);

    const double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    const bool timed = i >= o.warmup;
    if (timed) {
      lat_ms.push_back(ms);
    }
    std::cout << (timed ? "query " : "warmup ") << (timed ? (i - o.warmup + 1) : (i + 1))
              << std::fixed << std::setprecision(3) << "  " << ms << " ms  rows=" << ntuples
              << std::endl;
  }

  const double sum = std::accumulate(lat_ms.begin(), lat_ms.end(), 0.0);
  const double avg = sum / static_cast<double>(lat_ms.size());
  std::cout << std::fixed << std::setprecision(3)
            << "latency_ms  n=" << lat_ms.size() << "  min=" << *std::min_element(lat_ms.begin(), lat_ms.end())
            << "  p50=" << Percentile(lat_ms, 0.50) << "  p95=" << Percentile(lat_ms, 0.95)
            << "  p99=" << Percentile(lat_ms, 0.99) << "  avg=" << avg
            << "  max=" << *std::max_element(lat_ms.begin(), lat_ms.end()) << std::endl;
}

void RunExplain(PGconn* conn, const Options& o) {
  PrintInStyle(o);
  std::mt19937_64 rng(o.seed ^ 0x9e3779b97f4a7c15ULL);
  std::vector<double> requests;
  std::vector<double> ops;
  std::vector<double> exec_ms;
  requests.reserve(static_cast<size_t>(o.iterations));
  ops.reserve(static_cast<size_t>(o.iterations));
  exec_ms.reserve(static_cast<size_t>(o.iterations));

  bool printed_plan = false;
  const int total = o.warmup + o.iterations;
  for (int i = 0; i < total; ++i) {
    const std::string sql = BuildInQuery(o, rng);
    if (i == 0) {
      const std::string preview =
          sql.size() > 240 ? sql.substr(0, 240) + " ..." : sql;
      std::cout << "sample SQL: " << preview << "\n";
    }
    const std::string explain_sql = "EXPLAIN (" + o.explain_options + ") " + sql;

    const auto t0 = std::chrono::steady_clock::now();
    PGresult* res = PQexec(conn, explain_sql.c_str());
    const auto t1 = std::chrono::steady_clock::now();
    Check(conn, res, "EXPLAIN");
    const DistMetrics m = ParseDistMetrics(res);
    const bool timed = i >= o.warmup;
    if (timed && !printed_plan) {
      std::cout << "plan:\n";
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
              << "  requests=" << FmtMetric(m.Requests())
              << "  ops=" << FmtMetric(m.Ops())
              << "  rows_scanned=" << FmtMetric(m.RowsScanned())
              << "  exec_ms=";
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
            << " user=" << PQuser(conn) << " ssl=" << (PQsslInUse(conn) ? "yes" : "no") << std::endl;
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
