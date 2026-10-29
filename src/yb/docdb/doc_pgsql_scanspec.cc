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

#include "yb/docdb/doc_pgsql_scanspec.h"

#include <algorithm>
#include <limits>

#include "yb/common/pgsql_protocol.messages.h"
#include "yb/common/ql_value.h"
#include "yb/common/schema.h"

#include "yb/dockv/doc_key.h"
#include "yb/dockv/value_type.h"

#include "yb/qlexpr/doc_scanspec_util.h"

#include "yb/util/logging.h"
#include "yb/util/status_format.h"
#include "yb/util/yb_partition.h"

namespace yb::docdb {

using dockv::DocKey;
using dockv::KeyBytes;
using dockv::KeyEntryType;
using dockv::KeyEntryValue;

namespace {

const dockv::KeyEntryValues kEmptyKeyComponents;

// Cap on the encoded keys one slice holds at once.
constexpr size_t kHashColumnListSliceCap = static_cast<size_t>(YBPartition::kMaxHashCode) + 1;

}  // namespace

DocPgsqlScanSpec::DocPgsqlScanSpec(
    const Schema& schema,
    const rocksdb::QueryId query_id,
    const DocKey& doc_key,
    const std::optional<int32_t> hash_code,
    const std::optional<int32_t> max_hash_code,
    const DocKey& start_doc_key,
    bool is_forward_scan,
    const size_t prefix_length)
    : YQLScanSpec(
          YQL_CLIENT_PGSQL, schema, is_forward_scan, query_id, /* range_bounds = */ nullptr,
          prefix_length, SharedSmallArena()),
      range_components_(nullptr),
      options_groups_(schema.num_dockey_components()),
      hash_code_(hash_code),
      max_hash_code_(max_hash_code),
      start_doc_key_(start_doc_key.empty() ? KeyBytes() : start_doc_key.Encode()) {
  bounds_.lower = doc_key.Encode();
  // Compute lower and upper doc_key.
  // We add +inf as an extra component to make sure this is greater than all keys in range.
  // For lower bound, this is true already, because dockey + suffix is > dockey
  bounds_.upper = bounds_.lower;

  if (hash_code && !doc_key.has_hash()) {
    DocKey lower_doc_key = DocKey(doc_key);
    lower_doc_key.set_hash(*hash_code);
    if (lower_doc_key.hashed_group().empty()) {
      lower_doc_key.hashed_group().emplace_back(KeyEntryType::kLowest);
    }
    bounds_.lower = lower_doc_key.Encode();
  }

  if (max_hash_code) {
    DocKey upper_doc_key = DocKey(doc_key);
    upper_doc_key.set_hash(*max_hash_code);
    if (upper_doc_key.hashed_group().empty()) {
      upper_doc_key.hashed_group().emplace_back(KeyEntryType::kHighest);
    }
    bounds_.upper = upper_doc_key.Encode();
  }

  bounds_.upper.AppendKeyEntryTypeBeforeGroupEnd(KeyEntryType::kHighest);

  CompleteBounds();
}

DocPgsqlScanSpec::DocPgsqlScanSpec(
    const Schema& schema,
    const rocksdb::QueryId query_id,
    const ArenaPtr& arena,
    const std::vector<Slice>& encoded_hashed_components,
    std::reference_wrapper<const dockv::KeyEntryValues> range_components,
    PgsqlConditionPBPtr condition,
    const std::optional<int32_t> hash_code,
    const std::optional<int32_t> max_hash_code,
    const DocKey& start_doc_key,
    bool is_forward_scan,
    const DocKey& lower_doc_key,
    const DocKey& upper_doc_key,
    const size_t prefix_length,
    PgsqlHashColumnListsPBPtr column_lists,
    uint64_t column_list_begin,
    std::optional<uint64_t> column_list_end,
    size_t column_list_key_budget)
    : YQLScanSpec(
          YQL_CLIENT_PGSQL, schema, is_forward_scan, query_id,
          qlexpr::QLScanRange::Create(schema, condition),
          prefix_length, arena ? arena : SharedSmallArena()),
      range_components_(&range_components.get()),
      options_groups_(schema.num_dockey_components()),
      hash_code_(hash_code),
      max_hash_code_(max_hash_code),
      start_doc_key_(start_doc_key.empty() ? KeyBytes() : start_doc_key.Encode()) {
  bounds_.lower = lower_doc_key.Encode();
  bounds_.upper = upper_doc_key.Encode();

  if (!encoded_hashed_components.empty() && schema.num_hash_key_columns() > 0) {
    options_ = std::make_shared<std::vector<qlexpr::OptionList>>(schema.num_dockey_components());
    options_col_ids_.reserve(schema.num_dockey_components());

    // should come here if we are not batching hash keys as a part of IN condition
    options_groups_.BeginNewGroup();

    // dockeys contains elements in the format yb_hash_code, hk1, hk2, ... hkn followed by
    // rk1, rk2... rkn etc. As yb_hash_code is the first element and is not part of the schema
    // we add it manually.
    options_groups_.AddToLatestGroup(0);
    options_col_ids_.emplace_back(ColumnId(kYbHashCodeColId));

    (*options_)[0].push_back(dockv::EncodedHashCode(*arena, hash_code_.value()));
    DCHECK_EQ(encoded_hashed_components.size(), schema.num_hash_key_columns());
    for (size_t col_idx = 0; col_idx < schema.num_hash_key_columns(); ++col_idx) {
      // Adding 1 to col_idx to account for hash_code column
      options_groups_.AddToLatestGroup(schema.get_dockey_component_idx(col_idx));
      options_col_ids_.emplace_back(schema.column_id(col_idx));

      (*options_)[schema.get_dockey_component_idx(col_idx)].push_back(
          encoded_hashed_components[col_idx]);
    }
  }

  // Per-column hash-key value lists to be expanded by DocDB into the hash option group. This is
  // mutually exclusive with encoded_hashed_components (a fixed hash key) being set.
  if (column_lists != nullptr) {
    DCHECK(encoded_hashed_components.empty());
    if (options_ == nullptr) {
      options_ = std::make_shared<std::vector<qlexpr::OptionList>>(schema.num_dockey_components());
      options_col_ids_.reserve(schema.num_dockey_components());
    }
    if (column_lists.is_lightweight()) {
      InitHashColumnLists(
          *column_lists.lightweight(), column_list_begin, column_list_end, column_list_key_budget);
    } else {
      InitHashColumnLists(
          *column_lists.protobuf(), column_list_begin, column_list_end, column_list_key_budget);
    }
  }

  // We have hash or range columns with IN condition, try to construct the exact list of options to
  // scan for.
  const auto rangebounds = range_bounds();
  if (rangebounds &&
      (rangebounds->has_in_range_options() || rangebounds->has_in_hash_options())) {
    if (options_ == nullptr) {
      options_ = std::make_shared<std::vector<qlexpr::OptionList>>(schema.num_dockey_components());
    }
    DCHECK(condition);
    if (condition.is_lightweight()) {
      InitOptions(*condition.lightweight());
    } else {
      InitOptions(*condition.protobuf());
    }
  }

  auto calculated_bounds = CalculateBounds(encoded_hashed_components, schema);
  if (lower_doc_key.empty() || calculated_bounds.lower > bounds_.lower) {
    bounds_.lower = std::move(calculated_bounds.lower);
  }

  if (upper_doc_key.empty() || calculated_bounds.upper < bounds_.upper) {
    bounds_.upper = std::move(calculated_bounds.upper);
  }
  bounds_.trivial = calculated_bounds.trivial;

  CompleteBounds();
}

DocPgsqlScanSpec::DocPgsqlScanSpec(const Schema& schema, const PgsqlConditionPB* condition)
    : DocPgsqlScanSpec(schema, rocksdb::kDefaultQueryId, nullptr, {}, kEmptyKeyComponents,
                       PgsqlConditionPBPtr(condition), std::nullopt, std::nullopt) {}

template <class ConditionPB>
void DocPgsqlScanSpec::InitOptions(const ConditionPB& condition) {
  switch (condition.op()) {
    case QLOperator::QL_OP_AND:
      for (const auto& operand : condition.operands()) {
        DCHECK(operand.has_condition());
        InitOptions(operand.condition());
      }
      break;

    case QLOperator::QL_OP_EQUAL:
    case QLOperator::QL_OP_IN: {
      DCHECK_EQ(condition.operands_size(), 2);
      // Skip any condition where LHS is not a column (e.g. subscript columns: 'map[k] = v')
      // operands(0) always contains the column id.
      // operands(1) contains the corresponding value or a list values.
      auto it = condition.operands().begin();
      const auto& lhs = *it;
      const auto& rhs = *++it;
      if (lhs.expr_case() != PgsqlExpressionPB::kColumnId &&
          lhs.expr_case() != PgsqlExpressionPB::kTuple) {
        return;
      }

      // Skip any RHS expressions that are not evaluated yet.
      if (rhs.expr_case() != PgsqlExpressionPB::kValue &&
          rhs.expr_case() != PgsqlExpressionPB::kTuple) {
        return;
      }

      DCHECK(condition.op() == QL_OP_IN ||
             condition.op() == QL_OP_EQUAL); // move this up
      if (lhs.has_column_id()) {

        auto col_id = ColumnId(lhs.column_id());
        auto col_idx = schema().find_column_by_id(col_id);

        // Skip any non-range columns.
        if (!schema().is_range_column(col_idx)) {
          // Hashed columns should always be sent as tuples along with their yb_hash_code.
          // Hence, for hashed columns lhs should never be a column id.
          YB_LOG_EVERY_N_SECS_OR_VLOG(DFATAL, 60, 1)
              << "Expected only range column: id=" << col_id << " idx=" << col_idx;
          return;
        }

        auto sorting_type = get_sorting_type(col_idx);

        // Adding the offset if yb_hash_code is present after schema usages. Schema does not know
        // about yb_hash_code_column
        auto key_idx = schema().get_dockey_component_idx(col_idx);

        options_col_ids_.emplace_back(col_id);
        options_groups_.BeginNewGroup();
        options_groups_.AddToLatestGroup(key_idx);

        if (condition.op() == QL_OP_EQUAL) {
          auto op_it = condition.operands().begin();
          (*options_)[key_idx].push_back(dockv::EncodedKeyEntryValue(
              arena(), (++op_it)->value(), sorting_type));
        } else { // QL_OP_IN
          DCHECK_EQ(condition.op(), QL_OP_IN);
          DCHECK(rhs.value().has_list_value());
          const auto &options = rhs.value().list_value();
          size_t opt_size = options.elems().size();
          (*options_)[key_idx].reserve(opt_size);

          // IN arguments should have been de-duplicated and ordered ascendingly by the executor.
          bool is_reverse_order = get_scan_direction(col_idx);
          auto elem_it = is_reverse_order ? --options.elems().end() : options.elems().begin();
          for (size_t i = 0; i < opt_size; i++) {
            (*options_)[key_idx].push_back(dockv::EncodedKeyEntryValue(
                arena(), *elem_it, sorting_type));
            if (is_reverse_order) {
              --elem_it;
            } else {
              ++elem_it;
            }
          }
        }
      } else if (lhs.has_tuple()) {
        size_t total_cols = lhs.tuple().elems_size();
        DCHECK_GT(total_cols, 0);

        // Whenever you have a tuple as a part of IN array, the query is of two types:
        // 1. Range tuples SELECT * FROM table where (r1, r2) IN ((1, 1), (2, 2), (3,3));
        // 2. Hash tuples
        //    a. SELECT * FROM table where (h1, h2) IN ((1, 1), (2, 2), (3,3));
        //    b. SELECT * FROM table where h1 IN (1, 2, 3, 4) AND h2 IN (5, 6, 7, 8);
        // 3. Hash and range mix.
        // The hash columns in the lhs are always expected to appear to the left of all the
        // range columns. We only take care to add the range components of the lhs to
        // options_groups_ and options_col_ids_.
        // In each of these situations, the following steps have to be undertaken
        //
        // Step 1: Get the column ids of the elements
        // For range tuples its (r1, r2).
        // For hash tuples its (yb_hash_code, h1, h2), (yb_hash_code, h3, h4)
        // Push them into the options groups and options indexes as hybrid scan utilizes to match
        // target elements with their corresponding columns.
        int start_range_col_idx = 0;
        qlexpr::ColumnListVector col_idxs;
        options_groups_.BeginNewGroup();

        for (const auto& elem : lhs.tuple().elems()) {
          DCHECK(elem.has_column_id());
          ColumnId col_id = ColumnId(elem.column_id());
          auto col_idx = elem.column_id() == kYbHashCodeColId ? kYbHashCodeColId
              : schema().find_column_by_id(col_id);
          col_idxs.push_back(col_idx);
          if (!schema().is_range_column(col_idx)) {
            start_range_col_idx++;
          }
          options_col_ids_.emplace_back(col_id);
          // yb_hash_code takes the 0th group. If there exists a yb_hash_code column, then we offset
          // other columns by one position from what schema().find_column_by_id(col_id) provides us
          options_groups_.AddToLatestGroup(schema().get_dockey_component_idx(col_idx));
        }

        if (condition.op() == QL_OP_EQUAL) {
          DCHECK(rhs.value().has_list_value());
          const auto& value = rhs.value().list_value();
          DCHECK_EQ(total_cols, value.elems_size());
          auto elem_it = std::next(value.elems().begin(), start_range_col_idx);
          for (size_t i = start_range_col_idx; i < total_cols; i++) {
            // hash codes are always sorted ascending.
            auto options_idx = schema().get_dockey_component_idx(col_idxs[i]);
            (*options_)[options_idx].push_back(dockv::EncodedKeyEntryValue(
                arena(), *elem_it, get_sorting_type(col_idxs[i])));
          }
        } else if (condition.op() == QL_OP_IN) {
          // There should be no range columns before start_range_col_idx in col_idxs
          // and there should be no hash columns after start_range_col_idx
          DCHECK(std::find_if(col_idxs.begin(), col_idxs.begin() + start_range_col_idx,
                              [this] (int idx) { return schema().is_range_column(idx); })
                 == (col_idxs.begin() + start_range_col_idx));
          DCHECK(std::find_if(col_idxs.begin() + start_range_col_idx, col_idxs.end(),
                              [this] (int idx) { return schema().is_hash_key_column(idx); })
                 == (col_idxs.end()));

          // Obtain the list of tuples that contain the target values.
          DCHECK(rhs.value().has_list_value());
          const auto& options = rhs.value().list_value();

          // IN arguments should have been de-duplicated and ordered ascendingly by the executor.
          // For range columns, yb_scan sorts them according to the range key values at the pggate
          // layer itself. For hash key columns, we need to sort the options based on the
          // yb_hash_code value. This enables the docDB iterator to pursue a one pass scan on the
          // list of hash key columns
          //
          // Step 2: Obtain the sorting order for elements. For hash key columns its always
          // SortingType::kAscending based on the yb_hash_code, and then the individual hash key
          // components subsequently. For range key columns, we try to obtain it from the column
          // structure.
          std::vector<bool> reverse;
          reverse.reserve(total_cols);
          for (size_t i = 0; i < total_cols; i++) {
            reverse.push_back(get_scan_direction(col_idxs[i]));
          }

          const auto sorted_options = qlexpr::GetTuplesSortedByOrdering(
              options, schema(), is_forward_scan(), col_idxs);

          // Step 3: Add the sorted options into the options_ vector for HybridScan to use them to
          // perform seeks and nexts.
          // options_ array indexes into every key column. Here we append to every key column the
          // list of target elements that needs to be scanned.
          size_t num_options = options.elems().size();
          for (size_t i = 0; i < num_options; i++) {
            const auto& elem = sorted_options[i];
            DCHECK(elem->has_tuple_value());
            const auto& value = elem->tuple_value();
            DCHECK_EQ(total_cols, value.elems_size());

            auto elem_it = value.elems().begin();
            for (size_t j = 0; j < total_cols; ++j, ++elem_it) {
              const auto sorting_type = get_sorting_type(col_idxs[j]);

              // For hash tuples, the first element always contains the yb_hash_code
              auto options_idx = schema().get_dockey_component_idx(col_idxs[j]);
              auto value_slice = j == 0 && col_idxs[j] == kYbHashCodeColId
                  ? dockv::EncodedHashCode(arena(), elem_it->int32_value())
                  : dockv::EncodedKeyEntryValue(arena(), *elem_it, sorting_type);
              (*options_)[options_idx].push_back(value_slice);
            }
          }
        }
      }
      break;
    }

    default:
      // We don't support any other operators at this level.
      break;
  }
}

template <class HashColumnListsPB>
void DocPgsqlScanSpec::InitHashColumnLists(
    const HashColumnListsPB& column_lists,
    uint64_t column_list_begin,
    std::optional<uint64_t> column_list_end,
    size_t column_list_key_budget) {
  const size_t num_hash = schema().num_hash_key_columns();
  if (num_hash == 0) {
    YB_LOG_EVERY_N_SECS_OR_VLOG(DFATAL, 60, 1)
        << "column_lists set on a table without hash key columns";
    return;
  }

  // Pointer-to-value type (const QLValuePB* or const LWQLValuePB*) deduced from list elems.
  using ValuePtr = decltype(&*column_lists.column_list().begin()->list_value().elems().begin());

  // Collect the candidate values for each hash key column, in hash-key order. Each column_list
  // entry is a list_value (equality is a single-element list).
  std::vector<std::vector<ValuePtr>> col_values;
  col_values.reserve(num_hash);
  for (const auto& list_val : column_lists.column_list()) {
    std::vector<ValuePtr> values;
    for (const auto& elem : list_val.list_value().elems()) {
      values.push_back(&elem);
    }
    col_values.push_back(std::move(values));
  }

  if (col_values.size() != num_hash) {
    YB_LOG_EVERY_N_SECS_OR_VLOG(DFATAL, 60, 1)
        << "column_lists has " << col_values.size() << " columns, expected " << num_hash;
    hash_column_lists_empty_ = true;
    return;
  }

  // This tablet's inclusive hash range. Permutations whose yb_hash_code lands outside this range
  // belong to other tablets and are filtered out here.
  const uint16_t lo = hash_code_ ? static_cast<uint16_t>(*hash_code_) : YBPartition::kMinHashCode;
  const uint16_t hi =
      max_hash_code_ ? static_cast<uint16_t>(*max_hash_code_) : YBPartition::kMaxHashCode;

  // One encoded permutation: encoded[0] is yb_hash_code, encoded[1 + c] is the c-th hash column.
  struct EncodedPermutation {
    std::vector<Slice> encoded;
  };
  std::vector<EncodedPermutation> permutations;

  bool any_empty = false;
  for (const auto& values : col_values) {
    any_empty = any_empty || values.empty();
  }
  // An empty per-column list is a malformed request. A tablet whose range simply contains none of
  // the permutations also ends with an empty expansion, and that scan returns no rows.
  if (any_empty) {
    YB_LOG_EVERY_N_SECS_OR_VLOG(DFATAL, 60, 1) << "column_lists has an empty hash column list";
    hash_column_lists_empty_ = true;
    return;
  }

  // Linear index of the cartesian product. The last column is the fast digit, so consecutive
  // indexes tile the lists: the next slice starts where this one ended.
  std::vector<uint64_t> sizes;
  sizes.reserve(num_hash);
  uint64_t product = 1;
  for (const auto& values : col_values) {
    const auto size = static_cast<uint64_t>(values.size());
    if (product > std::numeric_limits<uint64_t>::max() / size) {
      YB_LOG_EVERY_N_SECS_OR_VLOG(DFATAL, 60, 1)
          << "hash column list product does not fit in uint64";
      hash_column_lists_empty_ = true;
      return;
    }
    product *= size;
    sizes.push_back(size);
  }

  hash_column_list_cursor_.begin = column_list_begin;
  if (column_list_begin > product ||
      (column_list_end.has_value() && *column_list_end < column_list_begin)) {
    YB_LOG_EVERY_N_SECS_OR_VLOG(DFATAL, 60, 1)
        << "hash column list slice [" << column_list_begin << ", "
        << column_list_end.value_or(product) << ") is outside [0, " << product << ")";
    hash_column_list_cursor_.end = column_list_begin;
    hash_column_list_cursor_.exhausted = true;
    hash_column_lists_empty_ = true;
    return;
  }

  // A fixed end is the slice that produced next_row_key. Growing it would add keys that sort
  // before that row and the seek would skip them. An open end runs until `budget` keys fall in
  // this tablet; keys for other tablets do not consume the budget.
  const bool grow = !column_list_end.has_value();
  const uint64_t limit_end = std::min(column_list_end.value_or(product), product);
  size_t budget = column_list_key_budget;
  if (budget == 0 || budget > kHashColumnListSliceCap) {
    budget = kHashColumnListSliceCap;
  }

  std::vector<size_t> pos(num_hash);
  std::string compound;
  size_t in_range = 0;
  uint64_t index = column_list_begin;
  while (index < limit_end) {
    uint64_t rest = index;
    for (size_t c = num_hash; c-- > 0; ) {
      pos[c] = static_cast<size_t>(rest % sizes[c]);
      rest /= sizes[c];
    }

    // AppendToKey mirrors how the partition schema (and pggate) builds the hash compound value.
    compound.clear();
    for (size_t c = 0; c < num_hash; ++c) {
      AppendToKey(*col_values[c][pos[c]], &compound);
    }
    const uint16_t hash_code = YBPartition::HashColumnCompoundValue(compound);
    ++index;
    if (hash_code < lo || hash_code > hi) {
      continue;
    }

    EncodedPermutation permutation;
    permutation.encoded.reserve(num_hash + 1);
    permutation.encoded.push_back(dockv::EncodedHashCode(arena(), hash_code));
    for (size_t c = 0; c < num_hash; ++c) {
      permutation.encoded.push_back(dockv::EncodedKeyEntryValue(
          arena(), *col_values[c][pos[c]], get_sorting_type(c)));
    }
    permutations.push_back(std::move(permutation));
    if (grow && ++in_range == budget) {
      break;
    }
  }

  hash_column_list_cursor_.end = index;
  hash_column_list_cursor_.exhausted = index >= product;

  // Sort permutations into scan order. The encoded slices are order-preserving (yb_hash_code is
  // big-endian; hash columns use their key sorting type), so lexicographic comparison of the
  // encoded components yields key order; flip the sense for a reverse scan. This matches the
  // ordering produced by the legacy ROW(yb_hash_code, ...) IN tuple path.
  const bool forward = is_forward_scan();
  std::sort(permutations.begin(), permutations.end(),
            [forward](const EncodedPermutation& a, const EncodedPermutation& b) {
    for (size_t i = 0; i < a.encoded.size(); ++i) {
      const int cmp = a.encoded[i].compare(b.encoded[i]);
      if (cmp != 0) {
        return forward ? cmp < 0 : cmp > 0;
      }
    }
    return false;
  });

  // No permutation hashed into this tablet. Registering an empty option group makes
  // CalculateBounds treat the scan as unconstrained.
  if (permutations.empty()) {
    hash_column_lists_empty_ = true;
    return;
  }

  // Register the hash option group: yb_hash_code followed by the hash key columns, all correlated
  // in a single group (each permutation is one option across these columns).
  options_groups_.BeginNewGroup();
  options_groups_.AddToLatestGroup(0);
  options_col_ids_.emplace_back(ColumnId(kYbHashCodeColId));
  for (size_t c = 0; c < num_hash; ++c) {
    options_groups_.AddToLatestGroup(schema().get_dockey_component_idx(c));
    options_col_ids_.emplace_back(schema().column_id(c));
  }

  for (const auto& permutation : permutations) {
    (*options_)[0].push_back(permutation.encoded[0]);
    for (size_t c = 0; c < num_hash; ++c) {
      (*options_)[schema().get_dockey_component_idx(c)].push_back(permutation.encoded[c + 1]);
    }
  }
}

qlexpr::ScanBounds DocPgsqlScanSpec::CalculateBounds(
    const std::vector<Slice>& encoded_hashed_components, const Schema& schema) const {
  if (hash_column_lists_empty_) {
    // No permutation falls in this tablet. Lower is +inf so a backward scan is already past every
    // row. The forward scan uses the upper bound as its iterator bound, which must start with the
    // table prefix (cotable or colocation id, then the hash-code marker). A bare kLowest fails
    // that check. The upper bound carries a full hash code because bloom filter setup decodes it
    // as a doc key. Every row key extends prefix + hash code 0, so the exclusive upper bound
    // excludes them all.
    qlexpr::ScanBounds bounds;
    bounds.lower.AppendKeyEntryType(KeyEntryType::kHighest);
    dockv::DocKeyEncoder(&bounds.upper).Schema(schema);
    AppendHash(0, &bounds.upper);
    return bounds;
  }

  bool has_hash_columns = schema.num_hash_key_columns() > 0;

  // The first column in a hash partitioned table is the hash code column.
  bool has_in_hash_options = has_hash_columns && options_ && !options_->empty()
      && !(*options_)[schema.get_dockey_component_idx(0)].empty();
  std::vector<Slice> hashed_components;
  hashed_components.reserve(schema.num_hash_key_columns());

  Slice hash_code;
  Slice max_hash_code;
  auto append_hashed_component = encoded_hashed_components.empty() && has_in_hash_options;

  dockv::KeyBytes key_buffer;

  if (append_hashed_component) {
    DCHECK_GE(options_->size(),
              schema.num_hash_key_columns() + schema.has_yb_hash_code());
    hash_code = (*options_)[0].front();
    max_hash_code = (*options_)[0].back();
  } else if (has_hash_columns) {
    AppendHash(hash_code_.value_or(std::numeric_limits<DocKeyHash>::min()), &key_buffer);
    auto hash_code_size = key_buffer.size();
    AppendHash(max_hash_code_.value_or(std::numeric_limits<DocKeyHash>::max()), &key_buffer);
    auto key_buffer_slice = key_buffer.AsSlice();
    // hash_code is encoded hash_code_ or min possible hash code.
    hash_code = key_buffer_slice.Prefix(hash_code_size);
    // max_hash_code is encoded max_hash_code_ or max possible hash code.
    max_hash_code = key_buffer_slice.WithoutPrefix(hash_code_size);
    for (const auto& component : encoded_hashed_components) {
      hashed_components.push_back(component);
    }
  }

  VLOG_WITH_FUNC(4)
      << "hash_code: " << hash_code.ToDebugHexString()
      << ", max_hash_code: " << max_hash_code.ToDebugHexString()
      << ", append_hashed_component: " << append_hashed_component
      << ", has_hash_columns: " << has_hash_columns;

  qlexpr::ScanBounds bounds;
  auto lower_bound_encoder = dockv::DocKeyEncoder(&bounds.lower).Schema(schema);
  auto upper_bound_encoder = dockv::DocKeyEncoder(&bounds.upper).Schema(schema);

  bool hash_components_unset =
      has_hash_columns && encoded_hashed_components.empty() && !append_hashed_component;
  if (hash_components_unset) {
    // use lower bound hash code if set in request (for scans using token)
    if (hash_code_) {
      bounds.lower.AppendRawBytes(hash_code);
      bounds.lower.AppendKeyEntryType(KeyEntryType::kLowest);
    }

    // use upper bound hash code if set in request (for scans using token)
    if (max_hash_code_) {
      bounds.upper.AppendRawBytes(max_hash_code);
    }
    bounds.upper.AppendKeyEntryType(KeyEntryType::kHighest);

    VLOG_WITH_FUNC(4) << bounds.ToString();

    return bounds;
  }

  bool single_hash = false;
  bool lower_trivial = false;
  bool upper_trivial = false;
  if (has_hash_columns) {
    single_hash = hash_code == max_hash_code;
    if (append_hashed_component) {
      for (size_t i = 0; i < schema.num_hash_key_columns(); ++i) {
        const auto& option = (*options_)[schema.get_dockey_component_idx(i)];
        hashed_components.push_back(option.front());
        single_hash = single_hash && (option.front() == option.back());
      }
    }
    lower_bound_encoder.
        Hash(hash_code, hashed_components).
        Range(DoRangeComponents(qlexpr::BoundType::kLower, nullptr, &lower_trivial));

    if (append_hashed_component) {
      hashed_components.clear();
      for (size_t i = 0; i < schema.num_hash_key_columns(); ++i) {
        hashed_components.push_back((*options_)[schema.get_dockey_component_idx(i)].back());
      }
    }
    upper_bound_encoder.
        Hash(max_hash_code, hashed_components).
        Range(DoRangeComponents(qlexpr::BoundType::kUpper, nullptr, &upper_trivial));
  } else {
    VLOG_WITH_FUNC(4) << "range_components: " << AsString(range_components_);
    single_hash = true;
    lower_bound_encoder.NoHash().Range(DoRangeComponents(
        qlexpr::BoundType::kLower, nullptr, &lower_trivial));
    upper_bound_encoder.NoHash().Range(DoRangeComponents(
        qlexpr::BoundType::kUpper, nullptr, &upper_trivial));
  }
  bounds.trivial = single_hash && lower_trivial && upper_trivial;

  VLOG_WITH_FUNC(4) << bounds.ToString();

  return bounds;
}

dockv::KeyEntryValues DocPgsqlScanSpec::RangeComponents(qlexpr::BoundType bound_type,
                                                        std::vector<bool>* inclusivities) const {
  return DoRangeComponents(bound_type, inclusivities);
}

dockv::KeyEntryValues DocPgsqlScanSpec::DoRangeComponents(
    qlexpr::BoundType bound_type, std::vector<bool>* inclusivities, bool* trivial) const {
  return GetRangeKeyScanSpec(schema(),
                             range_components_,
                             range_bounds(),
                             inclusivities,
                             bound_type,
                             false,
                             trivial);
}

void DocPgsqlScanSpec::CompleteBounds() {
  if (start_doc_key_.empty()) {
    return;
  }

  // When paging state is present, start_doc_key_ should have been provided, and the scan starting
  // point should be start_doc_key_ instead of the initial bounds.
  if (start_doc_key_ < bounds_.lower || start_doc_key_ > bounds_.upper) {
    LOG(DFATAL) << STATUS_FORMAT(Corruption, "Invalid start_doc_key: $0. Range: $1, $2",
                                 start_doc_key_, bounds_.lower, bounds_.upper);
    return;
  }

  // Paging state + forward scan.
  if (is_forward_scan()) {
    bounds_.lower = start_doc_key_;
    return;
  }

  // Paging state + reverse scan.
  // If using start_doc_key_ as upper bound append +inf as extra component to ensure it includes
  // the target start_doc_key itself (dockey + suffix < dockey + kHighest).
  // For lower bound, this is true already, because dockey + suffix is > dockey.
  bounds_.upper = start_doc_key_;
  bounds_.upper.AppendKeyEntryTypeBeforeGroupEnd(KeyEntryType::kHighest);
}

const DocKey& DocPgsqlScanSpec::DefaultStartDocKey() {
  static const DocKey result;
  return result;
}

}  // namespace yb::docdb
