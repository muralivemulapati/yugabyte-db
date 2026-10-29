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

#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <vector>

#include "yb/qlexpr/ql_scanspec.h"

#include "yb/docdb/doc_ql_scanspec.h"
#include "yb/docdb/docdb_fwd.h"
#include "yb/dockv/key_bytes.h"

#include "yb/rocksdb/options.h"

#include "yb/util/result.h"

namespace yb::docdb {

using PgsqlConditionPBPtr = rpc::AnyMessagePtrBase<
    const PgsqlConditionPB*, const LWPgsqlConditionPB*>;

using PgsqlHashColumnListsPBPtr = rpc::AnyMessagePtrBase<
    const PgsqlHashColumnListsPB*, const LWPgsqlHashColumnListsPB*>;

// One slice of a hash-column IN list: the linear index range [begin, end) over the cartesian
// product. The last column is the fast digit. exhausted means end is past the last permutation.
struct HashColumnListCursor {
  uint64_t begin = 0;
  uint64_t end = 0;
  bool exhausted = true;
};

// DocDB variant of scanspec.
class DocPgsqlScanSpec : public qlexpr::YQLScanSpec {
 public:
  // Scan for the specified doc_key.
  DocPgsqlScanSpec(const Schema& schema,
                   const rocksdb::QueryId query_id,
                   const dockv::DocKey& doc_key,
                   const std::optional<int32_t> hash_code = std::nullopt,
                   const std::optional<int32_t> max_hash_code = std::nullopt,
                   const dockv::DocKey& start_doc_key = DefaultStartDocKey(),
                   bool is_forward_scan = true,
                   const size_t prefix_length = 0);

  // Scan for the given hash key, a condition, and optional doc_key.
  //
  // Note: std::reference_wrapper is used instead of raw lvalue reference to prevent
  // temporary objects usage. The following code wont compile:
  //
  // DocPgsqlScanSpec spec(...{} /* hashed_components */, {} /* range_components */...);
  DocPgsqlScanSpec(const Schema& schema,
                   const rocksdb::QueryId query_id,
                   const ArenaPtr& arena,
                   const std::vector<Slice>& encoded_hashed_components,
                   std::reference_wrapper<const dockv::KeyEntryValues> range_components,
                   PgsqlConditionPBPtr condition,
                   std::optional<int32_t> hash_code,
                   std::optional<int32_t> max_hash_code,
                   const dockv::DocKey& start_doc_key = DefaultStartDocKey(),
                   bool is_forward_scan = true,
                   const dockv::DocKey& lower_doc_key = DefaultStartDocKey(),
                   const dockv::DocKey& upper_doc_key = DefaultStartDocKey(),
                   const size_t prefix_length = 0,
                   PgsqlHashColumnListsPBPtr column_lists = PgsqlHashColumnListsPBPtr(),
                   uint64_t column_list_begin = 0,
                   std::optional<uint64_t> column_list_end = std::nullopt,
                   size_t column_list_key_budget = 0);

  DocPgsqlScanSpec(const Schema& schema,
                   const PgsqlConditionPB* condition);

  // Returns the lower/upper range components of the key.
  dockv::KeyEntryValues RangeComponents(
      qlexpr::BoundType bound_type,
      std::vector<bool>* inclusivities = nullptr) const override;

  const std::shared_ptr<std::vector<qlexpr::OptionList>>& options() const override {
    return options_;
  }

  const std::vector<ColumnId>& options_indexes() const override { return options_col_ids_; }

  const ColGroupHolder& options_groups() const override { return options_groups_; }

  const HashColumnListCursor& hash_column_list_cursor() const { return hash_column_list_cursor_; }

 private:
  static const dockv::DocKey& DefaultStartDocKey();

  dockv::KeyEntryValues DoRangeComponents(
      qlexpr::BoundType bound_type,
      std::vector<bool>* inclusivities = nullptr,
      bool* trivial = nullptr) const;

  void CompleteBounds();

  // Returns the lower/upper doc key based on the passed hashed components and range components
  // field.
  qlexpr::ScanBounds CalculateBounds(
      const std::vector<Slice>& encoded_hashed_components, const Schema& schema) const;

  // Initialize options_ if range columns have one or more options (i.e. using EQ/IN
  // conditions). Otherwise options_ will stay null and we will only use the range_bounds for
  // scanning.
  template <class ConditionPB>
  void InitOptions(const ConditionPB& condition);

  // Expand one slice of PgsqlHashColumnListsPB into the hash option group consumed by
  // HybridScanChoices. The slice is the linear range [column_list_begin, column_list_end). When
  // column_list_end is unset, the slice grows until column_list_key_budget permutations have
  // yb_hash_code in [hash_code_, max_hash_code_]; permutations outside that range are not counted.
  // A set end is rebuilt as-is, so a resumed slice cannot grow past the range that produced
  // next_row_key. hash_column_list_cursor_ is that range.
  template <class HashColumnListsPB>
  void InitHashColumnLists(
      const HashColumnListsPB& column_lists,
      uint64_t column_list_begin,
      std::optional<uint64_t> column_list_end,
      size_t column_list_key_budget);

  // The range/hash value options if set (possibly more than one due to IN conditions).
  std::shared_ptr<std::vector<qlexpr::OptionList>> options_;

  // Ids of key columns that have filters such as h1 IN (1, 5, 6, 9) or r1 IN (5, 6, 7)
  std::vector<ColumnId> options_col_ids_;

  // The range_components are owned by the caller of QLScanSpec.
  const dockv::KeyEntryValues* range_components_;

  // Groups of column indexes found from the filters.
  // Eg: If we had an incoming filter of the form (r1, r3, r4) IN ((1,2,5), (5,4,3), ...)
  // AND r2 <= 5
  // where (r1,r2,r3,r4) is the primary key of this table, then
  // options_groups_ would contain the groups {0,2,3} and {1}.
  ColGroupHolder options_groups_;

  // True when column_lists was set but expanded to no permutations in [hash_code_, max_hash_code_].
  // CalculateBounds must not treat that as an unconstrained hash-range scan.
  bool hash_column_lists_empty_ = false;

  HashColumnListCursor hash_column_list_cursor_;

  // Hash code is used if encoded_hashed_components vector is empty.
  // hash values are positive int16_t.
  const std::optional<int32_t> hash_code_;

  // Max hash code is used if encoded_hashed_components vector is empty.
  // hash values are positive int16_t.
  const std::optional<int32_t> max_hash_code_;

  // Starting doc key when requested by the client.
  const dockv::KeyBytes start_doc_key_;

  DISALLOW_COPY_AND_ASSIGN(DocPgsqlScanSpec);
};

}  // namespace yb::docdb
