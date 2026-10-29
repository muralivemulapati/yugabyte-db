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

#include "yb/integration-tests/upgrade-tests/upgrade_test_base.h"

#include "yb/yql/pgwrapper/libpq_utils.h"

namespace yb {

// yb_push_hash_column_lists_to_docdb is a kLocalVolatile AutoFlag (initial false, target true).
// An old tserver drops the unknown column_lists field, so the flag has to stay off until every
// tserver is on this build and AutoFlags are promoted.
class HashColumnListsUpgradeTest : public UpgradeTestBase {
 public:
  HashColumnListsUpgradeTest() : UpgradeTestBase(kBuild_2_25_0_0) {}

 protected:
  void SetUpOptions(ExternalMiniClusterOptions& opts) override {
    opts.num_tablet_servers = kNumTabletServers;
    UpgradeTestBase::SetUpOptions(opts);
  }

  Result<std::string> TserverFlag(size_t tserver_index) {
    return cluster_->tablet_server(tserver_index)->GetFlag(kFlagName);
  }

  Result<std::string> SessionGuc(size_t tserver_index) {
    auto conn = VERIFY_RESULT(cluster_->ConnectToDB("yugabyte", tserver_index));
    return conn.FetchRow<std::string>("SHOW yb_push_hash_column_lists_to_docdb");
  }

  void ExpectOff(size_t tserver_index) {
    ASSERT_EQ(ASSERT_RESULT(TserverFlag(tserver_index)), "false");
    ASSERT_EQ(ASSERT_RESULT(SessionGuc(tserver_index)), "off");
  }

  void ExpectOn(size_t tserver_index) {
    ASSERT_EQ(ASSERT_RESULT(TserverFlag(tserver_index)), "true");
    ASSERT_EQ(ASSERT_RESULT(SessionGuc(tserver_index)), "on");
  }

  static constexpr auto kFlagName = "ysql_yb_push_hash_column_lists_to_docdb";
  static constexpr size_t kNumTabletServers = 3;
  // First tserver moved to the current build while the others stay on the old build.
  static constexpr size_t kUpgradedTserver = 0;
};

TEST_F(HashColumnListsUpgradeTest, TestFlagStaysOffUntilPromotion) {
  ASSERT_OK(StartClusterInOldVersion());
  ASSERT_OK(RestartAllMastersInCurrentVersion(kNoDelayBetweenNodes));

  ASSERT_OK(RestartTServerInCurrentVersion(
      *cluster_->tablet_server(kUpgradedTserver), /*wait_for_cluster_to_stabilize=*/true));
  ASSERT_NO_FATAL_FAILURE(ExpectOff(kUpgradedTserver));
  // Promotion is rejected while any tserver is still on the old build.
  ASSERT_NOK_STR_CONTAINS(
      PromoteAutoFlags(AutoFlagClass::kLocalVolatile),
      "Cannot promote AutoFlags before all yb-tservers have been upgraded to the current version");
  ASSERT_NO_FATAL_FAILURE(ExpectOff(kUpgradedTserver));

  ASSERT_OK(RestartAllTServersInCurrentVersion(kNoDelayBetweenNodes));
  for (size_t i = 0; i < kNumTabletServers; ++i) {
    ASSERT_NO_FATAL_FAILURE(ExpectOff(i));
  }

  ASSERT_OK(PromoteAutoFlags(AutoFlagClass::kLocalVolatile));
  for (size_t i = 0; i < kNumTabletServers; ++i) {
    ASSERT_NO_FATAL_FAILURE(ExpectOn(i));
  }
}

}  // namespace yb
