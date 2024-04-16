#include "velox/connectors/sample/SampleConnector.h"
#include <folly/init/Init.h>
#include "gmock/gmock-matchers.h"
#include "gtest/gtest.h"
#include "velox/core/Config.h"
#include <iostream>
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace {

using namespace facebook::velox;
using namespace facebook::velox::connector;
using namespace facebook::velox::connector::sample;
using namespace testing;

class SampleConnectorTest : public exec::test::OperatorTestBase {
 public:
  static constexpr const char* kSampleConnectorId = "sample-test";

  SampleConnectorTest() {
    auto factory = connector::getConnectorFactory(
        SampleConnectorFactory::kSampleConnectorName);
    registerConnector(factory->newConnector(
        kSampleConnectorId,
        std::make_shared<core::MemConfig>()));
  }

  ~SampleConnectorTest() {
    unregisterConnector(kSampleConnectorId);
  }
};

TEST(SampleConnectorCreationTest, FactoryTest) {
  auto factory = connector::getConnectorFactory(
      SampleConnectorFactory::kSampleConnectorName);
  EXPECT_NO_THROW(factory->newConnector(
      "sample-test", std::make_shared<core::MemConfig>()));
}

TEST_F(SampleConnectorTest, DataSinkTest) {
  EXPECT_THAT(
      []() {
        getConnector(kSampleConnectorId)->createDataSink(
            RowTypePtr{},
            std::shared_ptr<ConnectorInsertTableHandle>{},
            nullptr,
            CommitStrategy::kNoCommit);
      },
      Throws<VeloxRuntimeError>(Property(
          &VeloxRuntimeError::errorCode, StrEq(error_code::kNotImplemented))));
}

TEST_F(SampleConnectorTest, EmptyTest) {
  auto plan = exec::test::PlanBuilder()
      .startTableScan()
      .connectorId(kSampleConnectorId)
      .outputType(std::make_shared<RowType>(std::vector<std::string>{}, std::vector<std::shared_ptr<const Type>>{})) // Segfaults if not set
      .tableHandle(std::make_shared<SampleTableHandle>("sample-test")) // uses HiveTableHandle by default
      .assignments({})  // uses HiveColumnHandle by default
      .endTableScan()
      .planNode();

  exec::test::AssertQueryBuilder(plan)
      .splits(std::vector<std::shared_ptr<ConnectorSplit>>{})
      .assertEmptyResults();
  
  exec::test::AssertQueryBuilder(plan)
      .splits(std::vector<std::shared_ptr<ConnectorSplit>>{std::make_shared<ConnectorSplit>("sample-test")}) // waits infinitely if this function is not called (empty splits ok)
      .assertEmptyResults();
      // .copyResults(pool());
}

TEST_F(SampleConnectorTest, ColumnHandleTest) {
  auto plan = exec::test::PlanBuilder()
      .startTableScan()
      .connectorId(kSampleConnectorId)
      .outputType(std::make_shared<RowType>(
          std::vector<std::string>{"id"},
          std::vector<std::shared_ptr<const Type>>{BIGINT()}))
      .tableHandle(std::make_shared<SampleTableHandle>("sample-test"))
      // assignments cannot be empty because PlanBuilder will create assignments
      // using HiveColumnHandles by default
      .assignments({{"placeholder", std::shared_ptr<ColumnHandle>{}}})
      .endTableScan()
      .planNode();
  
  VELOX_ASSERT_THROW(
      exec::test::AssertQueryBuilder(plan)
          .splits(std::vector<std::shared_ptr<ConnectorSplit>>{
            std::make_shared<ConnectorSplit>("sample-test")
          })
          .assertEmptyResults(),
      "columnHandle is missing for output column");
  
  plan = exec::test::PlanBuilder()
      .startTableScan()
      .connectorId(kSampleConnectorId)
      .outputType(std::make_shared<RowType>(
          std::vector<std::string>{"id"},
          std::vector<std::shared_ptr<const Type>>{BIGINT()}))
      .tableHandle(std::make_shared<SampleTableHandle>("sample-test"))
      .assignments({{"id", std::shared_ptr<ColumnHandle>{}}})
      .endTableScan()
      .planNode();
  
  VELOX_ASSERT_THROW(
      exec::test::AssertQueryBuilder(plan)
          .splits(std::vector<std::shared_ptr<ConnectorSplit>>{
            std::make_shared<ConnectorSplit>("sample-test")
          })
          .assertEmptyResults(),
      "not a SampleColumnHandle"
  )
}

TEST_F(SampleConnectorTest, DataTest) {
  auto plan = exec::test::PlanBuilder()
      .startTableScan()
      .connectorId(kSampleConnectorId)
      .outputType(std::make_shared<RowType>(
          std::vector<std::string>{"id"},
          std::vector<std::shared_ptr<const Type>>{BIGINT()}))
      .tableHandle(std::make_shared<SampleTableHandle>("sample-test"))
      .assignments({{"id", std::make_shared<SampleColumnHandle>("id")}})
      .endTableScan()
      .planNode();
  
  exec::test::AssertQueryBuilder(plan)
      .splits(std::vector<std::shared_ptr<ConnectorSplit>>{
        std::make_shared<ConnectorSplit>("sample-test")
      })
      .assertResults(makeRowVector({makeFlatVector(std::vector<int64_t>{1, 2})}));
  
  plan = exec::test::PlanBuilder()
      .startTableScan()
      .connectorId(kSampleConnectorId)
      .outputType(std::make_shared<RowType>(
          std::vector<std::string>{"id", "value"},
          std::vector<std::shared_ptr<const Type>>{BIGINT(), BIGINT()}))
      .tableHandle(std::make_shared<SampleTableHandle>("sample-test"))
      .assignments({
        {"id", std::make_shared<SampleColumnHandle>("id")},
        {"value", std::make_shared<SampleColumnHandle>("value")}})
      .endTableScan()
      .planNode();
  
  exec::test::AssertQueryBuilder(plan)
      .splits(std::vector<std::shared_ptr<ConnectorSplit>>{
        std::make_shared<ConnectorSplit>("sample-test")
      })
      .assertResults(makeRowVector({
        makeFlatVector(std::vector<int64_t>{1, 2}),
        makeFlatVector(std::vector<int64_t>{41, 42})
      }));
}

} // namespace

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
