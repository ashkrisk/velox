#include "velox/connectors/sample/SampleConnector.h"
#include <folly/init/Init.h>
#include "gmock/gmock-matchers.h"
#include "gtest/gtest.h"
#include "velox/core/Config.h"
#include <iostream>
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"

namespace {

using namespace facebook::velox;
using namespace facebook::velox::connector;
using namespace facebook::velox::connector::sample;
using namespace testing;

class SampleConnectorTest : public exec::test::OperatorTestBase {
};

TEST_F(SampleConnectorTest, FactoryTest) {
  auto factory = connector::getConnectorFactory(
      SampleConnectorFactory::kSampleConnectorName);
  // EXPECT_THAT(
  //     [&factory]() { factory->newConnector("sample-test",
  //     std::make_shared<core::MemConfig>()); },
  //     Throws<VeloxRuntimeError>(Property(&VeloxRuntimeError::errorCode,
  //     StrEq(error_code::kNotImplemented)))
  // );
  EXPECT_NO_THROW(factory->newConnector(
      "sample-test", std::make_shared<core::MemConfig>()));
}

TEST_F(SampleConnectorTest, ConnectorTest) {
  auto factory = connector::getConnectorFactory(
      SampleConnectorFactory::kSampleConnectorName);
  auto connector =
      factory->newConnector("sample-test", std::make_shared<core::MemConfig>());

  registerConnector(connector);

  EXPECT_THAT(
      [&connector]() {
        connector->createDataSink(
            RowTypePtr{},
            std::shared_ptr<ConnectorInsertTableHandle>{},
            nullptr,
            CommitStrategy::kNoCommit);
      },
      Throws<VeloxRuntimeError>(Property(
          &VeloxRuntimeError::errorCode, StrEq(error_code::kNotImplemented))));

  // connector->createDataSource(
  //     RowTypePtr{},
  //     std::shared_ptr<ConnectorTableHandle>{},
  //     std::unordered_map<std::string, std::shared_ptr<ColumnHandle>>{},
  //     nullptr);
    
  auto plan = exec::test::PlanBuilder()
      .startTableScan()
      .connectorId("sample-test")
      // .outputType(std::make_shared<RowType>(std::vector<std::string>{"data"}, std::vector<std::shared_ptr<const Type>>{HUGEINT()})) // Segfaults if not set
      .outputType(std::make_shared<RowType>(std::vector<std::string>{}, std::vector<std::shared_ptr<const Type>>{})) // Segfaults if not set
      .tableHandle(std::make_shared<SampleTableHandle>("sample-test")) // uses HiveTableHandle by default
      // .assignments(std::unordered_map<std::string, std::shared_ptr<ColumnHandle>>{{"data", std::shared_ptr<ColumnHandle>{}}})  // uses HiveColumnHandle by default
      .assignments({})  // uses HiveColumnHandle by default
      .endTableScan()
      .planNode();

  auto results = exec::test::AssertQueryBuilder(plan)
      .splits(std::vector<std::shared_ptr<ConnectorSplit>>{std::make_shared<ConnectorSplit>("sample-test")}) // waits infinitely if this function is not called (empty splits ok)
      // .assertResults(makeRowVector({makeFlatVector(std::vector<int64_t>{42})}));
      .assertEmptyResults();
      // .copyResults(pool());

  // test::assertEqualVectors(results, makeRowVector({}));
}

} // namespace

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
