#include "velox/connectors/sample/SampleConnector.h"
#include <folly/init/Init.h>
#include "gmock/gmock-matchers.h"
#include "gtest/gtest.h"
#include "velox/core/Config.h"

namespace {

using namespace facebook::velox;
using namespace testing;
using namespace facebook::velox::connector::sample;

TEST(SampleConnectorTest, FactoryTest) {
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

} // namespace

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
