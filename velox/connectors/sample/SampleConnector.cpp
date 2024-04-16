#include "SampleConnector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::connector::sample {

SampleColumnHandle::SampleColumnHandle(std::string colName)
    : name_{std::move(colName)} {
  VELOX_CHECK(
    name_ == "id" || name_ == "value",
    "invalid SampleColumnHandle '{}', must be one of [id, value]",
    name_
  );
}

SampleDataSource::SampleDataSource(
    RowTypePtr outputType,
    const std::unordered_map<std::string, std::shared_ptr<ColumnHandle>>& assignments,
    memory::MemoryPool* pool)
    : outputType_{std::move(outputType)}, assignments_{assignments}, pool_{pool} {
  VELOX_CHECK_NOT_NULL(pool);
  for (auto const& colName : outputType_->names()) {
    auto it = assignments_.find(colName);
    VELOX_CHECK(
      it != assignments_.end(),
      "columnHandle is missing for output column {}",
      colName
    );

    auto sampleColumn =
        std::dynamic_pointer_cast<SampleColumnHandle>(it->second);
    VELOX_CHECK_NOT_NULL(
        sampleColumn,
        "column handle for column with alias '{}' is not a SampleColumnHandle",
        colName);
    columns_.push_back(std::dynamic_pointer_cast<SampleColumnHandle>(it->second));
  }
}

void SampleDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  // VELOX_NYI("not implemented yet");
  splits_ += 1;
}
std::optional<RowVectorPtr> SampleDataSource::next(
    uint64_t size,
    velox::ContinueFuture& future) {
  // VELOX_NYI("not implemented yet");
  if (completed_ >= splits_) {
    return nullptr;
  }
  completed_ += 1;
  if (columns_.size() <= 0) {
    return RowVector::createEmpty(outputType_, pool_);
  }

  std::vector<VectorPtr> children;
  for (auto const& col : columns_) {
    auto vec = BaseVector::create<FlatVector<int64_t>>(BIGINT(), 2, pool_);
    if (col->name() == "id") {
      vec->set(0, 1);
      vec->set(1, 2);
    }
    else {
      vec->set(0, 41);
      vec->set(1, 42);
    }
    children.push_back(std::move(vec));
  }
  return std::make_shared<RowVector>(
      pool_, outputType_, BufferPtr(nullptr), 2, children);
}

VELOX_REGISTER_CONNECTOR_FACTORY(std::make_shared<SampleConnectorFactory>())

} // namespace facebook::velox::connector::sample
