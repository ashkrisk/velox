add_library(velox_sample_connector OBJECT SampleConnector.cpp)
target_link_libraries(velox_sample_connector velox_connector)

if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
endif()
