add_executable(velox_sample_connector_test SampleConnectorTest.cpp)
add_test(NAME velox_sample_connector_test COMMAND velox_sample_connector_test)

target_link_libraries(velox_sample_connector_test velox_sample_connector gtest
                      gmock)
