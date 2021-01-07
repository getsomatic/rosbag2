cmake_minimum_required(VERSION 3.5)
project(rosbag2_compression_zstd)

# Default to C99
if(NOT CMAKE_C_STANDARD)
  set(CMAKE_C_STANDARD 99)
endif()

# Default to C++14
if(NOT CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 14)
endif()

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# find dependencies
find_package(ament_cmake REQUIRED)
find_package(rosbag2_compression REQUIRED)
find_package(zstd_vendor REQUIRED)
find_package(zstd REQUIRED)

# uncomment the following section in order to fill in
# further dependencies manually.
# find_package(<dependency> REQUIRED)

add_library(${PROJECT_NAME}_zstd
  SHARED
  src/rosbag2_compression/compression_utils.cpp
  src/rosbag2_compression/zstd_compressor.cpp
  src/rosbag2_compression/zstd_decompressor.cpp)
target_include_directories(${PROJECT_NAME}_zstd
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>)
ament_target_dependencies(${PROJECT_NAME}_zstd
  rcpputils
  rcutils
  rosbag2_storage
  rosbag2_compression
  zstd)
# Causes the visibility macros to use dllexport rather than dllimport.
# This is appropriate when building the dll but not consuming it.
target_compile_definitions(${PROJECT_NAME}_zstd
  PRIVATE
  ROSBAG2_COMPRESSION_BUILDING_DLL)

if(BUILD_TESTING)
  find_package(ament_lint_auto REQUIRED)
  # the following line skips the linter which checks for copyrights
  # uncomment the line when a copyright and license is not present in all source files
  #set(ament_cmake_copyright_FOUND TRUE)
  # the following line skips cpplint (only works in a git repo)
  # uncomment the line when this package is not in a git repo
  #set(ament_cmake_cpplint_FOUND TRUE)
  ament_lint_auto_find_test_dependencies()
endif()

ament_package()
