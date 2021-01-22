CMAKE_MINIMUM_REQUIRED(VERSION 2.8) # cmake最低版本要求

project(KafkaProducer)    # 工程名 CppTemplate

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_COMPILER "g++")
set(CMAKE_CXX_FLAGS "-std=c++11 -Wall -g ${CMAKE_CXX_FLAGS}")
set(CMAKE_INCLUDE_CURRENT_DIR ON)

set(LINK_DIR  "/usr/lib64/")
SET(EXECUTABLE_OUTPUT_PATH "${PROJECT_SOURCE_DIR}/bin") # 可执行文件输出目录

INCLUDE_DIRECTORIES("${PROJECT_SOURCE_DIR}/include") # 头文件包含目录
link_directories(${LINK_DIR})
# 这段代码可以区分操作系统
MESSAGE("Identifying the OS...")
if(WIN32)
  MESSAGE("This is Windows.")
elseif(APPLE)
  MESSAGE("This is MacOS.")
elseif(UNIX)
  MESSAGE("This is Linux.")
endif()
# 这段代码可以区分操作系统

AUX_SOURCE_DIRECTORY(src DIR_SRCS) # 添加源代码文件夹, 自动扫描所有文件

# 指定生成目标
add_executable(KafkaProducer  # KafkaProducer
   ${DIR_SRCS})

# 将第三方的库链接到可执行文件
target_link_libraries(KafkaProducer rdkafka++)
