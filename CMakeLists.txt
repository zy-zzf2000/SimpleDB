cmake_minimum_required(VERSION 3.10)

# set the project name and version
project(DBT VERSION 1.0)

# specify the C++ standard
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)

SET(CMAKE_BUILD_TYPE Debug)
set(CMAKE_C_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -Wall -g")

add_subdirectory(db)
link_directories(db)

# add the executable
add_executable(test main.c)

target_link_libraries(test PUBLIC mydb)
target_link_libraries(test PUBLIC apue)

# add the binary tree to the search path for include files
# so that we will find TutorialConfig.h
target_include_directories(test PUBLIC db)
