cmake_minimum_required(VERSION 3.8 FATAL_ERROR)
project(bigram_trigram_analysis LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)

find_package(OpenMP REQUIRED)

# source to build
add_executable(bigram_trigram_analysis main main.cpp)
# link libraries
target_link_libraries(bigram_trigram_analysis OpenMP::OpenMP_CXX)