cmake_minimum_required(VERSION 4.1)
project(WorkManager)

set(CMAKE_CXX_STANDARD 20)

find_package(Boost CONFIG REQUIRED COMPONENTS asio system uuid lexical_cast json log)
find_package(activemq-cpp CONFIG REQUIRED)
find_package(miniocpp REQUIRED)

add_executable(WorkManager main.cpp)
add_link_options("-fuse-ld=mold")

target_link_libraries(WorkManager PRIVATE
        Boost::asio
        Boost::system
        Boost::json
        Boost::log
        unofficial::activemq-cpp::activemq-cpp
        miniocpp::miniocpp
        Boost::uuid
        Boost::lexical_cast
)