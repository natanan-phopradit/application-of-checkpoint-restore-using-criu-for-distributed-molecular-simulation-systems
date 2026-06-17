cmake_minimum_required(VERSION 4.1)
project(Project2 LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 20)

find_package(Boost CONFIG REQUIRED COMPONENTS asio system process chrono json log)
find_package(PkgConfig REQUIRED)
find_package(activemq-cpp CONFIG REQUIRED)
find_package(lfreist-hwinfo CONFIG REQUIRED)

find_path(AYUSH272002_DOTENV_INCLUDE_DIRS "dotenv/dotenv.hpp")

pkg_check_modules(CRIU REQUIRED criu)

add_executable(my_app main.cpp)
add_link_options("-fuse-ld=mold")

target_include_directories(my_app PRIVATE
    ${CRIU_INCLUDE_DIRS}
)


target_link_libraries(my_app PRIVATE
        ${CRIU_LIBRARIES}
        Boost::asio
        Boost::system
        Boost::process
        Boost::chrono
        Boost::json
        Boost::log
        unofficial::activemq-cpp::activemq-cpp
        lfreist-hwinfo::hwinfo
)
