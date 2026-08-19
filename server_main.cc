#include <iostream>
#include <signal.h>
#include <google/protobuf/stubs/common.h>
#include "src/rpc/rpc_service.h"
#include "src/net/tcp_server.h"
#include "src/rpc/register_center.h"
#include "proto/rpc_proto.pb.h"
#include "src/util/log_util.h"

void handler(int sig) {
    if (sig == SIGINT || sig == SIGTERM) {
        RegisterCenter::get_instance().stop();
        ServiceManager::get_instance().stop_all_server();
    }
}

void register_center()
{
    RegisterCenter::get_instance().start();
}

void server1()
{
    TcpServer server(1);
    ServiceManager::get_instance().bind_tcpserver(server, 8888, 4);
    ServiceManager::get_instance().start(server);
}

void server2()
{
    TcpServer server(1);
    ServiceManager::get_instance().bind_tcpserver(server, 8000, 6);
    ServiceManager::get_instance().start(server);
}

int main()
{
    LOG_INIT("./logs", LOG_INFO);

    struct sigaction sa{};
    sa.sa_handler = handler;
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);

    std::vector<std::thread> ths;
    ths.emplace_back(register_center);
    std::this_thread::sleep_for(std::chrono::milliseconds(500)); // Waiting for register center to running

    ths.emplace_back(server1);
    ths.emplace_back(server2);

    for (auto &th : ths) {
        th.join();
    }

    google::protobuf::ShutdownProtobufLibrary();
    return 0;
}
