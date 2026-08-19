#include <iostream>
#include <cstdio>
#include <atomic>
#include "src/rpc/rpc_stub.h"
#include "proto/rpc_proto.pb.h"
#include "src/util/rpc_util.h"
#include "src/util/log_util.h"
#include <google/protobuf/stubs/common.h>

std::condition_variable g_async_wait;
std::mutex g_async_mtx;
std::atomic<int> g_count{0};
std::atomic<int> g_async{0};
std::atomic<int> g_sync{0};
int main()
{
    LOG_INIT("./logs", LOG_INFO);
    RpcStub stub;
    if(!stub.init())
    {
        std::cerr << "connect failed" << std::endl;
        return -1;
    }

    std::string message = "hello";

    minirpc::DemoReq req;
    long long reqid = get_serial_id();
    req.set_serialid(reqid);
    req.set_servername("EchoService");
    req.set_funcname("echo");
    req.set_param(message);
    std::string resp_bin;
    LOG_INFO_("[async] send:%s\n", message.c_str());
    g_count++;
    minirpc::DemoResp resp;
    int ret = stub.call_async(req, [](minirpc::DemoResp resp){
        LOG_INFO_("[async] recv:%s\n", resp.result().c_str());
        g_async++;
        g_async_wait.notify_one();
        return 0;
    });

    std::unique_lock<std::mutex> mtx(g_async_mtx);
    g_async_wait.wait_for(mtx, std::chrono::seconds(5), [](){
        return (g_async.load() > 0);
    });

    reqid = get_serial_id();
    req.set_serialid(reqid);
    req.set_servername("EchoService");
    req.set_funcname("echo");
    req.set_param(message);
    g_count++;
    LOG_INFO_("[ sync] send:%s\n", message.c_str());
    if (stub.call(req, resp)) {
        g_sync++;
        LOG_INFO_("[ sync] recv:%s\n", resp.result().c_str());
    } else {
        g_sync++;
        std::cerr << "####rpc sync call fail!!" << std::endl;
    }

    LOG_INFO_("request total:%d, pass:%d async:%d sync:%d\n", g_count.load(), (g_async.load() + g_sync.load()), g_async.load(), g_sync.load());
    stub.stop();
    google::protobuf::ShutdownProtobufLibrary();
    std::cin.get();
    return 0;
}
