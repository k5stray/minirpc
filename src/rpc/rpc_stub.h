#ifndef RPC_STUB_H
#define RPC_STUB_H
#include "../net/tcp_client.h"
#include "../../proto/rpc_proto.pb.h"
#include <string>

class RpcStub
{
public:
    RpcStub():inited(false){}
    bool init();
    bool call(const minirpc::DemoReq &req, minirpc::DemoResp &resp);
    bool call_async(const minirpc::DemoReq &req, ReadCallback callback);
    void stop();
private:
    bool inited;
    TcpClient cli_;
};

#endif
