#include "rpc_service.h"
#include "../net/tcp_server.h"
#include "../codec/rpc_codec.h"

struct RpcRequest : public ReqParam
{
    std::string body;
};

struct RpcResponse : public  RespParam
{
    std::string result;
};

class EchoService : public BaseService
{
public:
    EchoService()
    {
        BIND_METHOD(EchoService, echo);
    }

    int dispatch(const std::string& method, const minirpc::DemoReq &req, minirpc::DemoResp &resp) override
    {
        int ret = 0;
        RpcRequest l_req;
        RpcResponse l_resp;
        l_req.serial_id = req.serialid();
        l_req.service_name = req.servername();
        l_req.func_name = req.funcname();
        l_req.body = req.param();

        l_resp.serial_id = l_req.serial_id;
        l_resp.errnum = 0;

        auto it = method_map_.find(method);
        if(it != method_map_.end()) {
            it->second((ReqParam*)&l_req, (RespParam*)&l_resp);
        } else {
            l_resp.errnum = 2;
            l_resp.err_msg = "method not found";
            ret = -1;
        }

        resp.set_serialid(l_resp.serial_id);
        resp.set_errnum(l_resp.errnum);
        if (l_resp.err_msg.size() > 0)
            resp.set_errinfo(l_resp.err_msg);
        if (l_resp.result.size() > 0)
            resp.set_result(l_resp.result);

        return ret;
    }

private:
    void echo(const ReqParam* req, RespParam* resp)
    {
        static unsigned long long nums = 0;
        const RpcRequest *req_ = static_cast<const RpcRequest*>(req);
        RpcResponse *resp_ = static_cast<RpcResponse*>(resp);
        resp_->result = req_->body;
    }

};

REGISTER_RPC_SERVICE(EchoService)