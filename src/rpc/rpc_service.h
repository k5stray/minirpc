#ifndef DISPATCHER_H
#define DISPATCHER_H
#include <unordered_map>
#include <functional>
#include <string>
#include <memory>
#include "../util/noncopyable.h"
#include "../rpc/rpc_conf.h"
#include "../net/tcp_server.h"
#include "../../proto/rpc_proto.pb.h"

struct ReqParam
{
    RpcHeader header;
    uint64_t serial_id;
    std::string service_name;
    std::string func_name;
    void clear() {
        header.total_len = 0;
        serial_id = 0;
        service_name.clear();
        func_name.clear();
    }
};

struct RespParam
{
    RpcHeader header;
    uint64_t serial_id;
    int errnum;
    std::string err_msg;
    void clear() {
        header.total_len = 0;
        err_msg.clear();
    }
};
using RpcHandler = std::function<void(const ReqParam*, RespParam*)>;
class BaseService {
public:
    virtual ~BaseService() = default;
    virtual int dispatch(const std::string& method,
        const minirpc::DemoReq &req, minirpc::DemoResp &rpc_resp) = 0;
protected:
    std::unordered_map<std::string, RpcHandler> method_map_;
};

class ServiceManager : Noncopyable
{
public:
    static ServiceManager& get_instance();

    void register_service(const std::string& service_name, std::shared_ptr<BaseService> service);

    std::shared_ptr<BaseService> getService(const std::string& service_name);

    void bind_tcpserver(TcpServer &server, int port, int weight);
    bool set_server_id(std::string r_id, uint64_t id);
    void start(TcpServer &server);
    void stop(TcpServer &server);
    void stop_all_server();
    void register_server_done(std::string cmd);

private:
    ServiceManager() = default;
    ~ServiceManager() = default;
    ServiceManager(const ServiceManager&) = delete;
    ServiceManager& operator=(const ServiceManager&) = delete;

    std::unordered_map<std::string, std::shared_ptr<BaseService>> service_map_;
    std::unordered_map<std::string, TcpServer*> bind_tcpservers_;
};

#define REGISTER_RPC_SERVICE(Cls) \
__attribute__((used)) \
static bool auto_reg_##Cls = []() { \
    ServiceManager::get_instance().register_service( \
        #Cls, \
        std::make_shared<Cls>() \
    ); \
    return true; \
}();

#define BIND_METHOD(class, func) \
method_map_[#func] = std::bind(&class::func, this, std::placeholders::_1, std::placeholders::_2);

#endif
