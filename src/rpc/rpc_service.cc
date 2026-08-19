#include <sys/timerfd.h>

#include "rpc_service.h"
#include "../codec/rpc_codec.h"
#include "../../proto/rpc_proto.pb.h"
#include "../util/rpc_util.h"
#include "../util/log_util.h"

ServiceManager& ServiceManager::get_instance() {
    static ServiceManager ins;
    return ins;
}

void ServiceManager::register_service(const std::string& service_name, std::shared_ptr<BaseService> service) {
    service_map_[service_name] = service;
}

std::shared_ptr<BaseService> ServiceManager::getService(const std::string& service_name) {
    auto it = service_map_.find(service_name);
    return (it != service_map_.end()) ? it->second : nullptr;
}

static bool ready2read(const char *data, int len)
{
    RpcHeader header;
    if (!decode_header(data, len, header) || header.total_len < len)
        return false;

    return true;
}

static int read_handle(TcpConnectionPtr conn, const char* data, int len)
{
    std::string body;
    RpcHeader header;
    if (!decode_packet(data, len, header, body)) {
        conn->clean_readbuf();
        LOG_ERROR_("decaode fail!\n");
        return 0;
    }

    switch (header.type) {
        case MSG_DEMO_RPC: {
            minirpc::DemoReq req;
            req.ParseFromString(body);
            
            minirpc::DemoResp resp;
            auto service = ServiceManager::get_instance().getService(req.servername());
            if(service != nullptr) {
                service->dispatch(req.funcname(), req, resp);
            } else {
                resp.set_errinfo("service not exist");
                resp.set_errnum(2);
            }

            std::string resp_body, send_pkt;
            resp.SerializeToString(&resp_body);
            encode_packet(resp_body, send_pkt, MSG_DEMO_RPC);
        
            conn->send(send_pkt.data(), send_pkt.size());
            break;
        }
        case MSG_REGISTER: {
            minirpc::RegisterMessage resp;
            resp.ParseFromString(body);
            const std::string& cmd = resp.cmd();
            
            if (cmd == "PONG") {
                // do nothing;
            }else {
                ServiceManager::get_instance().register_server_done(cmd);
                ServiceManager::get_instance().set_server_id(cmd, resp.id());
            }
            break;
        }
        default:
            LOG_ERROR_("Unspport type:%d\n", header.type);
            break;
    }

    return header.total_len;
}

bool ServiceManager::set_server_id(std::string r_id, uint64_t id)
{
    if (bind_tcpservers_.count(r_id) > 0) {
        TcpServer *server = bind_tcpservers_[r_id];
        server->set_registerid(id);
        return true;
    }
    return false;
}

void ServiceManager::register_server_done(std::string r_id)
{
    if (bind_tcpservers_.count(r_id) > 0) {
        TcpServer *server = bind_tcpservers_[r_id];
        server->register_done();
    }
}

void ServiceManager::bind_tcpserver(TcpServer &server, int port, int weight)
{
    if (!server.connect_register_center(REGISTER_CENTER_IPADDR, REGISTER_CENTER_PORT, read_handle, ready2read)) {
        LOG_FATAL_("Fail to connect register center!\n");
        return;
    }

    server.add_listen(port, read_handle, ready2read);

    uint64_t r_id = get_serial_id();
    minirpc::RegisterMessage req;
    req.set_cmd("REGISTER_SERVER");
    req.set_id(r_id);
    bind_tcpservers_[std::to_string(r_id)] = &server;

    minirpc::IPElem *node = req.add_iplist();
    node->set_ip("127.0.0.1");
    node->set_port(port);
    node->set_weight(weight);

    std::string resp_body, send_pkt;
    req.SerializeToString(&resp_body);
    encode_packet(resp_body, send_pkt, MSG_REGISTER);
    if (server.send_message_to_center(send_pkt.data(), send_pkt.size()) < 0) {
        LOG_FATAL_("Fail to register server node\n");
        return;
    }

    int fd = get_timer_fd(4, 0);
    if (fd < 0) {
        LOG_ERROR_("init timer fail!\n");
        return;
    }

    server.set_timer(fd, [this, &server](uint64_t val){
        if (!server.registered()) {
            LOG_WARN_("Server is not registered, skip PING register center\n");
            return;
        }
        minirpc::RegisterMessage req;
        req.set_cmd("PING");
        req.set_id(server.get_registerid());

        std::string resp_body, send_pkt;
        req.SerializeToString(&resp_body);
        encode_packet(resp_body, send_pkt, MSG_REGISTER);

        server.send_message_to_center(send_pkt.data(), send_pkt.size());
    });
}

void ServiceManager::start(TcpServer &server)
{
    server.start();
}

void ServiceManager::stop(TcpServer &server)
{
    server.stop();
}

void ServiceManager::stop_all_server()
{
    for (auto &elem : bind_tcpservers_) {
        elem.second->stop();
    }
}
