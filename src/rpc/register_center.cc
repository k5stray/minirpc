
#include <vector>

#include "register_center.h"
#include "../rpc/rpc_conf.h"
#include "../codec/rpc_codec.h"
#include "../util/rpc_util.h"
#include "../util/log_util.h"

enum command_type {
    REGISTER_SERVER = 0,
    PING,
    GET_SERVER_LIST,
    COMMAND_END
};

const char* command_str[] = {
    [REGISTER_SERVER] = "REGISTER_SERVER",
    [PING] = "PING",
    [GET_SERVER_LIST] = "GET_SERVER_LIST",
    NULL,
};

static int get_command_type(const char *data, int len)
{
    for (int i = REGISTER_SERVER; i < COMMAND_END; i++) {
        if (strncmp(command_str[i], data, len) == 0) {
            return i;
        }
    }
    return -1;
}

static inline void update_time(TimePoint &time)
{
    time = Clock::now();
}

static bool check_timeout(TimePoint time, uint64_t timeout)
{
    uint64_t sec = (uint64_t)std::chrono::duration<double>(Clock::now() - time).count();
    if (sec < timeout) {
        update_time(time);
        return true;
    }
    return false;
}

bool RegisterCenter::check_id(uint64_t id)
{
    return (register_list_.count(id) > 0);
}

void RegisterCenter::update_id(uint64_t id)
{
    if (register_list_.count(id) > 0)
        register_list_[id].timepoint = Clock::now();
}

void RegisterCenter::add_node(uint64_t id, RegisterInfo node)
{
    register_list_[id] = node;
}

void RegisterCenter::get_nodes(minirpc::RegisterMessage &resp)
{
    for (const auto &node : register_list_) {
        minirpc::IPElem *elem = resp.add_iplist();
        elem->set_ip(node.second.ip);
        elem->set_port(node.second.port);
        elem->set_weight(node.second.weight);
    }
}

static int center_send_response(TcpConnectionPtr conn, minirpc::RegisterMessage &resp)
{
    std::string resp_body, send_pkt;
    resp.SerializeToString(&resp_body);
    encode_packet(resp_body, send_pkt, MSG_REGISTER);
    return conn->send(send_pkt.data(), send_pkt.size());
}

static int handle_req(TcpConnectionPtr conn, const char* data, int len)
{
    std::string body;
    RpcHeader header;
    if (!decode_packet(data, len, header, body)) {
        LOG_ERROR_("decaode fail!\n");
        return 0;
    }

    minirpc::RegisterMessage req;
    req.ParseFromString(body);

    std::string cmd = req.cmd();
    uint64_t req_id = req.id();

    char buf[512] = {0};
    minirpc::RegisterMessage resp;
    int type = get_command_type(cmd.c_str(), cmd.size());
    switch (type) {
        case REGISTER_SERVER: {
            uint64_t resp_id;
            std::string id_str;
            if (RegisterCenter::get_instance().check_id(req_id)) {
                LOG_WARN_("ID %lu already register!\n", req_id);
                RegisterCenter::get_instance().update_id(req_id);
                resp_id = req_id;
            } else {
                RegisterInfo node;
                resp_id = get_register_id();
                node.ip = req.iplist(0).ip();
                node.port = req.iplist(0).port();
                node.weight = req.iplist(0).weight();
                node.timepoint = Clock::now();
                LOG_INFO_("center: ADD node, ip:%s port:%d weight:%d\n",
                    node.ip.c_str(), node.port, node.weight);
                RegisterCenter::get_instance().add_node(resp_id, node);
            }
            resp.set_id(resp_id);
            resp.set_cmd(std::to_string(req_id));

            center_send_response(conn, resp);
            break;
        }
        case PING: {
            RegisterCenter::get_instance().update_id(req_id);
            resp.set_id(req_id);
            resp.set_cmd("PONG");

            center_send_response(conn, resp);
            break;
        }
        case GET_SERVER_LIST: {
            RegisterCenter::get_instance().get_nodes(resp);
            center_send_response(conn, resp);
            break;
        }
        default:
            break;
    }

    return header.total_len;
}

static bool ready2read(const char *data, int len)
{
    RpcHeader header;
    if (decode_header(data, len, header) && header.total_len <= len)
        return true;

    return false;
}

void RegisterCenter::start()
{
    if (running)
        return;
    int fd = get_timer_fd(10, 0);
    if (fd < 0) {
        return;
    }

    server = new TcpServer(0);
    server->set_timer(fd, [this](uint64_t val){
        for (auto it = register_list_.begin(); it != register_list_.end();) {
            auto data = it->second;
            if (!check_timeout(data.timepoint, (val * 20))) {
                LOG_WARN_("node PING/PONG timeout, delete=>id:%lu ip:%s port:%d weight:%d\n", it->first, data.ip.c_str(), data.port, data.weight);
                it = register_list_.erase(it);
            } else {
                it++;
            }
        }
    });

    server->add_listen(REGISTER_CENTER_PORT, handle_req, ready2read);
    server->start();
    running = true;
}

void RegisterCenter::stop()
{
    server->stop();
}

RegisterCenter::RegisterCenter() : running(false) {}

RegisterCenter::~RegisterCenter()
{
    delete server;
}