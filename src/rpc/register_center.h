#ifndef REGISTER_CENTER_H
#define REGISTER_CENTER_H

#include <ctime>
#include <chrono>
#include <cstdint>
#include <unordered_map>
#include <atomic>
#include <cstdint>

#include "../net/tcp_server.h"
#include "../../proto/rpc_proto.pb.h"

using Clock = std::chrono::steady_clock;
using TimePoint = std::chrono::time_point<Clock>;

static inline uint64_t get_register_id()
{
    static std::atomic<uint16_t> inc_id{1000};
    uint16_t seq = inc_id.fetch_add(1, std::memory_order_relaxed);
    uint64_t time_us = std::chrono::duration_cast<std::chrono::microseconds>(
                            Clock::now().time_since_epoch()).count();
    uint64_t time_low48 = time_us & 0x0000FFFFFFFFFFFFULL;
    return (static_cast<uint64_t>(seq) << 48) | time_low48;
}

struct Message {
    uint16_t len;
    uint64_t id;
    std::string cmd;
    std::string args;
};

struct RegisterInfo {
    std::string ip;
    int port;
    int weight;
    TimePoint timepoint;
};

class RegisterCenter {
public:
    static RegisterCenter& get_instance() {
        static RegisterCenter center;
        return center;
    }

    void start();
    void stop();
    bool check_id(uint64_t id);
    void update_id(uint64_t id);
    void add_node(uint64_t id, RegisterInfo node);
    void get_nodes(minirpc::RegisterMessage &resp);
    
private:
    ~RegisterCenter();
    RegisterCenter();
    RegisterCenter(const RegisterCenter&) = delete;
    RegisterCenter& operator=(const RegisterCenter&) = delete;

    bool running;
    TcpServer *server;
    std::unordered_map<uint64_t, RegisterInfo> register_list_;
};
#endif