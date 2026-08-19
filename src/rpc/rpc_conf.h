#ifndef RPC_INIT_H
#define RPC_INIT_H

#include <cstdint>

#define REGISTER_CENTER_IPADDR                 "127.0.0.1"
#define REGISTER_CENTER_PORT                   6666

#define TCPCLIENT_CONN_POOL_SIZE              10
#define TCPCLIENT_CONN_POLL_MAX_RE_ADD        10
#define TCPCLIENT_SEND_TIMEOUT                2000

enum MessageType {
    MSG_REGISTER = 0,
    MSG_DEMO_RPC,
};

struct RpcHeader
{
    uint32_t total_len;
    uint32_t type;
    static const int HEADER_LEN = 8;
};

#endif