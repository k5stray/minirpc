#include "rpc_codec.h"
#include <arpa/inet.h>
#include <cstring>

void encode_packet(const std::string& body, std::string& out, uint32_t type)
{
    RpcHeader h{};
    uint32_t total_len = RpcHeader::HEADER_LEN + body.size();
    h.total_len = htonl(total_len);
    h.type = htonl(type);

    out.assign((char*)&h, RpcHeader::HEADER_LEN);
    out += body;
}

bool decode_header(const char *data, int len, RpcHeader &header)
{
    if (len < RpcHeader::HEADER_LEN)
        return false;

    memcpy(&header, data, RpcHeader::HEADER_LEN);
    header.total_len = ntohl(header.total_len);
    header.type = ntohl(header.type);
    return true;
}

bool decode_packet(const char* data, int len, RpcHeader& header, std::string& body)
{
    if (!decode_header(data, len, header) || header.total_len > len)
        return false;
    body.append(data + RpcHeader::HEADER_LEN, header.total_len - RpcHeader::HEADER_LEN);
    return true;
}

