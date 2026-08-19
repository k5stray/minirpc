#ifndef RPC_CODEC_H
#define RPC_CODEC_H
#include <vector>
#include <string>
#include "../rpc/rpc_conf.h"

void encode_packet(const std::string& body, std::string& out, uint32_t type);

bool decode_header(const char *data, int len, RpcHeader &header);

bool decode_packet(const char* data, int len, RpcHeader& header, std::string& body);

#endif
