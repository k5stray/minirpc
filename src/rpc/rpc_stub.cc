#include <thread>
#include "rpc_stub.h"
#include "../codec/rpc_codec.h"

bool RpcStub::init()
{
    inited = true;
    return cli_.init();
}

bool RpcStub::call(const minirpc::DemoReq &req, minirpc::DemoResp &resp)
{
    if (!inited) {
        LOG_ERROR_("The RPC stub has not been initialized!\n");
        return false;
    }

    Connection* conn = ConnectionPool::get_instance().get();
    if (conn == nullptr) {
        LOG_ERROR_("get connection timeout!\n");
        return false;
    }
    Waiter waiter;
    waiter.ready = false;
    conn->waiter = &waiter;

    std::string pkt, req_body;
    req.SerializeToString(&req_body);
    encode_packet(req_body, pkt, MSG_DEMO_RPC);
    int ret = conn->send_all(pkt.c_str(), pkt.size());
    if (ret < 0) {
        LOG_ERROR_("send error:%d\n", ret);
        ConnectionPool::get_instance().return_conn(conn);
        return false;
    }

    {
        std::unique_lock<std::mutex> lock(conn->wait_mtx_);
        waiter.cond.wait(lock, [conn] {return conn->waiter->ready; });
    }

    if (conn->errnum.load() < 0) {
        LOG_ERROR_("Fail to call sync, errno:%d\n", conn->errnum.load());
        conn->clean_readbuf();
        ConnectionPool::get_instance().return_conn(conn);
        return false;
    }

    RpcHeader header{};
    std::string resp_body;
    if (!decode_packet(conn->get_readbuf(), conn->get_readbuf_size(), header, resp_body)) {
        conn->clean_readbuf();
        ConnectionPool::get_instance().return_conn(conn);
        return false;
    }

    if (!resp.ParseFromString(resp_body)) {
        LOG_ERROR_("Fail to ParseFromString\n");
        conn->clean_readbuf();
        ConnectionPool::get_instance().return_conn(conn);
        return false;
    }

    conn->erase_readbuf(0, header.total_len);
    conn->waiter = nullptr;
    ConnectionPool::get_instance().return_conn(conn);
    return true;
}


bool RpcStub::call_async(const minirpc::DemoReq &req, ReadCallback callback)
{
    if (!inited) {
        LOG_ERROR_("The RPC stub has not been initialized!\n");
        return false;
    }

    std::string req_body, pkt;
    req.SerializeToString(&req_body);
    encode_packet(req_body, pkt, MSG_DEMO_RPC);
    if (ConnectionPool::get_instance().send_async(req.serialid(), std::move(pkt), std::move(callback)) <= 0)
        return false;
    return true;
}

void RpcStub::stop()
{
    cli_.deinit();
    inited = false;
}
