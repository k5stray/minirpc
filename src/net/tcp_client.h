#ifndef NET_TCP_SERVER_H_
#define NET_TCP_SERVER_H_

#include <thread>
#include <functional>
#include <mutex>
#include <list>
#include <memory>
#include <vector>
#include <sys/epoll.h>
#include <sched.h>
#include <unistd.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <condition_variable>
#include <unordered_map>
#include <queue>

#include "ring_string.h"
#include "../util/list_head.h"
#include "../../proto/rpc_proto.pb.h"
#include "../util/log_util.h"

// using namespace std;

const int MAX_EVENTS = 1024;
const int MAX_BUFF = 4096;

using ReadCallback = std::function<int(minirpc::DemoResp resp)>;
struct ReqID {
    uint64_t s_id;
    ReadCallback callback;
};

struct Request {
    ReqID id;
    std::string buf;
};

struct Waiter
{
    std::condition_variable cond;
    bool ready;
};

struct ServerNode {
    std::string ip;
    int port;
    int weight;
    int nums;
};

class Connection;
class EventLoopConn {
public:
    EventLoopConn();
    ~EventLoopConn();
    void loop();
    void add_connection(Connection * conn);
    void mod_connection(Connection * conn);
    void del_connection(Connection * conn);
    void stop();

private:
    int epfd_;
    int qfd_;
    bool looping_;
    epoll_event evs_[MAX_EVENTS];
    list_head list_work_;
};

class EventLoopConnPool {
public:
    EventLoopConnPool(int num);
    ~EventLoopConnPool();
    EventLoopConn* get_next_loop();
    void stop_loop();

private:
    std::vector<EventLoopConn*> loops_;
    std::vector<std::thread> threads_;
    int next_;
};

class ConnectionPool;
class Connection {
public:
    using Callback = std::function<void()>;
    using Clock = std::chrono::steady_clock;
    using TimePoint = std::chrono::time_point<Clock>;

    void handle();
    void set_loop(EventLoopConn* loop) {loop_ = loop;};
    void set_event_handle(Callback&& callback);
    int send_all(const char *data, size_t size);
    int recv_all();
    void on_read();
    void enable_read();
    void clean_readbuf();
    const char* get_readbuf();
    uint64_t get_readbuf_size();
    void erase_readbuf(uint32_t, uint32_t len);
    void on_write();
    void enable_write();
    void disable_write();
    int append_write(const char *data, int len);
    ~Connection() {
        if (fd_ > 0)
            close(fd_);
    }
public:
    friend class EventLoopConn;
    friend class ConnectionPool;
    int send_async(Request &&req);
    Waiter *waiter;
    std::mutex wait_mtx_;
    std::atomic<int> errnum;

private:
    int fd_ = -1;
    std::string ip_;
    uint16_t port_ = 0;
    RingString write_buf_;
    RingString read_buf_;
    std::mutex write_lock_;
    EventLoopConn* loop_;
    uint32_t events_, revents_;
    list_head node_;
    Callback event_cb_;
    std::unordered_map<uint64_t, ReqID> req_map_;
    TimePoint send_time_;
};

class ConnectionPool {
public:
    static ConnectionPool& get_instance() {
        static ConnectionPool ins;
        return ins;
    }

    void init(const std::string& ip, uint16_t port, int conns);
    void init(std::vector<ServerNode> &node_list);
    void deinit();
    Connection* get(int seconds = 20);
    void return_conn(Connection *conn);
    Connection* get_async();
    void return_conn_async(Connection *conn);
    int send_async(uint64_t s_id, std::string &&req_data, ReadCallback &&callback);
    void check_timeout_conn();
    list_head list_work_;
    list_head list_idle_;

private:
    ConnectionPool();
    ~ConnectionPool();
    ConnectionPool(const ConnectionPool&) = delete;
    ConnectionPool& operator=(const ConnectionPool&) = delete;

    void replace_async_conn(Connection *conn);
    Connection* create_conn(const std::string& ip, uint16_t port);
    void handle_timeout_conn(Connection *conn);
    
    std::mutex pool_mtx_;
    uint8_t reset_conn_;
    std::condition_variable pool_ok_;
    EventLoopConnPool* loop_pool_;
    std::queue<Request> req_queue_;
    std::mutex req_mtx_;
};

class TcpClient {
public:
    TcpClient();
    ~TcpClient();

    bool init();
    void deinit();
};

#endif