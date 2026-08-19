#include <cstdio>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/eventfd.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <errno.h>
#include <iostream>
#include <atomic>

#include "tcp_client.h"
#include "../rpc/rpc_conf.h"
#include "../util/rpc_util.h"
#include "../codec/rpc_codec.h"

// ====================== EventLoopConn Start ====================== //
EventLoopConn::EventLoopConn() : looping_(false), epfd_(epoll_create1(0))
{
    INIT_LIST_HEAD(&list_work_);
    qfd_ = eventfd(0, EFD_NONBLOCK);
    if (qfd_ == -1) {
        LOG_ERROR_("Create eventfd fail!\n");
        return;
    }
    Connection *conn = new Connection();
    conn->events_ = EPOLLIN;
    conn->fd_ = qfd_;
    conn->set_event_handle([this, conn] {
        if (conn->revents_ & EPOLLIN) {
            uint64_t val;
            read(conn->fd_, &val, sizeof(val));
            if (val == 1) {
                LOG_INFO_("Quit epoll_wait! epfd:%d\n", epfd_);
                looping_ = false;
            }
        }
    });
    add_connection(conn);
    list_add(&conn->node_, &list_work_);

    int timerfd = get_timer_fd(1, 0);
    if (timerfd < 0) {
        LOG_ERROR_("fail to get timerfd\n");
        return;
    }
    conn = new Connection();
    conn->events_ = EPOLLIN;
    conn->fd_ = timerfd;
    conn->set_event_handle([this, conn] {
        if (conn->revents_ & EPOLLIN) {
            uint64_t val;
            read(conn->fd_, &val, sizeof(val));
            if (val >= 0) {
                ConnectionPool::get_instance().check_timeout_conn();
            }
        }
    });
    add_connection(conn);
    list_add(&conn->node_, &list_work_);
}

void EventLoopConn::loop()
{
    looping_ = true;
    while (looping_) {
        int n = epoll_wait(epfd_, evs_, MAX_EVENTS, -1);
        if (n == -1) {
            if (errno == EINTR)
                continue;
            LOG_ERROR_("epoll_wait error: %d\n", errno);
            return;
        }
        for (int i = 0; i < n; ++i) {
            Connection *conn = static_cast<Connection*>(evs_[i].data.ptr);
            if (evs_[i].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
                del_connection(conn);
                continue;
            }
            conn->revents_ = evs_[i].events;
            conn->handle();
        }
    }
}

void EventLoopConn::add_connection(Connection * conn)
{
    epoll_event e{};
    e.events = conn->events_;
    e.data.ptr = conn;
    if (::epoll_ctl(epfd_, EPOLL_CTL_ADD, conn->fd_, &e) == -1) {
        LOG_ERROR_("epoll_ctl add error: %d\n", errno);
        return;
    }
}

void EventLoopConn::mod_connection(Connection *conn)
{
    epoll_event e{};
    e.events = conn->events_;
    e.data.ptr = conn;
    if (::epoll_ctl(epfd_, EPOLL_CTL_MOD, conn->fd_, &e) == -1) {
        LOG_ERROR_("epoll_ctl mod error: %d\n", errno);
    }
}

void EventLoopConn::del_connection(Connection *conn)
{
    conn->events_ = 0;
    if (::epoll_ctl(epfd_, EPOLL_CTL_DEL, conn->fd_, nullptr) == -1) {
        LOG_ERROR_("epoll_ctl del error: %d", errno);
        return;
    }
}

void EventLoopConn::stop()
{
    uint64_t u = 1;
    write(qfd_, &u, sizeof(u));
}

EventLoopConn::~EventLoopConn()
{
    Connection *pos, *n;
    list_for_each_entry_safe(pos, n, &list_work_, node_, Connection) {
        del_connection(pos);
        delete pos;
    }
    close(epfd_);
}
// ====================== EventLoopConn End ====================== //

// ====================== EventLoopConn End ====================== //
EventLoopConnPool::EventLoopConnPool(int num) : next_(0)
{
    for (int i = 0; i < num; ++i) {
        auto loop = new EventLoopConn();
        loops_.push_back(loop);
        threads_.emplace_back([loop] {
            loop->loop();
        });
    }
}

EventLoopConn* EventLoopConnPool::get_next_loop()
{
    auto* loop = loops_[next_];
    next_ = (next_ + 1) % loops_.size();
    return loop;
}

void EventLoopConnPool::stop_loop()
{
    for (const auto &loop : loops_) {
        loop->stop();
    }
}

EventLoopConnPool::~EventLoopConnPool()
{
    for (auto &t : threads_) {
        t.join();
    }

    for (auto loop : loops_) {
        delete loop;
    }
}

// ====================== ConnectionPool start ==================== //
// Connection::Connection():events_(0){}

int Connection::send_all(const char *data, size_t size)
{
    if (data == nullptr || size == 0) {
        LOG_ERROR_("Invalid argement to send!\n");
        return -22;
    }

    this->send_time_ = Clock::now();
    int n = ::send(fd_, data, size, MSG_NOSIGNAL);
    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            append_write(data, size);
        } else {
            LOG_ERROR_("send fail,errno:%d\n", errno);
            return (0 - errno);
        }
    } else {
        if (n == 0) {
            LOG_ERROR_("+++error: send 0 bytes!\n");
        }
        if (n < size) {
            append_write(data + n, size - n);
        } else if (n == size) {
            enable_read();
        }
    }
    return size;
}

int Connection::send_async(Request &&req)
{
    const char *data = req.buf.data();
    int size = req.buf.size();
    if (data == nullptr || size == 0) {
        LOG_ERROR_("send Invalid argement!\n");
        return -22;
    }

    req_map_[req.id.s_id] = req.id;

    if (!write_buf_.empty()) {
        LOG_ERROR_("send write_buf_ not empty!\n");
        append_write(data, size);
        return size;
    }

    this->send_time_ = Clock::now();
    int n = ::send(fd_, data, size, MSG_NOSIGNAL);
    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            append_write(data, size);
        } else {
            LOG_ERROR_("send fail,errno:%d\n", errno);
            return (0 - errno);
        }
    } else {
        if (n < size) {
            append_write(data + n, size - n);
        } else if (n == size) {
            enable_read();
        }
    }
    return size;
}

int Connection::recv_all()
{
    int n;
    char buf[1024] = {0};
    while (true) {
        n = ::recv(fd_, buf, 1024, 0);
        if (n > 0) {
            read_buf_.append(buf, n);
        } else if (n == 0) {
            LOG_ERROR_("peer disconnect, fd:%d\n", fd_);
            return (0 - errno);
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            break;
        }
    }

    return 0;
}

void Connection::clean_readbuf()
{
    read_buf_.clean();
}

void Connection::erase_readbuf(uint32_t, uint32_t len)
{
    read_buf_.erase(0, len);
}

const char* Connection::get_readbuf()
{
    return read_buf_.data();
}

uint64_t Connection::get_readbuf_size()
{
    return read_buf_.size();
}

int Connection::append_write(const char *data, int len) {
    std::lock_guard<std::mutex> lock(write_lock_);
    write_buf_.append(data, len);
    enable_write();
    return 0;
}

void Connection::enable_write() {
    if (!(this->events_ & EPOLLOUT)) {
        this->events_ = EPOLLIN | EPOLLET | EPOLLOUT | EPOLLRDHUP;
        loop_->mod_connection(this);
    }
}

void Connection::disable_write() {
    this->events_ = EPOLLIN | EPOLLET | EPOLLRDHUP;
    loop_->mod_connection(this);
}

void Connection::enable_read()
{
    if (!(this->events_ & EPOLLIN)) {
        this->events_ = EPOLLIN | EPOLLET | EPOLLRDHUP;
        loop_->mod_connection(this);
    }
}

void Connection::set_event_handle(Callback&& callback)
{
    event_cb_ = std::move(callback);
}

void Connection::handle()
{
    if (event_cb_)
        return event_cb_();
    if (this->events_ & EPOLLOUT)
        return on_write();
    if (this->events_ & EPOLLIN) {
        on_read();
    }
}

void Connection::on_read()
{
    int ret = recv_all();
    if (ret == 0) {
        RpcHeader header{};
        if (decode_header(read_buf_.data(), read_buf_.size(), header) && header.total_len >= read_buf_.size()) {

            if (this->waiter != nullptr) {
                std::lock_guard<std::mutex> lock(wait_mtx_);
                this->waiter->ready = true;
                this->waiter->cond.notify_one();
                return;
            }

            if (!req_map_.empty()) {
                std::string resp_body;
                minirpc::DemoResp resp;
                if (!decode_packet(read_buf_.data(), read_buf_.size(), header, resp_body)) {
                    LOG_ERROR_("decode error!\n");
                }

                if (!resp.ParseFromString(resp_body)) {
                    LOG_ERROR_("Error:async pares, id:%lu\n", resp.serialid());
                }

                if (req_map_.count(resp.serialid()) > 0) {
                    auto tmp = req_map_[resp.serialid()];
                    tmp.callback(resp);
                    req_map_.erase(resp.serialid());
                }
                read_buf_.erase(0, header.total_len);
                if (req_map_.empty()) {
                    ConnectionPool::get_instance().return_conn_async(this);
                }
                return;
            }

            LOG_ERROR_("Error:unexpect request! conn:%p waiter:%p empty:%d\n",
                    this, this->waiter, req_map_.empty());
            read_buf_.clean();
        }
    }
}

void Connection::on_write()
{
    if (write_buf_.size() == 0) {
        LOG_ERROR_("on_write Invalid argement!\n");
        return;
    }

    this->send_time_ = Clock::now();
    int n = ::send(fd_, write_buf_.data(), write_buf_.size(), MSG_NOSIGNAL);
    if (n > 0) {
        write_buf_.erase(0, n);
        if (write_buf_.empty()) {
            disable_write();
        }
    } else if (n < 0 && (errno != EAGAIN && errno != EWOULDBLOCK)) {
        LOG_ERROR_("Connection write err:%d\n", errno);
    }
}
// ====================== ConnectionPool start ==================== //


// ====================== ConnectionPool start ==================== //
ConnectionPool::ConnectionPool() : loop_pool_(nullptr) {}

ConnectionPool::~ConnectionPool()
{
    delete loop_pool_;
}

void ConnectionPool::init(const std::string& ip, uint16_t port, int conns)
{
    auto lp = new EventLoopConnPool(1);
    loop_pool_ = lp;

    std::lock_guard<std::mutex> lock(pool_mtx_);
    INIT_LIST_HEAD(&list_work_);
    INIT_LIST_HEAD(&list_idle_);
    for (int i = 0; i < conns; ++i) {
        Connection* conn = create_conn(ip, port);
        if (conn) {
            EventLoopConn *lpp = lp->get_next_loop();
            list_add(&conn->node_, &list_idle_);
            conn->set_loop(lpp);
            lpp->add_connection(conn);
        }
    }
    reset_conn_ = 0;
}

void ConnectionPool::init(std::vector<ServerNode> &node_list)
{
    auto lp = new EventLoopConnPool(1);
    loop_pool_ = lp;

    std::lock_guard<std::mutex> lock(pool_mtx_);
    INIT_LIST_HEAD(&list_work_);
    INIT_LIST_HEAD(&list_idle_);
    int size = node_list.size();
    for (int i = 0; i < size; ++i) {
        int conns = node_list[i].nums;
        for (int j = 0; j < conns; j++) {
            Connection* conn = create_conn(node_list[i].ip, node_list[i].port);
            if (conn) {
                EventLoopConn *lpp = lp->get_next_loop();
                list_add(&conn->node_, &list_idle_);
                conn->set_loop(lpp);
                lpp->add_connection(conn);
            }
        }
    }

    reset_conn_ = 0;
}

void ConnectionPool::deinit()
{
    loop_pool_->stop_loop();

    Connection *pos, *n;
    list_for_each_entry_safe(pos, n, &list_work_, node_, Connection) {
        pos->loop_->del_connection(pos);
        delete pos;
    }
    list_for_each_entry_safe(pos, n, &list_idle_, node_, Connection) {
        pos->loop_->del_connection(pos);
        delete pos;
    }
}

void ConnectionPool::handle_timeout_conn(Connection *conn)
{
    if (conn->waiter != nullptr) {
        std::lock_guard<std::mutex> lock(conn->wait_mtx_);
        conn->waiter->ready = true;
        conn->waiter->cond.notify_one();
        return;
    }

    if (!conn->req_map_.empty()) {
        std::string resp_body;
        minirpc::DemoResp resp;
        
        for (const auto& elem: conn->req_map_) {
            resp.set_serialid(elem.first);
            resp.set_errnum(conn->errnum.load());
            elem.second.callback(resp);
        }

        ConnectionPool::get_instance().return_conn_async(conn);
        return;
    }

    LOG_ERROR_("Error:unexpect Connection!\n");
}

void ConnectionPool::check_timeout_conn()
{
    list_head timeout_list;
    INIT_LIST_HEAD(&timeout_list);
    Connection *pos, *n;
    {
        std::lock_guard<std::mutex> lock(pool_mtx_);
        list_for_each_entry_safe(pos, n, &list_work_, node_, Connection) {
            uint64_t sec = (uint64_t)std::chrono::duration<double>
                            (Connection::Clock::now() - pos->send_time_).count();
            if (sec > TCPCLIENT_SEND_TIMEOUT) {
                pos->errnum.store(-110);
                list_del(&pos->node_);
                list_add(&pos->node_, &timeout_list);
            }
        }
    }

    list_for_each_entry_safe(pos, n, &timeout_list, node_, Connection) {
        handle_timeout_conn(pos);
    }
}

Connection* ConnectionPool::get(int seconds)
{
    std::unique_lock<std::mutex> lock(pool_mtx_);
    pool_ok_.wait_for(lock, std::chrono::seconds(seconds), [this] {return !list_empty(&list_idle_); });
    if (list_empty(&list_idle_)) {
        return nullptr;
    }
    Connection* conn = list_pop_entry(&list_idle_, Connection, node_);
    list_add(&conn->node_, &list_work_);
    return conn;
}

void ConnectionPool::return_conn(Connection *conn)
{
    static int count = 0;
    if (conn->errnum.load() < 0) {
        conn->loop_->del_connection(conn);
        if (this->reset_conn_ < TCPCLIENT_CONN_POLL_MAX_RE_ADD) {
            EventLoopConn *lp = loop_pool_->get_next_loop();
            Connection *tmp = create_conn(conn->ip_, conn->port_);
            if (tmp) {
                tmp->set_loop(lp);
                lp->add_connection(tmp);

                std::lock_guard<std::mutex> lock(pool_mtx_);
                list_add(&tmp->node_, &list_idle_);
                pool_ok_.notify_one();
                reset_conn_++;
            }
        }
        delete conn;
        return;
    }
    std::lock_guard<std::mutex> lock(pool_mtx_);
    list_del(&conn->node_);
    list_add(&conn->node_, &list_idle_);

    pool_ok_.notify_one();
}

Connection* ConnectionPool::get_async()
{
    Connection* conn;
    std::lock_guard<std::mutex> lock(pool_mtx_);
    if (list_empty(&list_idle_)) {
        return nullptr;
    }

    conn = list_pop_entry(&list_idle_, Connection, node_);
    list_add(&conn->node_, &list_work_);
    return conn;
}

void ConnectionPool::replace_async_conn(Connection *conn)
{
    if (this->reset_conn_ < TCPCLIENT_CONN_POLL_MAX_RE_ADD) {
        EventLoopConn *lp = loop_pool_->get_next_loop();
        Connection *tmp = create_conn(conn->ip_, conn->port_);
        if (tmp) {
            tmp->set_loop(lp);
            lp->add_connection(tmp);

            Request req;
            bool run_async = false;
            {
                std::lock_guard<std::mutex> mtx(req_mtx_);
                if (!req_queue_.empty()) {
                    req = req_queue_.front();
                    req_queue_.pop();
                    run_async = true;
                }
            }

            if (run_async) {
                {
                    std::lock_guard<std::mutex> lock(pool_mtx_);
                    list_add(&tmp->node_, &list_work_);
                }
                tmp->send_async(std::move(req));
            } else {
                std::lock_guard<std::mutex> lock(pool_mtx_);
                list_add(&tmp->node_, &list_idle_);
                pool_ok_.notify_one();
            }
        }
    }
    conn->loop_->del_connection(conn);
    delete conn;
}

void ConnectionPool::return_conn_async(Connection *conn)
{
    if (conn->errnum.load() < 0) {
        LOG_WARN("async call errno:%d\n", conn->errnum.load());
        return replace_async_conn(conn);
    }

    {
        std::lock_guard<std::mutex> mtx(req_mtx_);
        if (!req_queue_.empty()) {
            Request req;
            req = req_queue_.front();
            req_queue_.pop();
            conn->send_async(std::move(req));
            return;
        }
    }
    std::lock_guard<std::mutex> lock(pool_mtx_);
    list_del(&conn->node_);
    list_add(&conn->node_, &list_idle_);
    pool_ok_.notify_one();
}

Connection* ConnectionPool::create_conn(const std::string& ip, uint16_t port)
{
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (fd < 0) return nullptr;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, ip.c_str(), &addr.sin_addr);

    if (connect(fd, (sockaddr*)&addr, sizeof(addr)) < 0 && errno != EINPROGRESS) {
        LOG_ERROR_("connect err, errno:%d\n", errno);
        close(fd);
        return nullptr;
    }

    Connection* c = new Connection();
    c->fd_ = fd;
    c->ip_ = ip;
    c->port_ = port;
    c->waiter = nullptr;
    return c;
}

int ConnectionPool::send_async(uint64_t s_id, std::string &&req_data, ReadCallback &&callback)
{
    Request req = {
        .id = {
           .s_id = s_id,
           .callback = std::move(callback),
        },
        .buf = std::move(req_data),
    };

    Connection* conn = get_async();
    if (conn == nullptr) {
        std::lock_guard<std::mutex> mtx(req_mtx_);
        req_queue_.push(std::move(req));
        return req_data.size();
    }

    int ret = conn->send_async(std::move(req));
    if (ret <= 0) {
        return ret;
    }

    return req_data.size();
}
// ====================== ConnectionPool End ====================== //

// ====================== TcpClient end Start ====================== //
TcpClient::TcpClient() {}

static void count_weight(std::vector<ServerNode> &node_list, int weights, int total)
{
    int count = 0;
    int size = node_list.size();
    for (int i = 0; i < size; i++) {
        double per = (double)(node_list[i].weight * total) / weights;
        if (per > 0 && per < 1) {
            node_list[i].nums = 1;
        } else {
            node_list[i].nums = (int)per;
        }
        count += node_list[i].nums;
    }
    if (count < total) {
        node_list[size - 1].weight += (total - count);
    } else if (count > total) {
        node_list[size - 1].weight -= (count - total);
    }
}

static bool get_server_nodes(std::string ip, int port, std::vector<ServerNode> &node_list)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        LOG_FATAL_("Fail to create socket!\n");
        return false;
    }

    struct timeval timeout;
    timeout.tv_sec = 5;
    timeout.tv_usec = 0;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (char *)&timeout, sizeof(timeout));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, (char *)&timeout, sizeof(timeout));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, ip.c_str(), &addr.sin_addr);

    if (connect(fd, (sockaddr*)&addr, sizeof(addr)) < 0 && errno != EINPROGRESS) {
        LOG_FATAL_("Fail to connect register center! client errno:%d\n", errno);
        close(fd);
        return false;
    }

    minirpc::RegisterMessage req;
    req.set_cmd("GET_SERVER_LIST");

    std::string resp_body, send_pkt;
    req.SerializeToString(&resp_body);
    encode_packet(resp_body, send_pkt, MSG_REGISTER);

    int n = send(fd, send_pkt.data(), send_pkt.size(), 0);
    if (n < 0) {
        LOG_FATAL_("Send to register center fail errno:%d\n", errno);
        return false;
    }

    char buf[1024] = {0};
    n = recv(fd, buf, 1024, 0);
    if (n < 0) {
        LOG_FATAL_("Recv from register center fail errno:%d\n", errno);
        close(fd);
        return false;
    }
    close(fd);

    std::string body;
    RpcHeader header;
    if (!decode_packet(buf, n, header, body)) {
        LOG_FATAL_("decodec fail! total:%d len:%d\n", header.total_len, n);
        return false;
    }

    int weights = 0;
    minirpc::RegisterMessage resp;
    resp.ParseFromString(body);
    int size = resp.iplist_size();
    if (size == 0) {
        LOG_FATAL_("No server nodes available!\n");
        return false;
    }
    for (int i = 0; i < size; i++) {
        const minirpc::IPElem &elem = resp.iplist(i);
        ServerNode node;
        node.ip = elem.ip();
        node.port = elem.port();
        node.weight = elem.weight();
        weights += node.weight;
        node_list.push_back(node);
    }

    count_weight(node_list, weights, TCPCLIENT_CONN_POOL_SIZE);

    return true;
}

bool TcpClient::init()
{
    std::vector<ServerNode> node_list;
    if (!get_server_nodes(REGISTER_CENTER_IPADDR, REGISTER_CENTER_PORT, node_list)) {
        LOG_FATAL_("Fail to get Server Node list!\n");
        return false;
    }

    ConnectionPool::get_instance().init(node_list);
    return true;
}
void TcpClient::deinit()
{
    ConnectionPool::get_instance().deinit();
}
TcpClient::~TcpClient()
{
}

// ====================== TcpClient end ====================== //
