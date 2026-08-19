#include <cstdio>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/eventfd.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <errno.h>
#include <iostream>


#include "tcp_server.h"
#include "../util/log_util.h"

static int setnonblock(int fd)
{
    int old = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, old | O_NONBLOCK);
    return old;
}

static bool setThreadAffinity(int cpu_id)
{
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu_id, &cpuset);
    int ret = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    if (ret != 0) {
        LOG_ERROR_("affinity failed: %s\n", strerror(errno));
        return false;
    }
    return true;
}

// ====================== Channel Start ====================== //
Channel::Channel(EventLoop* loop, int fd) : loop_(loop), fd_(fd), events_(0), revents_(0) {}

void Channel::set_event_handle(Callback func)
{
    callback_ = std::move(func);
}

void Channel::handle()
{
    if (callback_)
        callback_();
}

Channel::~Channel()
{
    close(fd_);
}
// ====================== Channel End ====================== //

// ====================== EventLoop Start ====================== //
EventLoop::EventLoop() : looping_(false), epfd_(epoll_create1(0))
{
    INIT_LIST_HEAD(&list_);
    qfd_ = eventfd(0, EFD_NONBLOCK);
    if (qfd_ == -1) {
        LOG_ERROR_("Create eventfd fail!\n");
        return;
    }
    Channel *ch = new Channel(this, qfd_);
    ch->events_ = EPOLLIN;
    ch->set_event_handle([this, ch] {
        if (ch->revents_ & EPOLLIN) {
            uint64_t val;
            read(qfd_, &val, sizeof(val));
            if (val == 1) {
                LOG_INFO_("Quit epoll_wait! epfd:%d\n", epfd_);
                looping_ = false;
            }
        }
    });
    add_channel(ch);
}

void EventLoop::loop()
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
            Channel *ch = static_cast<Channel*>(evs_[i].data.ptr);
            if (evs_[i].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
                del_channel(ch);
                continue;
            }
            ch->revents_ = evs_[i].events;
            ch->handle();
        }
    }
}

void EventLoop::add_channel(Channel * ch)
{
    epoll_event e{};
    e.events = ch->events_;
    e.data.ptr = ch;
    if (::epoll_ctl(epfd_, EPOLL_CTL_ADD, ch->fd_, &e) == -1) {
        LOG_ERROR_("epoll_ctl add error: %d\n", errno);
        return;
    }

    list_add_tail(&ch->node_, &list_);
}

void EventLoop::mod_channel(Channel * ch)
{
    epoll_event e{};
    e.events = ch->events_;
    e.data.ptr = ch;
    if (::epoll_ctl(epfd_, EPOLL_CTL_MOD, ch->fd_, &e) == -1) {
        LOG_ERROR_("epoll_ctl mod error: %d\n", errno);
    }
}

void EventLoop::del_channel(Channel * ch)
{
    if (::epoll_ctl(epfd_, EPOLL_CTL_DEL, ch->fd_, nullptr) == -1) {
        LOG_ERROR_("epoll_ctl del error: %d\n", errno);
        return;
    }
    list_del(&ch->node_);
    delete ch;
}

void EventLoop::stop()
{
    uint64_t u = 1;
    write(qfd_, &u, sizeof(u));
}

EventLoop::~EventLoop()
{
    Channel *pos, *n;
    list_for_each_entry_safe(pos, n, &list_, node_, Channel) {
        del_channel(pos);
    }
    close(epfd_);
}
// ====================== EventLoop End ====================== //

// ====================== EventLoopThreadPool Start ====================== //
EventLoopThreadPool::EventLoopThreadPool(int num) : main_loop_(new EventLoop()), next_(0)
{
    for (int i = 0; i < num; ++i) {
        auto loop = new EventLoop();
        loops_.push_back(loop);
        threads_.emplace_back([loop, i] {
            setThreadAffinity(i + 1);
            loop->loop();
        });
    }
}

EventLoop* EventLoopThreadPool::get_next_loop(bool is_workLoop)
{
    if (!is_workLoop || loops_.size() == 0) return main_loop_;
    auto* loop = loops_[next_];
    next_ = (next_ + 1) % loops_.size();
    return loop;
}

void EventLoopThreadPool::stop_loop()
{
    for (const auto& loop : loops_) {
        loop->stop();
    }
    main_loop_->stop();
}

void EventLoopThreadPool::set_timer(int fd, std::function<void(int)>&& handle)
{
    if (fd < 0) {
        LOG_ERROR_("Invalid fd:%d\n", fd);
        return;
    }
    EventLoop *loop = get_next_loop(false);
    Channel *ch = new Channel(loop, fd);
    ch->events_ = EPOLLIN | EPOLLET;
    ch->set_event_handle([ch, handle](){
        if (ch->revents_ & EPOLLIN) {
            uint64_t val;
            read(ch->fd_, &val, sizeof(val));
            handle(val);
        }
    });
    loop->add_channel(ch);
}

EventLoopThreadPool::~EventLoopThreadPool()
{
    for (auto &t : threads_) {
        t.join();
    }

    for (auto loop : loops_) {
        delete loop;
    }
    delete main_loop_;
}
// ====================== EventLoopThreadPool End ====================== //

// ====================== Acceptor Start ====================== //
Acceptor::Acceptor(EventLoop* loop, int port) : loop_(loop), listenfd_(-1)
{
    listenfd_ = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (listenfd_ == -1) {
        LOG_FATAL_("socket error: %d\n", errno);
        return;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    int opt = 1;
    if (setsockopt(listenfd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1) {
        LOG_ERROR_("setsockopt error: %d\n", errno);
        close(listenfd_);
        return;
    }
    if (setsockopt(listenfd_, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)) == -1) {
        LOG_ERROR_("setsockopt error: %d\n", errno);
        close(listenfd_);
        return;
    }

    if (::bind(listenfd_, (sockaddr *)&addr, sizeof(addr)) == -1) {
        LOG_FATAL_("bind error: %d\n", errno);
        close(listenfd_);
        return;
    }

    if (::listen(listenfd_, SOMAXCONN) == -1) {
        LOG_FATAL_("listen error: %d\n", errno);
        close(listenfd_);
        return;
    }
    LOG_INFO_("server start on port: %d listenfd:%d\n", port, listenfd_);
}

void Acceptor::set_acceptor_channel()
{
    ch_ = new Channel(loop_, listenfd_);
    ch_->events_ = EPOLLIN;
    ch_->set_event_handle([this] {
        if (ch_->revents_ & EPOLLIN) {
            do_accept();
        }
    });
    loop_->add_channel(ch_);
}

void Acceptor::do_accept()
{
    while (true) {
        sockaddr_in addr{};
        socklen_t len = sizeof(addr);
        int fd = accept4(listenfd_, (sockaddr*)&addr, &len, SOCK_NONBLOCK);
        if (fd <= 0) {
            break;
        }
        if (new_conn_cb_) new_conn_cb_(this, fd);
    }
}

Acceptor::ConnIterator Acceptor::register_connect(TcpConnectionPtr conn)
{
    std::lock_guard<std::mutex> lock(conn_mtx_);
    conn_list_.emplace_back(conn);
    return --conn_list_.end();
}

void Acceptor::unregister_connect(ConnIterator it)
{
    std::lock_guard<std::mutex> lock(conn_mtx_);
    conn_list_.erase(it);
}

void Acceptor::broadcast(TcpConnectionPtr srcConn, const char* data, int len)
{
    std::lock_guard<std::mutex> lock(conn_mtx_);
    for (auto& conn : conn_list_) {
        if (conn != srcConn) {
            conn->append_write(data, len);
        }
    }
}

Acceptor::~Acceptor()
{
}
// ====================== Acceptor End ====================== //

// ====================== TcpConnection Start ====================== //
TcpConnection::TcpConnection(EventLoop* loop, int fd) : closed_(false), loop_(loop), fd_(fd)
{}
    
TcpConnection::~TcpConnection()
{
}
    
void TcpConnection::bind_io_channel()
{
    ch_ = new Channel(loop_, fd_);
    ch_->events_ = EPOLLIN | EPOLLRDHUP;
    ch_->set_event_handle([this] {
        if (ch_->revents_ & EPOLLIN) {
            on_read();
        }
        if (ch_->revents_ & EPOLLOUT) {
            on_write();
        }
    });
    loop_->add_channel(ch_);
}

int TcpConnection::send(const char *data, size_t size)
{
    if (closed_ || data == nullptr || size == 0) {
        LOG_ERROR_("send Invalid argement!\n");
        return -1;
    }

    if (!write_buf_.empty()) {
        LOG_WARN("send write buf not empty!\n");
        append_write(data, size);
        return size;
    }

    int n = ::send(fd_, data, size, MSG_NOSIGNAL);
    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            append_write(data, size);
        } else {
            LOG_ERROR_("send fail,errno:%d\n", errno);
            close_conn();
        }
    } else {
        erase_write(0, n);
        if (n < size) {
            append_write(data + n, size - n);
        }
    }
    return n;
}

int TcpConnection::append_write(const char *data, int len)
{
    std::lock_guard<std::mutex> lock(write_lock_);
    write_buf_.append(data, len);
    enable_write();
    return 0;
}

int TcpConnection::erase_write(int start, int n)
{
    std::lock_guard<std::mutex> lock(write_lock_);
    write_buf_.erase(start, n);
    return 0;
}

void TcpConnection::notify_others(const char* data, int len)
{
    acceptor_->broadcast(shared_from_this(), data, len);
}

int TcpConnection::handle_read_data()
{
    if (message_cb_) {
        if (!ready_cb_ || ready_cb_(read_buf_.data(), read_buf_.size())) {
            return message_cb_(shared_from_this(), read_buf_.data(), read_buf_.size());
        }
    } else {
        return read_buf_.size();
    }
    return 0;
}

void TcpConnection::on_read()
{
    char buf[MAX_BUFF] = {0};
    ssize_t n = ::recv(fd_, buf, MAX_BUFF, 0);
    if (n > 0) {
        read_buf_.append(buf, n);
        int ret = handle_read_data();
        if (ret > 0) {
            read_buf_.erase(0, ret);
        }
    } else if (n == 0 || (n < 0 && (errno != EAGAIN && errno != EWOULDBLOCK))) {
        close_conn();
    }
}

void TcpConnection::on_write()
{
    if (closed_ || write_buf_.size() == 0) {
        LOG_ERROR_("on_write Invalid argement!\n");
        
        return;
    }

    int n = ::send(fd_, write_buf_.data(), write_buf_.size(), MSG_NOSIGNAL);
    if (n > 0) {
        erase_write(0, n);
        if (write_buf_.empty()) {
            disable_write();
        }
    } else if (n < 0 && (errno != EAGAIN && errno != EWOULDBLOCK)) {
        close_conn();
    }
}

void TcpConnection::enable_write()
{
    ch_->events_ = EPOLLIN | EPOLLOUT | EPOLLRDHUP;
    loop_->mod_channel(ch_);
}

void TcpConnection::disable_write()
{
    ch_->events_ = EPOLLIN | EPOLLRDHUP;
    loop_->mod_channel(ch_);
}

void TcpConnection::close_conn()
{
    closed_ = true;
    loop_->del_channel(ch_);
    acceptor_->unregister_connect(it_);
}

void TcpConnection::clean_readbuf()
{
    read_buf_.clean();
}
// ====================== TcpConnection End ====================== //

// ====================== TcpServer Start ====================== //
TcpServer::TcpServer(int ioThreads) : io_pool_(ioThreads), register_id_(0), register_ok_(false) {}

void TcpServer::add_listen(int port, MessageFunc mesCB, ReadyFunc readyCB)
{
    auto acceptor = std::make_shared<Acceptor>(io_pool_.get_next_loop(), port);
    acceptor->set_new_conn_callback([this, mesCB, readyCB](Acceptor *acceptor, int fd) {
        add_connect(acceptor, fd, mesCB, readyCB);
    });
    acceptors_.push_back(acceptor);
    acceptor->set_acceptor_channel();
}

void TcpServer::start()
{
    io_pool_.get_next_loop()->loop();
}

void TcpServer::stop()
{
    io_pool_.stop_loop();
}

TcpServer::~TcpServer()
{
}

void TcpServer::add_connect(Acceptor *acceptor, int fd, MessageFunc mesCB, ReadyFunc readyCB)
{
    EventLoop* ioLoop = io_pool_.get_next_loop(1);
    auto conn = std::make_shared<TcpConnection>(ioLoop, fd);

    conn->set_acceptor(acceptor);
    conn->set_conn_iterator(acceptor->register_connect(conn));
    conn->set_handle_callback(mesCB, readyCB);
    conn->bind_io_channel();
}

void TcpServer::set_timer(int fd, std::function<void(int)>&& handle)
{
    io_pool_.set_timer(fd, std::move(handle));
}

bool TcpServer::connect_register_center(std::string ip, int port, MessageFunc message_cb, ReadyFunc ready_cb)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        LOG_FATAL_("Fail to create socket!\b");
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

    if (connect(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        LOG_FATAL_("Fail to connect register center! errno:%d\n", errno);
        close(fd);
        return false;
    }

    EventLoop* ioLoop = io_pool_.get_next_loop(0);
    register_conn_ = std::make_shared<TcpConnection>(ioLoop, fd);
    register_conn_->set_handle_callback(message_cb, ready_cb);
    register_conn_->bind_io_channel();
    return true;
}

int TcpServer::send_message_to_center(const char *data, int len)
{
    return register_conn_->send(data, len);
}

void TcpServer::register_done()
{
    register_ok_ = true;
}

bool TcpServer::registered()
{
    return register_ok_;
}

void TcpServer::set_registerid(uint64_t id)
{
    register_id_.store(id);
}

uint64_t TcpServer::get_registerid()
{
    return register_id_.load();
}
// ====================== TcpServer end ====================== //
