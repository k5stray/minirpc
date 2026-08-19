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
#include <atomic>

#include "ring_string.h"
#include "../util/list_head.h"

const int MAX_EVENTS = 1024;
const int MAX_BUFF = 4096;

class EventLoop;

class Channel {
public:
    using Callback = std::function<void()>;

    Channel(EventLoop* loop, int fd);
    ~Channel();

    void set_event_handle(Callback func);
    void handle();

public:
    int fd_;
    uint32_t events_, revents_;
    EventLoop* loop_;
    list_head node_;
private:
    Callback callback_;
};

class EventLoop {
public:
    EventLoop();
    ~EventLoop();
    void loop();
    void add_channel(Channel * ch);
    void mod_channel(Channel * ch);
    void del_channel(Channel * ch);
    void stop();

private:
    int epfd_;
    int qfd_;
    bool looping_;
    epoll_event evs_[MAX_EVENTS];
    list_head list_;
};

class EventLoopThreadPool {
public:
    EventLoopThreadPool(int num);
    ~EventLoopThreadPool();
    EventLoop* get_next_loop(bool is_workLoop = false);
    void stop_loop();
    void set_timer(int fd, std::function<void(int)>&& handle);

private:
    EventLoop* main_loop_;
    std::vector<EventLoop*> loops_;
    std::vector<std::thread> threads_;
    int next_;
};

class TcpConnection;
using TcpConnectionPtr = std::shared_ptr<TcpConnection>;
using MessageFunc = int(*)(TcpConnectionPtr, const char*, int);
using ReadyFunc = bool(*)(const char*, int);

class Acceptor {
public:
    using NewConnectionCallback = std::function<void(Acceptor*, int)>;
    using ConnList = std::list<TcpConnectionPtr>;
    using ConnIterator = ConnList::iterator;

    Acceptor(EventLoop* loop, int port);
    ~Acceptor();

    void inline set_new_conn_callback(NewConnectionCallback cb) { new_conn_cb_ = std::move(cb); }
    void set_acceptor_channel();

    ConnIterator register_connect(TcpConnectionPtr conn);
    void unregister_connect(ConnIterator it);

    void do_accept();
    void broadcast(TcpConnectionPtr srcConn, const char* data, int len);

private:
    EventLoop* loop_;
    int listenfd_;
    Channel *ch_;
    NewConnectionCallback new_conn_cb_;
    std::list<TcpConnectionPtr> conn_list_;
    std::mutex conn_mtx_;
};

class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
public:
    TcpConnection(EventLoop* loop, int fd);
    ~TcpConnection();

    int send(const char *data, size_t size);
    int append_write(const char *data, int len);
    int erase_write(int start, int n);
    void clean_readbuf();
    void set_acceptor(Acceptor *acceptor) { this->acceptor_ = acceptor; };
    void set_conn_iterator(Acceptor::ConnIterator it) { this->it_ = it; };
    void inline set_handle_callback(MessageFunc mfunc, ReadyFunc rfunc) {
        message_cb_ = mfunc;
        ready_cb_ = rfunc;
    }
    void bind_io_channel();
    void notify_others(const char* data, int len);
    int get_fd() {return fd_;}

private:
    void on_read();
    void on_write();
    void enable_write();
    void disable_write();
    void close_conn();
    int handle_read_data();

private:
    int fd_;
    bool closed_;
    Channel *ch_;
    Acceptor *acceptor_;
    EventLoop* loop_;
    RingString read_buf_;
    RingString write_buf_;
    std::mutex write_lock_;
    Acceptor::ConnIterator it_;
    MessageFunc message_cb_;
    ReadyFunc ready_cb_;
};

class TcpServer {
public:
    TcpServer(int io_thread_num);
    ~TcpServer();

    void add_listen(int port, MessageFunc message_cb, ReadyFunc ready_cb);
    void start();
    void stop();
    void set_timer(int fd, std::function<void(int)>&& handle);
    bool connect_register_center(std::string ip, int port, MessageFunc message_cb, ReadyFunc ready_cb);
    int send_message_to_center(const char *data, int len);
    void register_done();
    bool registered();
    void set_registerid(uint64_t id);
    uint64_t get_registerid();
    
private:
    void add_connect(Acceptor *acceptor, int fd, MessageFunc mesCB, ReadyFunc readyCB);
    
    EventLoopThreadPool io_pool_;
    std::vector<std::shared_ptr<Acceptor>> acceptors_;
    bool register_ok_;
    TcpConnectionPtr register_conn_;
    std::atomic<uint64_t> register_id_;
};

#endif