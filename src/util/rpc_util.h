#ifndef RPC_UTIL_H
#define RPC_UTIL_H

#include <atomic>
#include <condition_variable>
#include <sys/timerfd.h>

inline static uint64_t get_serial_id() {
    static std::atomic<uint64_t> s_id{1000};
    return s_id.fetch_add(1, std::memory_order_relaxed);
}

static int get_timer_fd(int sec = 1, int ms = 0)
{
    int tfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    if (tfd < 0) return -1;
    struct itimerspec ts;
    ts.it_value.tv_sec = sec;
    ts.it_value.tv_nsec = (ms * 1000000LL);
    ts.it_interval = ts.it_value;
    timerfd_settime(tfd, 0, &ts, nullptr);
    return tfd;
}

#endif