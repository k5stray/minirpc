#ifndef LOG_UTIL_H
#define LOG_UTIL_H

#define LOG_UTIL_ON  1
#define LOG_UTIL_OFF 0
#define LOG_UTIL LOG_UTIL_ON

#if LOG_UTIL

#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <cstdarg>
#include <ctime>
#include <cstdio>
#include <cstring>
#include <unistd.h>

enum LogLevel
{
    LOG_TRACE = 0,
    LOG_DEBUG,
    LOG_INFO,
    LOG_WARN,
    LOG_ERROR,
    LOG_FATAL
};

constexpr int BUF_SIZE = 1024 * 1024;

class LogBuffer
{
public:
    LogBuffer() : cur_pos_(0)
    {
        memset(buf_, 0, sizeof(buf_));
    }

    bool append(const char* data, size_t len)
    {
        if (cur_pos_ + len >= BUF_SIZE)
            return false;
        memcpy(buf_ + cur_pos_, data, len);
        cur_pos_ += len;
        return true;
    }

    bool full() const
    {
        return cur_pos_ >= BUF_SIZE - 100;
    }

    void clear()
    {
        cur_pos_ = 0;
        memset(buf_, 0, sizeof(buf_));
    }

    const char* data() const
    {
        return buf_;
    }

    size_t size() const
    {
        return cur_pos_;
    }

private:
    char buf_[BUF_SIZE];
    size_t cur_pos_;
};

class AsyncLogger
{
public:
    static AsyncLogger& GetInstance();
    void init(const std::string& log_dir, LogLevel level, size_t max_file_size = 1024 * 1024 * 100);
    void stop();
    void write_log(LogLevel lev, bool pri, const char* file, int line, const char* fmt, ...);
    void write_log_non(LogLevel lev, bool pri, const char* file, int line, const char* fmt, ...);
    void flush();

private:
    AsyncLogger();
    ~AsyncLogger();
    AsyncLogger(const AsyncLogger&) = delete;
    AsyncLogger& operator=(const AsyncLogger&) = delete;

    void backend_thread_func();
    void swap_buffer();
    void flush_buffer(LogBuffer& buf);
    std::string get_log_filename();
    std::string level_to_str(LogLevel lev);

private:
    bool running_;
    LogLevel min_level_;
    std::string log_dir_;
    size_t max_file_size_;
    size_t cur_file_size_;
    FILE* log_fp_;

    std::unique_ptr<LogBuffer> curr_buf_;
    std::unique_ptr<LogBuffer> flush_buf_;

    std::mutex mtx_;
    std::condition_variable cv_;
    std::thread log_thread_;
};

static inline const char* get_filename(const char* path)
{
    const char* p = strrchr(path, '/');
    return p ? p+1 : path;
}

#ifdef __FILE_NAME__
#define LOG_FILE  __FILE_NAME__
#else
#define LOG_FILE  get_filename(__FILE__)
#endif

#define LOG_INIT(dir, log_level) AsyncLogger::GetInstance().init(dir, log_level)
#define LOG_STOP() AsyncLogger::GetInstance().stop()
#define LOG_FLUSH() AsyncLogger::GetInstance().flush()

// Output to file
#define LOG_TRACE(fmt, ...) AsyncLogger::GetInstance().write_log(LOG_TRACE, false, LOG_FILE, __LINE__, fmt, ##__VA_ARGS__)
#define LOG_DEBUG(fmt, ...) AsyncLogger::GetInstance().write_log(LOG_DEBUG, false, LOG_FILE, __LINE__, fmt, ##__VA_ARGS__)
#define LOG_INFO(fmt, ...)  AsyncLogger::GetInstance().write_log(LOG_INFO, false,  LOG_FILE, __LINE__, fmt, ##__VA_ARGS__)
#define LOG_WARN(fmt, ...)  AsyncLogger::GetInstance().write_log(LOG_WARN, false,  LOG_FILE, __LINE__, fmt, ##__VA_ARGS__)
#define LOG_ERROR(fmt, ...) AsyncLogger::GetInstance().write_log(LOG_ERROR, false, LOG_FILE, __LINE__, fmt, ##__VA_ARGS__)
#define LOG_FATAL(fmt, ...) AsyncLogger::GetInstance().write_log(LOG_FATAL, false, LOG_FILE, __LINE__, fmt, ##__VA_ARGS__)
#define LOG_NONE(fmt, ...) AsyncLogger::GetInstance().write_log_non(LOG_FATAL, false, LOG_FILE, __LINE__, fmt, ##__VA_ARGS__)

// Output to file and screen
#define LOG_TRACE_(fmt, ...) AsyncLogger::GetInstance().write_log(LOG_TRACE, true, LOG_FILE, __LINE__, fmt, ##__VA_ARGS__)
#define LOG_DEBUG_(fmt, ...) AsyncLogger::GetInstance().write_log(LOG_DEBUG, true, LOG_FILE, __LINE__, fmt, ##__VA_ARGS__)
#define LOG_INFO_(fmt, ...)  AsyncLogger::GetInstance().write_log(LOG_INFO, true,  LOG_FILE, __LINE__, fmt, ##__VA_ARGS__)
#define LOG_WARN_(fmt, ...)  AsyncLogger::GetInstance().write_log(LOG_WARN, true,  LOG_FILE, __LINE__, fmt, ##__VA_ARGS__)
#define LOG_ERROR_(fmt, ...) AsyncLogger::GetInstance().write_log(LOG_ERROR, true, LOG_FILE, __LINE__, fmt, ##__VA_ARGS__)
#define LOG_FATAL_(fmt, ...) AsyncLogger::GetInstance().write_log(LOG_FATAL, true, LOG_FILE, __LINE__, fmt, ##__VA_ARGS__)
#define LOG_NONE_(fmt, ...) AsyncLogger::GetInstance().write_log_non(LOG_FATAL, true, LOG_FILE, __LINE__, fmt, ##__VA_ARGS__)

#else /* LOG_UTIL */

#define LOG_INIT(dir, log_level)
#define LOG_STOP()
#define LOG_FLUSH()

#define LOG_TRACE(fmt, ...)
#define LOG_DEBUG(fmt, ...)
#define LOG_INFO(fmt, ...)  printf("INFO:" fmt, ##__VA_ARGS__)
#define LOG_WARN(fmt, ...)  printf("WARN:" fmt, ##__VA_ARGS__)
#define LOG_ERROR(fmt, ...) printf("ERROR:" fmt, ##__VA_ARGS__)
#define LOG_FATAL(fmt, ...) printf("FATAL:" fmt, ##__VA_ARGS__)
#define LOG_NONE(fmt, ...)  printf(fmt, ##__VA_ARGS__)

#define LOG_TRACE_(fmt, ...)
#define LOG_DEBUG_(fmt, ...)
#define LOG_INFO_(fmt, ...)  printf("INFO:" fmt, ##__VA_ARGS__)
#define LOG_WARN_(fmt, ...)  printf("WARN:" fmt, ##__VA_ARGS__)
#define LOG_ERROR_(fmt, ...) printf("ERROR:" fmt, ##__VA_ARGS__)
#define LOG_FATAL_(fmt, ...) printf("FATAL:" fmt, ##__VA_ARGS__)
#define LOG_NONE_(fmt, ...)  printf(fmt, ##__VA_ARGS__)

#endif /* LOG_UTIL */

#endif /* LOG_UTIL_H */
