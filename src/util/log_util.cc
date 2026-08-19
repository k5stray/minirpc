#include "log_util.h"
#include <sys/stat.h>

#if LOG_UTIL

AsyncLogger& AsyncLogger::GetInstance()
{
    static AsyncLogger ins;
    return ins;
}

AsyncLogger::AsyncLogger()
    : running_(false), min_level_(LOG_INFO), max_file_size_(0), cur_file_size_(0), log_fp_(nullptr),
    curr_buf_(new LogBuffer()), flush_buf_(new LogBuffer())
{}

AsyncLogger::~AsyncLogger()
{
    stop();
}

void AsyncLogger::init(const std::string& log_dir, LogLevel level, size_t max_file_size)
{
    log_dir_ = log_dir;
    min_level_ = level;
    max_file_size_ = max_file_size;

    mkdir(log_dir_.c_str(), 0755);
    std::string name = get_log_filename();
    log_fp_ = fopen(name.c_str(), "a");
    if (!log_fp_) return;

    running_ = true;
    log_thread_ = std::thread(&AsyncLogger::backend_thread_func, this);
}

void AsyncLogger::stop()
{
    if (!running_) return;
    running_ = false;
    cv_.notify_one();
    if (log_thread_.joinable())
        log_thread_.join();

    flush_buffer(*curr_buf_);
    if (log_fp_)
    {
        fflush(log_fp_);
        fclose(log_fp_);
        log_fp_ = nullptr;
    }
}

static void get_fast_timestamp(char* buf, size_t len, bool with_date = false)
{
    static thread_local char cached_date[20];
    static thread_local std::time_t last_sec = 0;

    auto now = std::chrono::system_clock::now();
    auto time_t_now = std::chrono::system_clock::to_time_t(now);

    if (time_t_now != last_sec) {
        std::tm tm_buf;
        localtime_r(&time_t_now, &tm_buf);
        if (with_date) {
            strftime(cached_date, sizeof(cached_date), "%Y-%m-%d %H:%M:%S", &tm_buf);
        } else {
            strftime(cached_date, sizeof(cached_date), "%H:%M:%S", &tm_buf);
        }
        last_sec = time_t_now;
    }

    snprintf(buf, len, "%s", cached_date);
}

void AsyncLogger::write_log(LogLevel lev, bool pri, const char* file, int line, const char* fmt, ...)
{
    if (!running_ || lev < min_level_) return;

    char content_buf[512] = {0};
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(content_buf, sizeof(content_buf), fmt, ap);
    va_end(ap);

    char log_head[256] = {0};
    char time_str[32] = {0};
    get_fast_timestamp(time_str, 32);
    std::string lev_str = level_to_str(lev);

    snprintf(log_head, sizeof(log_head),
             "[%s][%s][%s:%d] ",
             time_str, lev_str.c_str(), file, line);

    char final_log[1024] = {0};
    snprintf(final_log, sizeof(final_log), "%s%s", log_head, content_buf);
    if (pri) {
        std::cout << final_log;
    }

    std::lock_guard<std::mutex> lock(mtx_);
    if (!curr_buf_->append(final_log, strlen(final_log))) {
        std::cerr << "Error: log buf overflow!" << std::endl;
    }

    if (curr_buf_->full())
        swap_buffer();
}

void AsyncLogger::write_log_non(LogLevel lev, bool pri, const char* file, int line, const char* fmt, ...)
{
    if (!running_ || lev < min_level_) return;

    char content_buf[512] = {0};
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(content_buf, sizeof(content_buf), fmt, ap);
    va_end(ap);

    char final_log[1024] = {0};
    snprintf(final_log, sizeof(final_log), "%s", content_buf);
    if (pri) {
        std::cout << final_log;
    }

    std::lock_guard<std::mutex> lock(mtx_);
    if (!curr_buf_->append(final_log, strlen(final_log))) {
        std::cerr << "Error: log buf over flow!" << std::endl;
    }

    if (curr_buf_->full())
        swap_buffer();
}

void AsyncLogger::swap_buffer()
{
    std::swap(curr_buf_, flush_buf_);
    cv_.notify_one();
}

void AsyncLogger::flush()
{
    std::lock_guard<std::mutex> lock(mtx_);
    swap_buffer();
}

void AsyncLogger::backend_thread_func()
{
    while (running_)
    {
        std::unique_lock<std::mutex> lock(mtx_);
        cv_.wait_for(lock, std::chrono::milliseconds(50), [this](){
            return flush_buf_->size() > 0 || !running_;
        });

        if (flush_buf_->size() > 0)
        {
            flush_buffer(*flush_buf_);
            flush_buf_->clear();
        }
    }
}

void AsyncLogger::flush_buffer(LogBuffer& buf)
{
    if (!log_fp_ || buf.size() == 0) return;

    size_t write_len = buf.size();
    cur_file_size_ += write_len;

    if (cur_file_size_ >= max_file_size_)
    {
        fclose(log_fp_);
        std::string new_name = get_log_filename();
        log_fp_ = fopen(new_name.c_str(), "a");
        cur_file_size_ = 0;
    }

    fwrite(buf.data(), 1, write_len, log_fp_);
    fflush(log_fp_);
}

std::string AsyncLogger::get_log_filename()
{
    time_t now = time(nullptr);
    struct tm tm_now = *localtime(&now);
    char tmp[128] = {0};
    snprintf(tmp, sizeof(tmp), "%s/log_%04d%02d%02d_%02d%02d%02d.log",
             log_dir_.c_str(),
             tm_now.tm_year+1900, tm_now.tm_mon+1, tm_now.tm_mday,
             tm_now.tm_hour, tm_now.tm_min, tm_now.tm_sec);
    return std::string(tmp);
}

std::string AsyncLogger::level_to_str(LogLevel lev)
{
    switch (lev)
    {
        case LOG_TRACE: return "TRACE";
        case LOG_DEBUG: return "DEBUG";
        case LOG_INFO:  return "INFO";
        case LOG_WARN:  return "WARN";
        case LOG_ERROR: return "ERROR";
        case LOG_FATAL: return "FATAL";
        default: return "UNKOWN";
    }
}

#endif /* LOG_UTIL */