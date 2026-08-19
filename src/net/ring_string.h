#ifndef NET_RING_STRING_H_
#define NET_RING_STRING_H_

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <utility>
#include <new>

#define STR_MIN_LEN 32
class RingString {
public:
    RingString() : r_ptr_(0), w_ptr_(0), str_(short_str_), back_(str_ + STR_MIN_LEN), capacity_(STR_MIN_LEN) {
    }
    RingString(uint32_t size);
    RingString(const RingString&) = delete;
    RingString& operator=(const RingString&) = delete;
    ~RingString() {
        if (str_ != nullptr && str_ != short_str_) {
            delete[] str_;
        }
    }

    int append(const char *src, uint32_t len);
    std::pair<const char*, int> get_data();
    void erase(uint32_t, uint32_t len);
    int shrink();

    uint64_t size() {
        return buf_len();
    }
    uint64_t length() {
        return size();
    }
    bool empty() {
        return (buf_len() == 0);
    }
    const char* data() {
        return get_data().first;
    }
    void clean() {
        w_ptr_ = r_ptr_ = 0;
    }
private:
    inline int min(uint32_t a, uint32_t b) {
        return (a > b ? b : a);
    }
    inline uint32_t buf_left() {
        return (capacity_ - (w_ptr_ - r_ptr_));
    }
    inline uint32_t buf_len() {
        return (w_ptr_ - r_ptr_);
    }
    inline uint32_t get_offset(uint32_t ptr) {
        return (ptr & (capacity_ - 1));
    }

    uint32_t roundup_power_of_two(uint32_t size);
    int resize(uint32_t len);
private:
    uint32_t capacity_;
    uint64_t r_ptr_;
    uint64_t w_ptr_;
    char *str_;
    char *back_;
    char short_str_[64] = {0};
};

#endif