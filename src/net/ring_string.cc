#include "ring_string.h"
#include "../util/log_util.h"

#define RING_BUFFER_MAX_SIZE (32*1024*1024)

RingString::RingString(uint32_t size) : RingString() {
    if (size <= STR_MIN_LEN) {
        return;
    }
    capacity_ = roundup_power_of_two(size);
    if (capacity_ * 2 > RING_BUFFER_MAX_SIZE) {
        capacity_ = RING_BUFFER_MAX_SIZE / 2;
        LOG_ERROR_("Capacity is out of MAX_SIZE:%d, set to MAX_SIZE\n", RING_BUFFER_MAX_SIZE);
    }
    str_ = new char[capacity_ * 2]();
    if (str_ == nullptr) {
        throw std::bad_alloc();
    }
    back_ = str_ + capacity_;
}

int RingString::append(const char *src, uint32_t len) {
    if (buf_left() < len && buf_len() + len > RING_BUFFER_MAX_SIZE) {
        LOG_ERROR_("buf is out of MAX SIZE!\n");
        return 0;
    }
    if (buf_len() + len > capacity_) {
        resize(buf_len() + len);
    }
    int first = min(len, capacity_ - get_offset(w_ptr_));
    memcpy(str_ + get_offset(w_ptr_), src, first);
    memcpy(str_, src + first, len - first);
    w_ptr_ += len;
    return len;
}

std::pair<const char*, int> RingString::get_data() {
    if (buf_len() == 0) {
        return {nullptr, 0};
    }

    if (get_offset(r_ptr_) >= get_offset(w_ptr_)) {
        uint32_t first = capacity_ - get_offset(r_ptr_);
        memcpy(back_, str_ + get_offset(r_ptr_), first);
        memcpy(back_ + first, str_, get_offset(w_ptr_));
        if (buf_len() < capacity_)
            back_[buf_len()] = '\0';
        return {back_, buf_len()};
    }
    if (get_offset(w_ptr_) != get_offset(r_ptr_))
        str_[get_offset(w_ptr_)] = '\0';
    return {str_ + get_offset(r_ptr_), buf_len()};
}

void RingString::erase(uint32_t, uint32_t len) {
    int sz = len;

    if (buf_len() == 0)
        return;

    if (len > buf_len())
        sz = buf_len();
    r_ptr_ += sz; 
}

int RingString::shrink() {
    int sz = roundup_power_of_two(buf_len());
    if (sz == capacity_) {
        return capacity_;
    }
    if (str_ == short_str_) {
        return STR_MIN_LEN;
    }
    if (sz <= STR_MIN_LEN) {
        int buf_size = buf_len();
        int first = min(buf_size, capacity_ - get_offset(r_ptr_));
        memcpy(short_str_, str_ + get_offset(r_ptr_), first);
        memcpy(short_str_ + first, str_, buf_size - first);
        delete[] str_;
        str_ = short_str_;
        back_ = str_ + STR_MIN_LEN;
        capacity_ = STR_MIN_LEN;
        r_ptr_ = 0;
        w_ptr_ = buf_size;
        return STR_MIN_LEN;
    }
    return resize(buf_len());
}

uint32_t RingString::roundup_power_of_two(uint32_t size) {
    int cap = size, i = 0;
    if (cap == 0) return 2;
    for (; cap != 0; i++)
        cap >>= 1;
    if (1U << i == size << 1)
        return size;
    return 1U << i;
}

int RingString::resize(uint32_t len) {
    int size = roundup_power_of_two(len);
    char *tmp = new char[size << 1]();
    if (tmp == nullptr) {
        throw std::bad_alloc();
    }
    int buf_size = buf_len();
    int first = min(buf_size, capacity_ - get_offset(r_ptr_));
    memcpy(tmp, str_ + get_offset(r_ptr_), first);
    memcpy(tmp + first, str_, buf_size - first);
    if (str_ != short_str_) {
        delete[] str_;
    }
    str_ = tmp;
    back_ = str_ + size;
    capacity_ = size;
    r_ptr_ = 0;
    w_ptr_ = buf_size;
    return size;
}
