#ifndef NONCOPYABLE_H
#define NONCOPYABLE_H

class Noncopyable
{
protected:
    Noncopyable() = default;
    ~Noncopyable() = default;
private:
    Noncopyable(const Noncopyable&) = delete;
    Noncopyable& operator=(const Noncopyable&) = delete;
};

#endif
