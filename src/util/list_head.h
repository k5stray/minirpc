#ifndef LIST_HEAD_H
#define LIST_HEAD_H

// Linux 内核双向循环链表结构
struct list_head {
    list_head *next, *prev;
};

// 初始化头节点
#define INIT_LIST_HEAD(ptr) do { \
    (ptr)->next = (ptr); \
    (ptr)->prev = (ptr); \
} while (0)

// 添加节点：new 插在 prev 和 next 之间
static inline void __list_add(list_head *new_node,
                              list_head *prev,
                              list_head *next)
{
    next->prev = new_node;
    new_node->next = next;
    new_node->prev = prev;
    prev->next = new_node;
}

// 头部添加
static inline void list_add(list_head *new_node, list_head *head)
{
    __list_add(new_node, head, head->next);
}

// 尾部添加
static inline void list_add_tail(list_head *new_node, list_head *head)
{
    __list_add(new_node, head->prev, head);
}

// 删除节点
static inline void __list_del(list_head *prev, list_head *next)
{
    next->prev = prev;
    prev->next = next;
}

// 删除节点并初始化
static inline void list_del(list_head *entry)
{
    __list_del(entry->prev, entry->next);
    entry->next = nullptr;
    entry->prev = nullptr;
}

// 判断链表是否为空
static inline int list_empty(const list_head *head)
{
    return head->next == head;
}

// #define offset_of(type, name) \
//     (reinterpret_cast<char*>(&reinterpret_cast<type*>(16)->name) - reinterpret_cast<char*>(16))

#define offset_of(type, name) \
    (reinterpret_cast<char*>(&reinterpret_cast<type*>(0)->name))

// 核心：通过成员变量地址获取结构体首地址（标准C++）
#define list_entry(ptr, type, member) \
    ((type *)((char *)(ptr) - offset_of(type, member)))

#define list_pop_entry(head, type, member) ({ \
    type *__ret = NULL; \
    if (!list_empty(head)) { \
        struct list_head *__n = (head)->next; \
        list_del(__n); \
        __ret = list_entry(__n, type, member); \
    } \
    __ret; \
})

// ===================== 【无typeof、无decltype】 =====================
// 遍历：必须手动指定类型
#define list_for_each_entry(pos, head, member, type) \
    for (pos = list_entry((head)->next, type, member); \
         &pos->member != (head) && !list_empty(head); \
         pos = list_entry(pos->member.next, type, member))

// 安全遍历：必须手动指定类型
#define list_for_each_entry_safe(pos, n, head, member, type) \
    for (pos = list_entry((head)->next, type, member), \
         n = list_entry(pos->member.next, type, member); \
         &pos->member != (head) && !list_empty(head); \
         pos = n, n = list_entry(n->member.next, type, member))

#define list_for_each_entry_debug(pos, n, head, member, type) \
    for (pos = list_entry((head)->next, type, member), n = (head)->next; \
         &pos->member != (head) && !list_empty(head); \
         pos = list_entry(pos->member.next, type, member))

#endif