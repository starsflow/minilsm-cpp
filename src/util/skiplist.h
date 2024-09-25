/*
 * @Author: Jung-Sang Ahn
 * @Date: 2024-09-16 22:48:31
 * @Url: https://github.com/greensky00/skiplist
 * @Description: 
 */
#pragma once

#include "defs.h"

#ifndef SKIPLIST_H
#define SKIPLIST_H

#define SKIPLIST_MAX_LAYER (64)

struct _skiplist_node;

//#define _STL_ATOMIC (1)
#ifdef __APPLE__
    #define _STL_ATOMIC (1)
#endif
#if defined(_STL_ATOMIC) && defined(__cplusplus)
    #include <atomic>
    typedef std::atomic<_skiplist_node*>   atm_node_ptr;
    typedef std::atomic<bool>              atm_bool;
    typedef std::atomic<uint8_t>           atm_uint8_t;
    typedef std::atomic<uint16_t>          atm_uint16_t;
    typedef std::atomic<uint32_t>          atm_uint32_t;
#else
    typedef struct _skiplist_node*         atm_node_ptr;
    typedef uint8_t                        atm_bool;
    typedef uint8_t                        atm_uint8_t;
    typedef uint16_t                       atm_uint16_t;
    typedef uint32_t                       atm_uint32_t;
#endif

extern "C" {

typedef struct _skiplist_node {
    atm_node_ptr *next;
    atm_bool is_fully_linked;
    atm_bool being_modified;
    atm_bool removed;
    uint8_t top_layer; // 0: bottom
    atm_uint16_t ref_count;
    atm_uint32_t accessing_next;
} skiplist_node;

// *a  < *b : return neg
// *a == *b : return 0
// *a  > *b : return pos
typedef int skiplist_cmp_t(skiplist_node *a, skiplist_node *b, void *aux);

typedef struct {
    size_t fanout;
    size_t maxLayer;
    void *aux;
} skiplist_raw_config;

typedef struct {
    skiplist_node head;
    skiplist_node tail;
    skiplist_cmp_t *cmp_func;
    void *aux;
    atm_uint32_t num_entries;
    atm_uint32_t* layer_entries;
    atm_uint8_t top_layer;
    uint8_t fanout;
    uint8_t max_layer;
} skiplist_raw;

#ifndef _get_entry
#define _get_entry(ELEM, STRUCT, MEMBER)                              \
        ((STRUCT *) ((uint8_t *) (ELEM) - offsetof (STRUCT, MEMBER)))
#endif

void skiplist_init(skiplist_raw* slist,
                   skiplist_cmp_t* cmp_func);
void skiplist_free(skiplist_raw* slist);

void skiplist_init_node(skiplist_node* node);
void skiplist_free_node(skiplist_node* node);

size_t skiplist_get_size(skiplist_raw* slist);

skiplist_raw_config skiplist_get_default_config();
skiplist_raw_config skiplist_get_config(skiplist_raw* slist);

void skiplist_set_config(skiplist_raw* slist,
                         skiplist_raw_config config);

int skiplist_insert(skiplist_raw* slist,
                    skiplist_node* node);
int skiplist_insert_nodup(skiplist_raw *slist,
                          skiplist_node *node);

skiplist_node* skiplist_find(skiplist_raw* slist,
                             skiplist_node* query);
skiplist_node* skiplist_find_smaller_or_equal(skiplist_raw* slist,
                                              skiplist_node* query);
skiplist_node* skiplist_find_greater_or_equal(skiplist_raw* slist,
                                              skiplist_node* query);

int skiplist_erase_node_passive(skiplist_raw* slist,
                                skiplist_node* node);
int skiplist_erase_node(skiplist_raw *slist,
                        skiplist_node *node);
int skiplist_erase(skiplist_raw* slist,
                   skiplist_node* query);

int skiplist_is_valid_node(skiplist_node* node);
int skiplist_is_safe_to_free(skiplist_node* node);
void skiplist_wait_for_free(skiplist_node* node);

void skiplist_grab_node(skiplist_node* node);
void skiplist_release_node(skiplist_node* node);

skiplist_node* skiplist_next(skiplist_raw* slist,
                             skiplist_node* node);
skiplist_node* skiplist_prev(skiplist_raw* slist,
                             skiplist_node* node);
skiplist_node* skiplist_begin(skiplist_raw* slist);
skiplist_node* skiplist_end(skiplist_raw* slist);

}
#endif 

template<typename K, typename V, typename Comp = std::less<K>>
struct map_node {
    map_node() {
        skiplist_init_node(&snode);
    }
    ~map_node() {
        skiplist_free_node(&snode);
    }
    static int cmp(skiplist_node* a, skiplist_node* b, void* aux) {
        map_node *aa, *bb;
        aa = _get_entry(a, map_node, snode);
        bb = _get_entry(b, map_node, snode);
        
        Comp cmp_func;
        return cmp_func(aa->kv.first, bb->kv.first);
        // if (aa->kv.first < bb->kv.first) return -1;
        // if (aa->kv.first == bb->kv.first) return 0;
        // return 1;
    }

    skiplist_node snode;
    std::pair<K, V> kv;
};

template<typename K, typename V, typename Comp = std::less<K>> class sl_map;

template<typename K, typename V, typename Comp = std::less<K>>
class map_iterator {
    friend class sl_map<K, V, Comp>;

private:
    using T = std::pair<K, V>;
    using Node = map_node<K, V, Comp>;

public:
    map_iterator() : slist(nullptr), cursor(nullptr) {}

    map_iterator(map_iterator&& src)
        : slist(src.slist), cursor(src.cursor)
    {
        // Mimic perfect forwarding.
        src.slist = nullptr;
        src.cursor = nullptr;
    }

    ~map_iterator() {
        if (cursor) skiplist_release_node(cursor);
    }

    void operator=(const map_iterator& src) {
        // This reference counting is similar to that of shared_ptr.
        skiplist_node* tmp = cursor;
        if (src.cursor)
            skiplist_grab_node(src.cursor);
        cursor = src.cursor;
        if (tmp)
            skiplist_release_node(tmp);
    }

    bool operator==(const map_iterator& src) const { return (cursor == src.cursor); }
    bool operator!=(const map_iterator& src) const { return !operator==(src); }

    T* operator->() const {
        Node* node = _get_entry(cursor, Node, snode);
        return &node->kv;
    }
    T& operator*() const {
        Node* node = _get_entry(cursor, Node, snode);
        return node->kv;
    }

    // ++A
    map_iterator& operator++() {
        if (!slist || !cursor) {
            cursor = nullptr;
            return *this;
        }
        skiplist_node* next = skiplist_next(slist, cursor);
        skiplist_release_node(cursor);
        cursor = next;
        return *this;
    }
    // A++
    map_iterator& operator++(int) { return operator++(); }
    // --A
    map_iterator& operator--() {
        if (!slist || !cursor) {
            cursor = nullptr;
            return *this;
        }
        skiplist_node* prev = skiplist_prev(slist, cursor);
        skiplist_release_node(cursor);
        cursor = prev;
        return *this;
    }
    // A--
    map_iterator operator--(int) { return operator--(); }

private:
    map_iterator(skiplist_raw* _slist,
                 skiplist_node* _cursor)
        : slist(_slist), cursor(_cursor) {}

    skiplist_raw* slist;
    skiplist_node* cursor;
};


template<typename K, typename V, typename Comp>
class sl_map {
private:
    using T = std::pair<K, V>;
    using Node = map_node<K, V, Comp>;

public:
    using iterator = map_iterator<K, V>;
    using reverse_iterator = map_iterator<K, V>;

    sl_map() {
        skiplist_init(&slist, Node::cmp);
    }

    virtual
    ~sl_map() {
        skiplist_node* cursor = skiplist_begin(&slist);
        while (cursor) {
            Node* node = _get_entry(cursor, Node, snode);
            cursor = skiplist_next(&slist, cursor);
            // Don't need to care about release.
            delete node;
        }
        skiplist_free(&slist);
    }

    bool empty() {
        skiplist_node* cursor = skiplist_begin(&slist);
        if (cursor) {
            skiplist_release_node(cursor);
            return false;
        }
        return true;
    }

    size_t size() { return skiplist_get_size(&slist); }

    std::pair<iterator, bool> insert(const std::pair<K, V>& kv) {
        do {
            Node* node = new Node();
            node->kv = kv;

            int rc = skiplist_insert_nodup(&slist, &node->snode);
            if (rc == 0) {
                skiplist_grab_node(&node->snode);
                return std::pair<iterator, bool>
                       ( iterator(&slist, &node->snode), true );
            }
            delete node;

            Node query;
            query.kv.first = kv.first;
            skiplist_node* cursor = skiplist_find(&slist, &query.snode);
            if (cursor) {
                return std::pair<iterator, bool>
                       ( iterator(&slist, cursor), false );
            }
        } while (true);

        // NOTE: Should not reach here.
        return std::pair<iterator, bool>(iterator(), false);
    }

    iterator find(K key) {
        Node query;
        query.kv.first = key;
        skiplist_node* cursor = skiplist_find(&slist, &query.snode);
        return iterator(&slist, cursor);
    }

    virtual
    iterator erase(iterator& position) {
        skiplist_node* cursor = position.cursor;
        skiplist_node* next = skiplist_next(&slist, cursor);

        skiplist_erase_node(&slist, cursor);
        skiplist_release_node(cursor);
        skiplist_wait_for_free(cursor);
        Node* node = _get_entry(cursor, Node, snode);
        delete node;

        position.cursor = nullptr;
        return iterator(&slist, next);
    }

    virtual
    size_t erase(const K& key) {
        size_t count = 0;
        Node query;
        query.kv.first = key;
        skiplist_node* cursor = skiplist_find(&slist, &query.snode);
        while (cursor) {
            Node* node = _get_entry(cursor, Node, snode);
            if (node->kv.first != key) break;

            cursor = skiplist_next(&slist, cursor);

            skiplist_erase_node(&slist, &node->snode);
            skiplist_release_node(&node->snode);
            skiplist_wait_for_free(&node->snode);
            delete node;
        }
        if (cursor) skiplist_release_node(cursor);
        return count;
    }

    iterator begin() {
        skiplist_node* cursor = skiplist_begin(&slist);
        return iterator(&slist, cursor);
    }
    iterator end() { return iterator(); }

    reverse_iterator rbegin() {
        skiplist_node* cursor = skiplist_end(&slist);
        return reverse_iterator(&slist, cursor);
    }
    reverse_iterator rend() { return reverse_iterator(); }

protected:
    skiplist_raw slist;
};
