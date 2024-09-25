/*
 * @Author: lxc
 * @Date: 2024-08-09 20:47:46
 * @Description: wrapper for file io operations
 */
#ifndef FILE_H
#define FILE_H

#include "defs.h"
#include "util/bytes.h"
#include <cstddef>
#include <fstream>
#include <ios>

namespace minilsm {

using std::string;
using std::fstream;
using std::ios;

class File {
private:
    string path_;
    fstream stream_;

public:
    File(const string& name, std::ios_base::openmode mod) : path_(name) {
        this->stream_.open(name, mod);
    }

    bool is_open() { return this->stream_.good(); }

    bool remove() {
        this->stream_.close();
        if (std::remove(path_.c_str())) {
            return false;
        }
        return true;
    }

    bool write(const u8* src, size_t size) {
        if (stream_.good()) {
            stream_.write(reinterpret_cast<const char*>(src), size);
            return true;
        } 
        return false;
    }

    Bytes read(size_t offset, size_t len) {
        Bytes buf;
        if (stream_.good()) {
            buf.resize(len);
            stream_.seekp(offset);
            stream_.read(reinterpret_cast<char*>(buf.data()), len);
        } 
        return buf;
    }

    auto size() { 
        stream_.seekg(0, ios::end);
        return stream_.tellg();
    }

    bool flush() {
        if (stream_.good()) {
            stream_.flush();
            return true;
        }
        return false;
    }

    bool close() {
        if (stream_.good()) {
            stream_.close();
            return true;
        }
        return false;
    }
};

}

#endif