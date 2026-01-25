#pragma once

#include <cstring>
#include <string>
#include <string_view>
#include <iostream>
#include <algorithm>

namespace facebook::velox {

class StringView {
public:
    static constexpr uint32_t kInlineSize = 12;

    StringView() : size_(0) {
        std::memset(prefix_, 0, 4);
        value_.ptr = nullptr; 
    }

    StringView(const char* data, int32_t len) : size_(len) {
        if (len <= (int32_t)kInlineSize) {
             std::memcpy(prefix_, data, len);
             // Zero pad the rest of prefix and inline part
             if (len < 4) {
                 std::memset(prefix_ + len, 0, 4 - len);
                 std::memset(value_.inline_, 0, 8);
             } else {
                 // prefix full
                 int32_t remaining = len - 4;
                 if (remaining > 0) std::memcpy(value_.inline_, data + 4, remaining);
                 if (remaining < 8) std::memset(value_.inline_ + remaining, 0, 8 - remaining);
             }
        } else {
             std::memcpy(prefix_, data, 4);
             value_.ptr = data;
        }
    }

    StringView(const std::string& str) : StringView(str.data(), (int32_t)str.size()) {}
    StringView(const char* data) : StringView(data, std::strlen(data)) {}

    int32_t size() const { return size_; }
    
    const char* data() const {
        if (isInline()) {
            return prefix_;
        }
        return value_.ptr;
    }

    bool isInline() const { return size_ <= (int32_t)kInlineSize; }

    std::string str() const { return std::string(data(), size_); }
    operator std::string() const { return str(); }
    
    bool operator==(const StringView& other) const {
        if (size_ != other.size_) return false;
        if (size_ == 0) return true;
        // Compare prefix (first 4 bytes or size)
        int32_t cmpLen = std::min(size_, (uint32_t)4);
        if (std::memcmp(prefix_, other.prefix_, cmpLen) != 0) return false;
        
        if (isInline()) {
             // If inline and prefix matched, check remaining 8 bytes
             // Actually, simplest is just memcmp the whole data reconstructed
             // But let's optimize slightly:
             if (size_ <= 4) return true; // prefix covered it
             return std::memcmp(value_.inline_, other.value_.inline_, size_ - 4) == 0;
        }
        
        // Not inline, compare full buffer
        return std::memcmp(data(), other.data(), size_) == 0;
    }
    
    friend std::ostream& operator<<(std::ostream& os, const StringView& sv) {
        os.write(sv.data(), sv.size());
        return os;
    }

private:
    uint32_t size_;
    char prefix_[4];
    union {
        char inline_[8];
        const char* ptr;
    } value_;
};

} // namespace facebook::velox
