#pragma once

#include "velox/vector/BaseVector.h"
#include "velox/vector/SelectivityVector.h"

namespace facebook::velox {

namespace exec {
class EvalCtx;
} // namespace exec

class Callable {
public:
    virtual ~Callable() = default;

    virtual void apply(
        vector_size_t topLevelRow,
        const SelectivityVector& rows,
        exec::EvalCtx& context,
        const std::vector<VectorPtr>& args,
        VectorPtr& result) = 0;
};

class FunctionVector : public BaseVector {
public:
    class Iterator {
    public:
        struct Entry {
            Callable* callable;
            SelectivityVector* rows;

            explicit operator bool() const { return callable != nullptr; }
        };

        Iterator(const FunctionVector* vector, const SelectivityVector* rows)
            : rows_(*rows),
              functions_(vector->functions_),
              rowSets_(vector->rowSets_),
              effectiveRows_(rows->size(), false) {}

        Entry next() {
            while (index_ < functions_.size()) {
                effectiveRows_ = rowSets_[index_];
                effectiveRows_.intersect(rows_);
                if (!effectiveRows_.hasSelections()) {
                    ++index_;
                    continue;
                }
                Entry entry{functions_[index_].get(), &effectiveRows_};
                ++index_;
                return entry;
            }
            return {nullptr, nullptr};
        }

    private:
        const SelectivityVector& rows_;
        const std::vector<std::shared_ptr<Callable>>& functions_;
        const std::vector<SelectivityVector>& rowSets_;
        size_t index_ = 0;
        SelectivityVector effectiveRows_;
    };

    FunctionVector(memory::MemoryPool* pool, TypePtr type, vector_size_t size)
        : BaseVector(pool, std::move(type), VectorEncoding::Simple::FUNCTION, BufferPtr(nullptr), size) {}

    void addFunction(std::shared_ptr<Callable> callable, const SelectivityVector& rows) {
        for (const auto& otherRows : rowSets_) {
            VELOX_CHECK(
                !otherRows.intersects(rows),
                "Functions in a FunctionVector may not have intersecting SelectivityVectors");
        }
        rowSets_.push_back(rows);
        functions_.push_back(std::move(callable));
    }

    Iterator iterator(const SelectivityVector* rows) const {
        return Iterator(this, rows);
    }

    std::string toString(vector_size_t index) const override {
        return std::string("<lambda@") + std::to_string(index) + ">";
    }

    int32_t compare(const BaseVector* /*other*/, vector_size_t /*index*/, vector_size_t /*otherIndex*/) const override {
        VELOX_NYI("compare not supported for FunctionVector");
    }

    void copy(const BaseVector* /*source*/, vector_size_t /*sourceIndex*/, vector_size_t /*targetIndex*/) override {
        VELOX_NYI("copy not supported for FunctionVector");
    }

private:
    std::vector<std::shared_ptr<Callable>> functions_;
    std::vector<SelectivityVector> rowSets_;
};

}
