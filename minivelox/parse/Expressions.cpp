#include "parse/Expressions.h"
#include "exec/Function.h"
#include <iostream>
#include <sstream>
#include <cctype>

namespace facebook::velox::parse {

// Recursive Descent Parser
class Parser {
public:
    Parser(const std::string& text) : text_(text), pos_(0) {}

    IExprPtr parse() {
        return parseExpression();
    }

private:
    std::string text_;
    size_t pos_;

    char peek() {
        while (pos_ < text_.size() && std::isspace(text_[pos_])) pos_++;
        if (pos_ >= text_.size()) return 0;
        return text_[pos_];
    }

    char advance() {
        char c = peek();
        if (pos_ < text_.size()) pos_++;
        return c;
    }

    bool match(char c) {
        if (peek() == c) {
            advance();
            return true;
        }
        return false;
    }
    
    bool matchOp(const std::string& op) {
        size_t start = pos_;
        while(start < text_.size() && std::isspace(text_[start])) start++;
        if (text_.substr(start, op.size()) == op) {
            pos_ = start + op.size();
            return true;
        }
        return false;
    }

    IExprPtr parseExpression() {
        return parseEquality();
    }
    
    IExprPtr parseEquality() {
        auto left = parseTerm();
        // Support ==
        if (matchOp("==")) {
             auto right = parseTerm();
             return std::make_shared<IFunction>("eq", std::vector<IExprPtr>{left, right});
        }
        return left;
    }

    IExprPtr parseTerm() {
        auto left = parseFactor();
        while (true) {
            if (match('+')) {
                auto right = parseFactor();
                left = std::make_shared<IFunction>("plus", std::vector<IExprPtr>{left, right});
            } else if (match('-')) {
                // minus
            } else {
                break;
            }
        }
        return left;
    }

    IExprPtr parseFactor() {
        auto left = parsePrimary();
        while (true) {
            if (match('*')) {
                auto right = parsePrimary();
                left = std::make_shared<IFunction>("multiply", std::vector<IExprPtr>{left, right});
            } else if (match('%')) {
                auto right = parsePrimary();
                left = std::make_shared<IFunction>("mod", std::vector<IExprPtr>{left, right});
            } else {
                break;
            }
        }
        return left;
    }

    IExprPtr parsePrimary() {
        char c = peek();
        if (std::isdigit(c)) {
            size_t start = pos_;
            while (pos_ < text_.size() && std::isdigit(text_[pos_])) pos_++;
            return std::make_shared<ILiteral>(text_.substr(start, pos_ - start));
        }
        if (std::isalpha(c)) {
            size_t start = pos_;
            while (pos_ < text_.size() && (std::isalnum(text_[pos_]) || text_[pos_] == '_')) pos_++;
            std::string name = text_.substr(start, pos_ - start);
            
            if (peek() == '(') {
                advance(); // eat '('
                std::vector<IExprPtr> args;
                if (peek() != ')') {
                    args.push_back(parseExpression());
                    while (match(',')) {
                        args.push_back(parseExpression());
                    }
                }
                match(')');
                return std::make_shared<IFunction>(name, std::move(args));
            }
            return std::make_shared<IIdentifier>(name);
        }
        return nullptr;
    }
};

IExprPtr DuckSqlExpressionsParser::parseExpr(const std::string& text) {
    Parser parser(text);
    return parser.parse();
}

} // namespace facebook::velox::parse

namespace facebook::velox::core {

std::shared_ptr<exec::Expr> Expressions::inferTypes(
    const std::shared_ptr<parse::IExpr>& untyped,
    const std::shared_ptr<const RowType>& rowType,
    memory::MemoryPool* pool) {
    
    if (auto lit = std::dynamic_pointer_cast<parse::ILiteral>(untyped)) {
        // Simplified inference: all numbers are BigInt unless they fit in Int?
        // Velox usually parses constants carefully.
        // Demo uses 1, 2, 3. Let's make them BigInt or Integer depending on size.
        // Let's assume Integer for small, BigInt for large.
        // Or just BigInt (safe).
        int64_t val = std::stoll(lit->value_);
        // If it was float?
        // Demo uses ints mostly.
        return std::make_shared<exec::ConstantExpr>(Variant(val));
    }
    
    if (auto id = std::dynamic_pointer_cast<parse::IIdentifier>(untyped)) {
        // Look up in rowType
        for (size_t i = 0; i < rowType->size(); ++i) {
            if (rowType->nameOf(i) == id->name_) {
                return std::make_shared<exec::FieldReference>(rowType->childAt(i), id->name_, i);
            }
        }
        throw std::runtime_error("Column not found: " + id->name_);
    }
    
    if (auto func = std::dynamic_pointer_cast<parse::IFunction>(untyped)) {
        std::vector<exec::ExprPtr> args;
        for (auto& arg : func->args_) {
            args.push_back(inferTypes(arg, rowType, pool));
        }
        
        // Output type inference?
        // Simplified: hardcode for known functions.
        // Plus/Mult/Mod -> BigInt
        // Concat/Upper/Substr -> Varchar
        // Eq -> Boolean (Integer here)
        
        TypePtr outType = BigIntType::create(); 
        if (func->name_ == "concat" || func->name_ == "upper" || func->name_ == "substr") {
            outType = VarcharType::create();
        } else if (func->name_ == "eq") {
            outType = IntegerType::create(); // As boolean placeholder
        }
        
        return std::make_shared<exec::FunctionExpr>(func->name_, args, outType);
    }
    
    throw std::runtime_error("Unknown expression type");
}

} // namespace facebook::velox::core
