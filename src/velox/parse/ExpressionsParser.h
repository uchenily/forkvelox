#pragma once
#include "velox/core/Expressions.h"
#include "velox/type/Type.h"
#include <algorithm>
#include <cctype>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace facebook::velox::parse {

enum class TokenType {
  END,
  IDENTIFIER,
  NUMBER,
  PLUS,
  MINUS,
  MUL,
  DIV,
  MOD,
  LPAREN,
  RPAREN,
  COMMA,
  EQ,
  NEQ,
  LT,
  GT,
  LTE,
  GTE,
  ARROW
};

struct Token {
  TokenType type;
  std::string text;
};

class Lexer {
public:
  Lexer(const std::string &input) : input_(input), pos_(0) {}

  Token next() {
    skipWhitespace();
    if (pos_ >= input_.size())
      return {TokenType::END, ""};

    char c = input_[pos_];

    if (std::isdigit(c)) {
      size_t start = pos_;
      while (pos_ < input_.size() && std::isdigit(input_[pos_]))
        pos_++;
      return {TokenType::NUMBER, input_.substr(start, pos_ - start)};
    }

    if (std::isalpha(c) || c == '_') {
      size_t start = pos_;
      while (pos_ < input_.size() &&
             (std::isalnum(input_[pos_]) || input_[pos_] == '_'))
        pos_++;
      return {TokenType::IDENTIFIER, input_.substr(start, pos_ - start)};
    }

    pos_++;
    switch (c) {
    case '+':
      return {TokenType::PLUS, "+"};
    case '-':
      if (pos_ < input_.size() && input_[pos_] == '>') {
        pos_++;
        return {TokenType::ARROW, "->"};
      }
      return {TokenType::MINUS, "-"};
    case '*':
      return {TokenType::MUL, "*"};
    case '/':
      return {TokenType::DIV, "/"};
    case '%':
      return {TokenType::MOD, "%"};
    case '(':
      return {TokenType::LPAREN, "("};
    case ')':
      return {TokenType::RPAREN, ")"};
    case ',':
      return {TokenType::COMMA, ","};
    case '=':
      if (pos_ < input_.size() && input_[pos_] == '=') {
        pos_++;
        return {TokenType::EQ, "=="};
      }
      throw std::runtime_error("Unexpected char: =");
    case '!':
      if (pos_ < input_.size() && input_[pos_] == '=') {
        pos_++;
        return {TokenType::NEQ, "!="};
      }
      throw std::runtime_error("Unexpected char: !");
    case '<':
      if (pos_ < input_.size() && input_[pos_] == '=') {
        pos_++;
        return {TokenType::LTE, "<="};
      }
      return {TokenType::LT, "<"};
    case '>':
      if (pos_ < input_.size() && input_[pos_] == '=') {
        pos_++;
        return {TokenType::GTE, ">="};
      }
      return {TokenType::GT, ">"};
    }

    throw std::runtime_error(std::string("Unexpected char: ") + c);
  }

private:
  void skipWhitespace() {
    while (pos_ < input_.size() && std::isspace(input_[pos_]))
      pos_++;
  }

  std::string input_;
  size_t pos_;
};

class DuckSqlExpressionsParser {
public:
  core::TypedExprPtr parseExpr(const std::string &text) {
    Lexer lexer(text);
    tokens_.clear();
    Token t;
    do {
      t = lexer.next();
      tokens_.push_back(t);
    } while (t.type != TokenType::END);

    pos_ = 0;
    return parseExpression();
  }

private:
  std::vector<Token> tokens_;
  size_t pos_;

  Token current() {
    if (pos_ < tokens_.size())
      return tokens_[pos_];
    return {TokenType::END, ""};
  }

  Token consume() {
    Token t = current();
    if (pos_ < tokens_.size())
      pos_++;
    return t;
  }

  bool match(TokenType type) {
    if (current().type == type) {
      pos_++;
      return true;
    }
    return false;
  }

  void expect(TokenType type) {
    if (!match(type))
      throw std::runtime_error("Unexpected token");
  }

  core::TypedExprPtr parseExpression() { return parseComparison(); }

  core::TypedExprPtr parseComparison() {
    auto left = parseAdditive();
    while (true) {
      std::string opName;
      if (match(TokenType::EQ))
        opName = "eq";
      else if (match(TokenType::NEQ))
        opName = "neq";
      else if (match(TokenType::LT))
        opName = "lt";
      else if (match(TokenType::GT))
        opName = "gt";
      else if (match(TokenType::LTE))
        opName = "lte";
      else if (match(TokenType::GTE))
        opName = "gte";
      else
        break;

      auto right = parseAdditive();
      left = std::make_shared<core::CallTypedExpr>(
          UNKNOWN(), std::vector<core::TypedExprPtr>{left, right}, opName);
    }
    return left;
  }

  core::TypedExprPtr parseAdditive() {
    auto left = parseMultiplicative();
    while (true) {
      std::string opName;
      if (match(TokenType::PLUS))
        opName = "plus";
      else if (match(TokenType::MINUS))
        opName = "minus";
      else
        break;

      auto right = parseMultiplicative();
      left = std::make_shared<core::CallTypedExpr>(
          UNKNOWN(), std::vector<core::TypedExprPtr>{left, right}, opName);
    }
    return left;
  }

  core::TypedExprPtr parseMultiplicative() {
    auto left = parsePrimary();
    while (true) {
      std::string opName;
      if (match(TokenType::MUL))
        opName = "multiply";
      else if (match(TokenType::DIV))
        opName = "divide";
      else if (match(TokenType::MOD))
        opName = "mod";
      else
        break;

      auto right = parsePrimary();
      left = std::make_shared<core::CallTypedExpr>(
          UNKNOWN(), std::vector<core::TypedExprPtr>{left, right}, opName);
    }
    return left;
  }

  bool isLambdaParameterList() {
    if (current().type != TokenType::LPAREN) {
      return false;
    }
    size_t i = pos_ + 1;
    if (i >= tokens_.size() || tokens_[i].type != TokenType::IDENTIFIER) {
      return false;
    }
    ++i;
    while (i < tokens_.size() && tokens_[i].type == TokenType::COMMA) {
      ++i;
      if (i >= tokens_.size() || tokens_[i].type != TokenType::IDENTIFIER) {
        return false;
      }
      ++i;
    }
    if (i >= tokens_.size() || tokens_[i].type != TokenType::RPAREN) {
      return false;
    }
    ++i;
    return i < tokens_.size() && tokens_[i].type == TokenType::ARROW;
  }

  core::TypedExprPtr parseLambdaAfterParams(std::vector<std::string> params) {
    expect(TokenType::ARROW);
    auto body = parseExpression();
    std::vector<TypePtr> paramTypes(params.size(), UNKNOWN());
    auto signature = ROW(std::move(params), std::move(paramTypes));
    return std::make_shared<core::LambdaTypedExpr>(signature, body);
  }

  core::TypedExprPtr parsePrimary() {
    if (current().type == TokenType::IDENTIFIER) {
      std::string name = consume().text;
      if (match(TokenType::ARROW)) {
        std::vector<std::string> params{name};
        auto body = parseExpression();
        std::vector<TypePtr> paramTypes(params.size(), UNKNOWN());
        auto signature = ROW(std::move(params), std::move(paramTypes));
        return std::make_shared<core::LambdaTypedExpr>(signature, body);
      }
      if (match(TokenType::LPAREN)) {
        // Function call
        std::vector<core::TypedExprPtr> args;
        if (current().type != TokenType::RPAREN) {
          args.push_back(parseExpression());
          while (match(TokenType::COMMA)) {
            args.push_back(parseExpression());
          }
        }
        expect(TokenType::RPAREN);
        return std::make_shared<core::CallTypedExpr>(UNKNOWN(), args, name);
      }
      // Field access
      return std::make_shared<core::FieldAccessTypedExpr>(UNKNOWN(), name);
    }

    if (current().type == TokenType::LPAREN) {
      if (isLambdaParameterList()) {
        expect(TokenType::LPAREN);
        std::vector<std::string> params;
        while (current().type == TokenType::IDENTIFIER) {
          params.push_back(consume().text);
          if (!match(TokenType::COMMA)) {
            break;
          }
        }
        expect(TokenType::RPAREN);
        return parseLambdaAfterParams(std::move(params));
      }
      expect(TokenType::LPAREN);
      auto expr = parseExpression();
      expect(TokenType::RPAREN);
      return expr;
    }

    if (current().type == TokenType::NUMBER) {
      int64_t val = std::stoll(consume().text);
      return std::make_shared<core::ConstantTypedExpr>(Variant(val));
    }

    throw std::runtime_error("Unexpected token in primary: " + current().text);
  }
};

inline void registerTypeResolver() {}

} // namespace facebook::velox::parse
