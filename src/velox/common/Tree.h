#pragma once

#include <algorithm>
#include <cstddef>
#include <memory>
#include <numeric>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace facebook::velox::common {

struct Point {
  std::size_t x;
  std::size_t y;
};

struct Tree {
  std::string label;
  std::vector<std::shared_ptr<Tree>> children;

  explicit Tree(std::string label) : label{std::move(label)} {}
};

struct DrawableTree {
  static constexpr std::size_t kMargin = 1;

  using DrawableTreePtr = std::shared_ptr<DrawableTree>;
  using Canvas = std::vector<std::vector<char>>;

  std::size_t width;
  std::size_t height;
  std::size_t centerX;
  std::size_t overallWidth;
  std::size_t overallHeight;
  std::string label;
  std::size_t childrenLeftOffset;
  std::vector<DrawableTreePtr> children;

  static DrawableTreePtr fromTree(const std::shared_ptr<Tree> &root) {
    std::vector<DrawableTreePtr> drawableChildren;
    drawableChildren.reserve(root->children.size());
    for (const auto &child : root->children) {
      drawableChildren.emplace_back(fromTree(child));
    }

    std::size_t childrenWidth = 0;
    if (!drawableChildren.empty()) {
      auto sumChildrenOnly = std::accumulate(
          drawableChildren.begin(), drawableChildren.end(), std::size_t{0},
          [](std::size_t sum, const DrawableTreePtr &child) {
            return sum + child->overallWidth;
          });
      childrenWidth = sumChildrenOnly + (drawableChildren.size() - 1) * kMargin;
    }

    std::size_t childrenHeight = 0;
    if (!drawableChildren.empty()) {
      auto highest = *std::max_element(
          drawableChildren.begin(), drawableChildren.end(),
          [](const DrawableTreePtr &a, const DrawableTreePtr &b) {
            return a->overallHeight < b->overallHeight;
          });
      childrenHeight = highest->overallHeight;
    }

    std::size_t width = root->label.size() + 2;
    std::size_t height = 1;

    std::size_t overallWidth = std::max(width, childrenWidth);
    std::size_t overallHeight =
        childrenHeight == 0 ? height : height + childrenHeight + 1;

    auto centerOfCurrentBox = (width - 1) / 2;
    auto centerX = centerOfCurrentBox;
    std::size_t childrenLeftOffset = 0;

    if (!drawableChildren.empty()) {
      auto firstChildCenter = drawableChildren.front()->centerX;
      auto lastChildCenter = drawableChildren.back()->centerX;
      auto connectionBarWidth =
          childrenWidth - (firstChildCenter - 1) -
          (drawableChildren.back()->overallWidth - lastChildCenter);
      auto centerOfChildren =
          firstChildCenter + (connectionBarWidth + 1) / 2 - 1;
      centerX = std::max(centerOfCurrentBox, centerOfChildren);
      childrenLeftOffset = std::max(0, static_cast<int>(centerOfCurrentBox) -
                                           static_cast<int>(centerOfChildren));
      auto currentNodeRightBuffer = width / 2;
      auto lastChildRightBuffer =
          drawableChildren.back()->overallWidth - lastChildCenter;
      auto connectionBarRightBuffer = connectionBarWidth / 2;
      overallWidth =
          std::max(centerX + currentNodeRightBuffer + 1,
                   centerX + connectionBarRightBuffer + lastChildRightBuffer);
    }

    return std::make_shared<DrawableTree>(DrawableTree{
        width, height, centerX, overallWidth, overallHeight, root->label,
        childrenLeftOffset, std::move(drawableChildren)});
  }

  std::string render(std::string_view title = "") {
    Canvas canvas(overallHeight, std::vector<char>(overallWidth, ' '));

    renderInternal(&canvas, {0, 0});

    std::ostringstream out;
    if (!title.empty()) {
      out << "==== " << title << " ====\n";
    }
    for (const auto &row : canvas) {
      out << std::string_view(row.data(), row.size()) << '\n';
    }
    return out.str();
  }

private:
  void renderInternal(Canvas *canvas, const Point &pos) {
    std::size_t left = pos.x + centerX - (width + 1) / 2;

    std::size_t labelStart = left + (width - label.size()) / 2 + 1;
    for (std::size_t i = 0; i < label.size(); ++i) {
      (*canvas)[pos.y][labelStart + i] = label[i];
    }

    if (pos.y != 0 || pos.x != 0) {
      (*canvas)[pos.y - 1][pos.x + centerX] = '+';
    }

    renderChildren(canvas, pos);
  }

  void renderChildren(Canvas *canvas, const Point &pos) {
    if (children.empty()) {
      return;
    }

    std::size_t childOriginY = pos.y + height + 1;

    Point childPos = {pos.x + childrenLeftOffset, childOriginY};

    for (std::size_t childId = 0; childId < children.size(); ++childId) {
      const auto &child = children[childId];
      child->renderInternal(canvas, childPos);

      if (childId != children.size() - 1) {
        std::size_t start = childPos.x + child->centerX + 1;
        std::size_t end = childPos.x + child->overallWidth + kMargin +
                          children[childId + 1]->centerX;

        for (std::size_t x = start; x < end; ++x) {
          if (x != pos.x + centerX) {
            (*canvas)[pos.y + height][x] = '-';
          } else {
            (*canvas)[pos.y + height][x] = '+';
          }
        }

        childPos.x += child->overallWidth + kMargin;
      }
    }
  }
};

} // namespace facebook::velox::common
