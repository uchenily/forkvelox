参考velox目录下的代码(仅参考, 不要修改velox这个目录下的任何代码), 实现一个复刻版本的执行引擎(不能简化设计而导致执行低效, 比如使用std::string代替StringView), 特别需要注意的模块 Driver Buffer MemoryPool Vector Executor Operator PlanBuilder PlanNode PlanFragment Task Expr

注意, 现阶段只需要实现需要的算子, 示例中使用PlanBuilder 构建一个计划, 参考完成示例 @velox/velox/exec/tests/VeloxIn10MinDemo.cpp  确保能正常编译和运行

不要第三方依赖. 使用cmake构建, 使用c++23标准(尽可能使用现代c++语法和基础库) 底层数据结构比如Vector/Buffer尽可能按照保证和原实现一样高效(字符串Vector中字符串类型不应该直接使用std::string), 对外提供的api应该和原实现保持一致.

另外, 需要添加详尽的日志 展示执行的过程, 也方便定位排查问题

类型方面只需要关注 int32和变长字符串类型

另外注意velox不是一个单纯的push-based或者pull-based模型, 需要按照原velox的设计来实现

这是一个非常复杂的任务, 你需要将任务分解为可执行的多个步骤. 每一个步骤完成后git commit提交, 注意先实现基础的底层功能, 然后再实现上层功能. 每一个commit提交的代码行数应该尽可能控制在300行以内.

不要简化velox原来的设计与实现, 尽可能遵照原实现.
