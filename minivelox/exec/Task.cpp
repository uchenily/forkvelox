#include "exec/Task.h"

namespace facebook::velox::exec {

void Task::start(int maxDrivers, int concurrentSplitGroups) {
    // In mini version, we might not need full Task logic yet.
    // The demo uses `AssertQueryBuilder` which likely bypasses complex Task scheduling
    // and just runs a Driver.
}

}
