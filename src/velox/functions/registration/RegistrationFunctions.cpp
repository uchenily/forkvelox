#include "velox/functions/registration/RegistrationFunctions.h"

namespace facebook::velox::functions {

extern void registerArithmeticFunctions();
extern void registerStringFunctions();
extern void registerComparisonFunctions();

void registerAllScalarFunctions() {
    registerArithmeticFunctions();
    registerStringFunctions();
    registerComparisonFunctions();
}

}
