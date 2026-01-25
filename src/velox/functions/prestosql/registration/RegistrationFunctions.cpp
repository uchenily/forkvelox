#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

namespace facebook::velox::functions::prestosql {

extern void registerArithmeticFunctions();
extern void registerStringFunctions();
extern void registerComparisonFunctions();

void registerAllScalarFunctions() {
    registerArithmeticFunctions();
    registerStringFunctions();
    registerComparisonFunctions();
}

}
