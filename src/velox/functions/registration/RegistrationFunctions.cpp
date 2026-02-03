namespace facebook::velox::functions {

extern void registerArithmeticFunctions();
extern void registerStringFunctions();
extern void registerComparisonFunctions();
extern void registerLambdaFunctions();

void registerAllScalarFunctions() {
  registerArithmeticFunctions();
  registerStringFunctions();
  registerComparisonFunctions();
  registerLambdaFunctions();
}

} // namespace facebook::velox::functions
