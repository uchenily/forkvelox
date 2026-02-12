#pragma once

#include <string>

namespace facebook::velox {

/// Dynamically opens and registers functions defined in a shared library.
///
/// The library needs to provide a function with the following signature:
///
///   extern "C" void registerExtensions();
///
/// The registration function must be defined in the global namespace.
void loadDynamicLibrary(
    const std::string &fileName,
    const std::string &entryPointSymbol = "registerExtensions");

} // namespace facebook::velox
