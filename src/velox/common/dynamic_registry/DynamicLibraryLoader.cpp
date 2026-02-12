#include "velox/common/dynamic_registry/DynamicLibraryLoader.h"

#include <dlfcn.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/base/Log.h"

namespace facebook::velox {

void loadDynamicLibrary(const std::string &fileName, const std::string &entryPointSymbol) {
  void *handler = dlopen(fileName.c_str(), RTLD_NOW);
  if (handler == nullptr) {
    VELOX_FAIL("Error while loading shared library '{}': {}", fileName, dlerror());
  }

  LOG_INFO("Loaded library {}. Searching registry symbol {}", fileName, entryPointSymbol);

  // Clear any existing error before dlsym.
  dlerror();
  void *registrySymbol = dlsym(handler, entryPointSymbol.c_str());
  const char *error = dlerror();

  if (error != nullptr) {
    VELOX_FAIL("Couldn't find Velox registry symbol: {}", error);
  }

  auto loadUserLibrary = reinterpret_cast<void (*)()>(registrySymbol);
  if (loadUserLibrary == nullptr) {
    VELOX_FAIL("Symbol '{}' resolved to a nullptr, unable to invoke it.", entryPointSymbol);
  }

  loadUserLibrary();
  LOG_INFO("Registered functions by {}", entryPointSymbol);
}

} // namespace facebook::velox
