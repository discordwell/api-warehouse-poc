#include <pybind11/pybind11.h>
#include "resolver.h"
#include "backend.h"

namespace py = pybind11;

PYBIND11_MODULE(native_ext, m) {
    m.doc() = "HBDB Native C++ Extensions";

    init_resolver(m);
    init_backend(m);
}
