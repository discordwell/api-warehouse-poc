#include <iostream>
#include <map>
#include <mutex>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <string>
#include <vector>

namespace py = pybind11;

class NativeResolver {
public:
  NativeResolver() : current_ts(0) {}

  uint64_t get_read_timestamp() {
    std::lock_guard<std::mutex> lock(mtx);
    return current_ts;
  }

  void set_current_timestamp(uint64_t ts) {
    std::lock_guard<std::mutex> lock(mtx);
    if (ts > current_ts) {
      current_ts = ts;
    }
  }

  // Returnspair<success, commit_ts>
  std::pair<bool, uint64_t>
  commit(uint64_t read_ts, const std::vector<std::string> &read_keys,
         const std::vector<std::pair<std::string, std::string>> &read_ranges,
         const std::vector<std::string> &write_keys) {

    if (write_keys.empty()) {
      return {true, read_ts};
    }

    std::lock_guard<std::mutex> lock(mtx);

    // 1. Check Read Keys
    for (const auto &key : read_keys) {
      auto it = committed_writes.find(key);
      if (it != committed_writes.end() && it->second > read_ts) {
        return {false, 0};
      }
    }

    // 2. Check Read Ranges
    // Efficient scanning using std::map::lower_bound
    for (const auto &range : read_ranges) {
      const std::string &start = range.first;
      const std::string &end = range.second;

      // Find first key >= start
      auto it = committed_writes.lower_bound(start);

      // Scan until we hit end or run out
      while (it != committed_writes.end()) {
        if (it->first >= end)
          break; // Reached end of range

        if (it->second > read_ts) {
          return {false, 0}; // Conflict!
        }
        it++;
      }
    }

    // 3. Commit
    current_ts++;
    uint64_t commit_ts = current_ts;

    for (const auto &key : write_keys) {
      committed_writes[key] = commit_ts;
    }

    return {true, commit_ts};
  }

private:
  std::mutex mtx;
  std::map<std::string, uint64_t> committed_writes;
  uint64_t current_ts;
};

void init_resolver(py::module &m) {
  py::class_<NativeResolver>(m, "NativeResolver")
      .def(py::init<>())
      .def("get_read_timestamp", &NativeResolver::get_read_timestamp)
      .def("set_current_timestamp", &NativeResolver::set_current_timestamp)
      .def("commit", &NativeResolver::commit);
}
