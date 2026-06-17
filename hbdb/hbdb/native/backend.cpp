#include <algorithm>
#include <map>
#include <mutex>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <string>
#include <vector>

namespace py = pybind11;

struct VersionedValue {
  uint64_t ts;
  py::object value; // Store Python object directly
};

#include <fstream>
#include <iostream>

class NativeBackend {
public:
  NativeBackend() {}

  void write(const std::string &key, py::object value, uint64_t ts) {
    std::lock_guard<std::mutex> lock(mtx);
    auto &versions = store[key];
    // Keep ascending ts order regardless of arrival order: WAL replay
    // and racing same-key commits can apply writes out of timestamp
    // order. Common case (ascending arrivals) stays an append.
    auto it = versions.end();
    while (it != versions.begin() && std::prev(it)->ts > ts)
      --it;
    versions.insert(it, {ts, value});
  }

  py::object read(const std::string &key, uint64_t read_ts) {
    std::lock_guard<std::mutex> lock(mtx);
    auto it = store.find(key);
    if (it == store.end())
      return py::none();

    const auto &versions = it->second;
    // Find latest version <= read_ts
    // Versions are appended in order, so we can search backwards
    for (auto rit = versions.rbegin(); rit != versions.rend(); ++rit) {
      if (rit->ts <= read_ts) {
        return rit->value;
      }
    }
    return py::none();
  }

  std::vector<std::pair<std::string, py::object>>
  scan(const std::string &start, const std::string &end, uint64_t read_ts) {
    std::lock_guard<std::mutex> lock(mtx);
    std::vector<std::pair<std::string, py::object>> result;

    auto it = store.lower_bound(start);
    while (it != store.end()) {
      if (it->first >= end)
        break;

      const auto &versions = it->second;
      // Find visible version
      for (auto rit = versions.rbegin(); rit != versions.rend(); ++rit) {
        if (rit->ts <= read_ts) {
          result.push_back({it->first, rit->value});
          break;
        }
      }

      it++;
    }
    return result;
  }

  // Snapshot Format:
  // [Magic:4][Version:4][NumKeys:8]
  // Foreach Key:
  //   [KeyLen:4][KeyBytes...]
  //   [NumVers:4]
  //   Foreach Ver:
  //     [TS:8]
  //     [ValLen:4][ValPickledBytes...]

  void save_snapshot(const std::string &path) {
    std::lock_guard<std::mutex> lock(mtx);
    std::ofstream out(path, std::ios::binary);
    if (!out)
      throw std::runtime_error("Cannot open file for snapshot");

    const char magic[] = "HBDB";
    uint32_t version = 1;
    uint64_t num_keys = store.size();

    out.write(magic, 4);
    out.write(reinterpret_cast<char *>(&version), 4);
    out.write(reinterpret_cast<char *>(&num_keys), 8);

    py::object dumps = py::module::import("pickle").attr("dumps");

    for (const auto &kv : store) {
      const std::string &key = kv.first;
      const auto &versions = kv.second;

      uint32_t klen = key.size();
      out.write(reinterpret_cast<char *>(&klen), 4);
      out.write(key.data(), klen);

      uint32_t nver = versions.size();
      out.write(reinterpret_cast<char *>(&nver), 4);

      for (const auto &v : versions) {
        out.write(reinterpret_cast<const char *>(&v.ts), 8);

        // Serialize value using Python pickle
        py::bytes bytes = dumps(v.value);
        std::string s_bytes = bytes; // Copy to C++ string
        uint32_t vlen = s_bytes.size();

        out.write(reinterpret_cast<char *>(&vlen), 4);
        out.write(s_bytes.data(), vlen);
      }
    }
    out.close();
  }

  static void read_exact(std::ifstream &in, char *buf, std::streamsize n) {
    in.read(buf, n);
    if (in.gcount() != n)
      throw std::runtime_error("Truncated snapshot file");
  }

  uint64_t load_snapshot(const std::string &path) {
    std::lock_guard<std::mutex> lock(mtx);
    std::ifstream in(path, std::ios::binary);
    if (!in)
      throw std::runtime_error("Cannot open snapshot file");

    char magic[5] = {0};
    read_exact(in, magic, 4);
    if (std::string(magic) != "HBDB")
      throw std::runtime_error("Invalid snapshot magic");

    uint32_t version;
    read_exact(in, reinterpret_cast<char *>(&version), 4);
    if (version != 1)
      throw std::runtime_error("Unsupported snapshot version");

    uint64_t num_keys;
    read_exact(in, reinterpret_cast<char *>(&num_keys), 8);

    // Parse into a fresh map and swap at the end, so a corrupt or
    // truncated file throws without destroying the current state.
    std::map<std::string, std::vector<VersionedValue>> new_store;

    uint64_t max_ts = 0;

    py::object loads = py::module::import("pickle").attr("loads");

    for (uint64_t i = 0; i < num_keys; ++i) {
      uint32_t klen;
      read_exact(in, reinterpret_cast<char *>(&klen), 4);
      std::string key(klen, '\0');
      read_exact(in, &key[0], klen);

      uint32_t nver;
      read_exact(in, reinterpret_cast<char *>(&nver), 4);

      std::vector<VersionedValue> &versions = new_store[key];
      versions.reserve(nver);

      for (uint32_t j = 0; j < nver; ++j) {
        uint64_t ts;
        read_exact(in, reinterpret_cast<char *>(&ts), 8);
        if (ts > max_ts)
          max_ts = ts;

        uint32_t vlen;
        read_exact(in, reinterpret_cast<char *>(&vlen), 4);
        std::string vbytes(vlen, '\0');
        read_exact(in, &vbytes[0], vlen);

        // Deserialize
        py::object val = loads(py::bytes(vbytes));
        versions.push_back({ts, val});
      }
    }

    store.swap(new_store);
    return max_ts;
  }

private:
  std::mutex mtx;
  std::map<std::string, std::vector<VersionedValue>> store;
};

void init_backend(py::module &m) {
  py::class_<NativeBackend>(m, "NativeBackend")
      .def(py::init<>())
      .def("write", &NativeBackend::write)
      .def("read", &NativeBackend::read)
      .def("scan", &NativeBackend::scan)
      .def("save_snapshot", &NativeBackend::save_snapshot)
      .def("load_snapshot", &NativeBackend::load_snapshot);
}
