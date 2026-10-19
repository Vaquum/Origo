#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <exception>
#include <memory>
#include <stdexcept>
#include <vector>

#if __BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__
#error Book RowBinary requires little-endian transport
#endif

struct Buffers {
  Py_buffer views[11]{};
  int acquired = 0;
  ~Buffers() {
    for (int i = 0; i < acquired; ++i)
      PyBuffer_Release(&views[i]);
  }
  template <class T> const T *data(int i) const {
    return static_cast<const T *>(views[i].buf);
  }
  size_t count(int i) const {
    return static_cast<size_t>(views[i].len / views[i].itemsize);
  }
};

struct Side {
  std::vector<int32_t> quantities;
  std::vector<uint64_t> words, summary;
  explicit Side(size_t n)
      : quantities(n, 0), words((n + 63) / 64, 0),
        summary((words.size() + 63) / 64, 0) {}
  void set(size_t p, int32_t q) {
    quantities[p] = q;
    size_t w = p / 64;
    uint64_t bit = uint64_t(1) << (p % 64);
    if (q)
      words[w] |= bit;
    else
      words[w] &= ~bit;
    uint64_t marker = uint64_t(1) << (w % 64);
    if (words[w])
      summary[w / 64] |= marker;
    else
      summary[w / 64] &= ~marker;
  }
  int word(int start, bool descending) const {
    if (start < 0 || static_cast<size_t>(start) >= words.size())
      return -1;
    int group = start / 64;
    uint64_t mask = descending ? (~uint64_t(0) >> (63 - start % 64))
                               : (~uint64_t(0) << (start % 64));
    uint64_t present = summary[group] & mask;
    while (!present) {
      group += descending ? -1 : 1;
      if (group < 0 || static_cast<size_t>(group) >= summary.size())
        return -1;
      present = summary[group];
    }
    return group * 64 + (descending ? 63 - __builtin_clzll(present)
                                    : __builtin_ctzll(present));
  }
  int best(bool descending) const {
    int w = word(descending ? int(words.size()) - 1 : 0, descending);
    return w < 0 ? -1
                 : w * 64 + (descending ? 63 - __builtin_clzll(words[w])
                                        : __builtin_ctzll(words[w]));
  }
};

struct Chunk {
  const int64_t *offsets;
  const uint8_t *sides;
  const uint32_t *prices;
  const int32_t *quantities;
  size_t nevent, nlevel;
};
struct Replay {
  const int64_t *offsets, *events;
  const uint8_t *adjacent;
  std::vector<uint8_t> observed;
  int64_t floor, ceiling;
  const uint64_t *native;
  const uint8_t *kinds, *sides;
  const uint32_t *prices;
  const int32_t *quantities;
  const double *price_values, *quantity_values;
  size_t nevent, nlevel, nprice, nquantity;
  Side bid, ask;
  std::vector<int32_t> replacement;
  char *small, *large;
  size_t small_rows = 0, large_rows = 0;
  int64_t next, end;
  int current = -1;
  const std::vector<Chunk> *chunks;
  const uint32_t *ranks;
  size_t nrank;
  const int64_t *chunk_offsets = nullptr;
  size_t local = 0;
  int64_t begin(size_t e) const {
    return chunks ? chunk_offsets[local] : offsets[e];
  }
  int64_t finish(size_t e) const {
    return chunks ? chunk_offsets[local + 1] : offsets[e + 1];
  }
  uint32_t price(size_t i) const {
    return chunks ? ranks[prices[i]] : prices[i];
  }
  void select(size_t e) {
    if (chunks) {
      uint64_t location = uint64_t(offsets[e]);
      const auto &c = (*chunks)[location >> 32];
      local = uint32_t(location);
      chunk_offsets = c.offsets;
      sides = c.sides;
      prices = c.prices;
      quantities = c.quantities;
    }
  }
  Replay(const Buffers &b, int64_t start, int64_t finish, char *out20,
         char *out200, const std::vector<Chunk> *parts = nullptr)
      : offsets(b.data<int64_t>(0)), events(b.data<int64_t>(2)),
        adjacent(b.data<uint8_t>(9)), observed(2 * b.count(7), 0),
        floor(int64_t(b.count(7))), ceiling(-1), native(b.data<uint64_t>(3)),
        kinds(b.data<uint8_t>(1)), sides(b.data<uint8_t>(4)),
        prices(b.data<uint32_t>(5)), quantities(b.data<int32_t>(6)),
        price_values(b.data<double>(7)), quantity_values(b.data<double>(8)),
        nevent(b.count(1)), nlevel(b.count(4)), nprice(b.count(7)),
        nquantity(b.count(8)), bid(nprice), ask(nprice), replacement(nprice, 0),
        small(out20), large(out200), next(start), end(finish), chunks(parts),
        ranks(parts ? b.data<uint32_t>(10) : nullptr),
        nrank(parts ? b.count(10) : 0) {}
  void validate() const {
    if (!nevent || !nprice || !nquantity ||
        (!chunks && (offsets[0] != 0 || offsets[nevent] != int64_t(nlevel))))
      throw std::invalid_argument("Invalid replay dimensions.");
    for (size_t e = 0; e < nevent; ++e) {
      if (kinds[e] > 1)
        throw std::invalid_argument("Invalid event kind.");
      if (chunks) {
        uint64_t location = uint64_t(offsets[e]);
        size_t chunk = location >> 32;
        if (chunk >= chunks->size() ||
            uint32_t(location) >= (*chunks)[chunk].nevent)
          throw std::invalid_argument("Invalid chunk event.");
      } else if (offsets[e] < 0 || offsets[e] > offsets[e + 1])
        throw std::invalid_argument("Invalid event offsets.");
      if (e && events[e] < events[e - 1])
        throw std::invalid_argument("Exchange clock regressed.");
    }
    if (chunks) {
      for (size_t p = 0; p < nrank; ++p)
        if (ranks[p] >= nprice)
          throw std::invalid_argument("Invalid price rank.");
      for (const auto &c : *chunks) {
        if (c.offsets[0] != 0 || c.offsets[c.nevent] != int64_t(c.nlevel))
          throw std::invalid_argument("Invalid chunk offsets.");
        for (size_t e = 0; e < c.nevent; ++e)
          if (c.offsets[e] < 0 || c.offsets[e] > c.offsets[e + 1])
            throw std::invalid_argument("Invalid chunk order.");
        for (size_t i = 0; i < c.nlevel; ++i)
          if (c.sides[i] > 1 || c.prices[i] >= nrank || c.quantities[i] < 0 ||
              size_t(c.quantities[i]) >= nquantity)
            throw std::invalid_argument("Invalid chunk level.");
      }
    }
    for (size_t i = 0; i < nlevel; ++i) {
      if (sides[i] > 1 || prices[i] >= nprice || quantities[i] < 0 ||
          size_t(quantities[i]) >= nquantity)
        throw std::invalid_argument("Invalid level index.");
    }
  }
  static void fail(const char *message) { throw std::runtime_error(message); }
  void uncrossed() const {
    int b = bid.best(true), a = ask.best(false);
    if (b >= 0 && a >= 0 && b >= a)
      fail("BOOK_CROSSED|Witnessed positive levels cross.");
  }
  void snapshot(size_t e) {
    for (int side = 0; side < 2; ++side) {
      size_t bound = side ? 0 : nprice;
      bool seen = false;
      for (int64_t i = begin(e); i < finish(e); ++i)
        if (sides[i] == side) {
          if (!quantities[i])
            throw std::invalid_argument("Snapshot contains zero quantity.");
          bound = side ? std::max(bound, size_t(price(i)))
                       : std::min(bound, size_t(price(i)));
          seen = true;
        }
      if (!seen)
        continue;
      size_t lo = side ? 0 : bound, hi = side ? bound + 1 : nprice;
      std::fill(replacement.begin() + lo, replacement.begin() + hi, 0);
      for (int64_t i = begin(e); i < finish(e); ++i)
        if (sides[i] == side)
          replacement[price(i)] = quantities[i];
      Side &book = side ? ask : bid;
      for (size_t p = lo; p < hi; ++p) {
        if ((observed[side * nprice + p] ||
             (side ? int64_t(p) <= ceiling : int64_t(p) >= floor)) &&
            book.quantities[p] != replacement[p])
          fail("BOOK_SNAPSHOT_MISMATCH|Snapshot disagrees with known levels.");
        book.set(p, replacement[p]);
      }
      if (side)
        ceiling = std::max(ceiling, int64_t(bound));
      else
        floor = std::min(floor, int64_t(bound));
    }
  }
  double write_side(char *&out, const Side &side, int depth, bool descending,
                    int side_number) {
    int w = side.word(descending ? int(side.words.size()) - 1 : 0, descending);
    uint64_t present = w < 0 ? 0 : side.words[w];
    double first = 0, previous = 0;
    for (int level = 0; level < depth; ++level) {
      while (!present) {
        w = side.word(w + (descending ? -1 : 1), descending);
        if (w < 0)
          fail("BOOK_KNOWN_DEPTH_EXHAUSTED|Fewer than the requested levels.");
        present = side.words[w];
      }
      int bit =
          descending ? 63 - __builtin_clzll(present) : __builtin_ctzll(present);
      size_t p = size_t(w) * 64 + bit;
      double price = price_values[p],
             quantity = quantity_values[side.quantities[p]];
      if (!std::isfinite(price) || !std::isfinite(quantity) || price <= 0 ||
          quantity <= 0 ||
          (level && (descending ? price >= previous : price <= previous)))
        fail("BOOK_FLOAT64_INVALID|Sample loses finite positive ordered "
             "levels.");
      if (!level)
        first = price;
      if (level == depth - 1 &&
          (side_number ? int64_t(p) > ceiling : int64_t(p) < floor))
        fail("BOOK_KNOWN_DEPTH_EXHAUSTED|Top depth exceeds the proven region.");
      std::memcpy(out, &price, 8);
      std::memcpy(out + 8, &quantity, 8);
      out += 16;
      previous = price;
      present &= ~(uint64_t(1) << bit);
    }
    return first;
  }
  void sample(int depth) {
    if (current < 0 || events[current] < 0)
      fail("BOOK_VENDOR_CLOCK_UNPROVEN|Grid has no exchange witness.");
    if (next - events[current] > 5000)
      fail("BOOK_EVENT_STALE|Grid has no timely exchange witness.");
    char *out =
        depth == 20 ? small + small_rows++ * 666 : large + large_rows++ * 6428;
    std::memcpy(out, &next, 8);
    std::memcpy(out + 8, &events[current], 8);
    std::memcpy(out + 16, &native[current], 8);
    out += 24;
    *out++ = char(depth);
    if (depth == 200)
      *out++ = 1;
    double best_bid = write_side(out, bid, depth, true, 0);
    *out++ = char(depth);
    if (depth == 200)
      *out++ = 1;
    double best_ask = write_side(out, ask, depth, false, 1);
    if (best_bid >= best_ask)
      fail("BOOK_FLOAT64_INVALID|Sample loses noncrossing prices.");
  }
  void until(int64_t event) {
    while (next < event && next < end) {
      sample(20);
      if (next % 1000 == 0)
        sample(200);
      next += 100;
    }
  }
  void run() {
    validate();
    for (size_t e = 0; e < nevent; ++e) {
      select(e);
      until(events[e]);
      if (kinds[e]) {
        for (int64_t i = begin(e); i < finish(e); ++i) {
          (sides[i] ? ask : bid).set(price(i), quantities[i]);
          observed[size_t(sides[i]) * nprice + price(i)] = 1;
        }
        int best_bid = bid.best(true), best_ask = ask.best(false);
        if (best_bid >= 0 && best_ask >= 0 && best_bid >= best_ask)
          fail("BOOK_CROSSED|Witnessed positive levels cross.");
        if (best_bid >= 0 && best_ask >= 0) {
          floor = std::min(floor, int64_t(best_ask));
          ceiling = std::max(ceiling, int64_t(best_bid));
        }
      } else {
        snapshot(e);
        uncrossed();
      }
      while (floor > 0 && floor < int64_t(nprice) && adjacent[floor - 1] &&
             observed[floor - 1])
        --floor;
      while (ceiling >= 0 && ceiling + 1 < int64_t(nprice) &&
             adjacent[ceiling] && observed[nprice + ceiling + 1])
        ++ceiling;
      current = int(e);
    }
    until(end);
    if (small_rows != 36000 || large_rows != 3600)
      throw std::runtime_error("Unwritten output samples.");
  }
};

static PyObject *replay(PyObject *, PyObject *args) {
  PyObject *inputs, *parts = Py_None;
  long long start, end;
  if (!PyArg_ParseTuple(args, "OLL|O", &inputs, &start, &end, &parts))
    return nullptr;
  bool chunked = parts != Py_None;
  int width = chunked ? 11 : 10;
  if (!PyTuple_Check(inputs) || PyTuple_Size(inputs) != width || start < 0 ||
      start % 3600000 || start > INT64_MAX - 3600000 ||
      end != start + 3600000) {
    PyErr_SetString(PyExc_ValueError, "Invalid replay arguments.");
    return nullptr;
  }
  Buffers buffers;
  const int sizes[11] = {8, 1, 8, 8, 1, 4, 4, 8, 8, 1, 4};
  for (int i = 0; i < width; ++i) {
    if (PyObject_GetBuffer(PyTuple_GetItem(inputs, i), &buffers.views[i],
                           PyBUF_CONTIG_RO) < 0)
      return nullptr;
    ++buffers.acquired;
    if (buffers.views[i].itemsize != sizes[i] ||
        buffers.views[i].len % sizes[i]) {
      PyErr_SetString(PyExc_ValueError, "Invalid replay buffer width.");
      return nullptr;
    }
  }
  const size_t n = buffers.count(1), p = buffers.count(7),
               levels = buffers.count(4);
  if (n > INT32_MAX || p > INT32_MAX || levels > UINT32_MAX ||
      buffers.count(0) != n + 1 || buffers.count(2) != n ||
      buffers.count(3) != n || buffers.count(5) != levels ||
      buffers.count(6) != levels || !p || buffers.count(9) != p - 1) {
    PyErr_SetString(PyExc_ValueError, "Invalid replay buffer length.");
    return nullptr;
  }
  std::vector<std::unique_ptr<Buffers>> owners;
  std::vector<Chunk> chunks;
  if (chunked) {
    if (!PyTuple_Check(parts)) {
      PyErr_SetString(PyExc_ValueError, "Chunks require a tuple.");
      return nullptr;
    }
    const int sizes[4] = {8, 1, 4, 4};
    for (Py_ssize_t j = 0; j < PyTuple_Size(parts); ++j) {
      PyObject *part = PyTuple_GetItem(parts, j);
      if (!PyTuple_Check(part) || PyTuple_Size(part) != 4) {
        PyErr_SetString(PyExc_ValueError, "Invalid chunk tuple.");
        return nullptr;
      }
      auto owner = std::make_unique<Buffers>();
      for (int k = 0; k < 4; ++k) {
        if (PyObject_GetBuffer(PyTuple_GetItem(part, k), &owner->views[k],
                               PyBUF_CONTIG_RO) < 0)
          return nullptr;
        ++owner->acquired;
        if (owner->views[k].itemsize != sizes[k] ||
            owner->views[k].len % sizes[k]) {
          PyErr_SetString(PyExc_ValueError, "Invalid chunk width.");
          return nullptr;
        }
      }
      size_t rows = owner->count(1);
      if (!owner->count(0) || owner->count(2) != rows ||
          owner->count(3) != rows) {
        PyErr_SetString(PyExc_ValueError, "Invalid chunk lengths.");
        return nullptr;
      }
      size_t total = rows;
      for (const auto &existing : chunks)
        total += existing.nlevel;
      if (total > UINT32_MAX) {
        PyErr_SetString(PyExc_ValueError, "Invalid replay level count.");
        return nullptr;
      }
      chunks.push_back({owner->data<int64_t>(0), owner->data<uint8_t>(1),
                        owner->data<uint32_t>(2), owner->data<int32_t>(3),
                        owner->count(0) - 1, rows});
      owners.push_back(std::move(owner));
    }
  }
  PyObject *small = PyBytes_FromStringAndSize(nullptr, 36000 * 666);
  if (!small)
    return nullptr;
  PyObject *large = PyBytes_FromStringAndSize(nullptr, 3600 * 6428);
  if (!large) {
    Py_DECREF(small);
    return nullptr;
  }
  std::exception_ptr error;
  Py_BEGIN_ALLOW_THREADS try {
    Replay worker(buffers, start, end, PyBytes_AS_STRING(small),
                  PyBytes_AS_STRING(large), chunked ? &chunks : nullptr);
    worker.run();
  } catch (...) {
    error = std::current_exception();
  }
  Py_END_ALLOW_THREADS if (error) {
    Py_DECREF(small);
    Py_DECREF(large);
    try {
      std::rethrow_exception(error);
    } catch (const std::exception &caught) {
      PyErr_SetString(PyExc_ValueError, caught.what());
    }
    return nullptr;
  }
  return Py_BuildValue("NN", small, large);
}
static PyMethodDef methods[] = {
    {"replay", replay, METH_VARARGS,
     "Replay typed archive buffers into the two exact RowBinary grids."},
    {nullptr, nullptr, 0, nullptr}};
static struct PyModuleDef module = {PyModuleDef_HEAD_INIT, "_origo_book_replay",
                                    nullptr, -1, methods};
PyMODINIT_FUNC PyInit__origo_book_replay() { return PyModule_Create(&module); }
