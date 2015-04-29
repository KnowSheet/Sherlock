/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2015 Dmitry "Dima" Korolev <dmitry.korolev@gmail.com>
          (c) 2015 Maxim Zhurovich <zhurovich@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*******************************************************************************/

#include "yoda.h"

#include <string>
#include <atomic>
#include <thread>

#include "../../Bricks/dflags/dflags.h"
#include "../../Bricks/3party/gtest/gtest-main-with-dflags.h"

// TODO(mzhurovich): We'll need it for REST API tests.
// DEFINE_int32(yoda_http_test_port, 8090, "Local port to use for Sherlock unit test.");

using std::string;
using std::atomic_size_t;
using bricks::strings::Printf;

struct KeyValueEntry {
  int key;
  double value;

  // Uncomment one line below to see the test does not compile.
  // constexpr static bool allow_nonthrowing_get = true;
  // Change `struct KeyValueEntry` into `struct KeyValueEntry : yoda::Nullable` and
  // uncomment the above and the below line to see the test compile and fail due to not throwing on a `Get()`.
  // void operator=(const KeyValueEntry& rhs) { std::tie(key, value) = std::make_tuple(rhs.key, rhs.value); }

  KeyValueEntry(const int key = 0, const double value = 0.0) : key(key), value(value) {}

  template <typename A>
  void serialize(A& ar) {
    ar(CEREAL_NVP(key), CEREAL_NVP(value));
  }
};

struct KeyValueSubscriptionData {
  atomic_size_t seen_;
  string results_;
  KeyValueSubscriptionData() : seen_(0u) {}
};

struct KeyValueAggregateListener {
  KeyValueSubscriptionData& data_;
  size_t max_to_process_ = static_cast<size_t>(-1);

  KeyValueAggregateListener() = delete;
  explicit KeyValueAggregateListener(KeyValueSubscriptionData& data) : data_(data) {}

  KeyValueAggregateListener& SetMax(size_t cap) {
    max_to_process_ = cap;
    return *this;
  }

  bool Entry(const KeyValueEntry& entry, size_t index, size_t total) {
    static_cast<void>(index);
    static_cast<void>(total);
    if (data_.seen_) {
      data_.results_ += ",";
    }
    data_.results_ += Printf("%d=%.2lf", entry.key, entry.value);
    ++data_.seen_;
    return data_.seen_ < max_to_process_;
  }

  void Terminate() {
    if (data_.seen_) {
      data_.results_ += ",";
    }
    data_.results_ += "DONE";
  }
};

TEST(Sherlock, NonPolymorphicKeyEntryStorage) {
  typedef yoda::API<yoda::KeyEntry<KeyValueEntry>> TestAPI;
  TestAPI api("non_polymorphic_keyentry_yoda");

  // Add the first key-value pair.
  // Use `UnsafeStream()`, since generally the only way to access the underlying stream is to make API calls.
  api.UnsafeStream().Emplace(2, 0.5);

  while (!api.CaughtUp()) {
    // Spin lock, for the purposes of this test.
    // Ensure that the data has reached the the processor that maintains the in-memory state of the API.
  }

  // Future expanded syntax.
  std::future<KeyValueEntry> f1 = api.AsyncGet(TestAPI::T_KEY(2));
  KeyValueEntry r1 = f1.get();
  EXPECT_EQ(2, r1.key);
  EXPECT_EQ(0.5, r1.value);

  // Future short syntax.
  EXPECT_EQ(0.5, api.AsyncGet(TestAPI::T_KEY(2)).get().value);

  // Future short syntax with type omitted.
  EXPECT_EQ(0.5, api.AsyncGet(2).get().value);

  // Callback version.
  struct CallbackTest {
    explicit CallbackTest(const int key, const double value, const bool expect_success = true)
        : key(key), value(value), expect_success(expect_success) {}

    void found(const KeyValueEntry& entry) const {
      ASSERT_FALSE(called);
      called = true;
      EXPECT_TRUE(expect_success);
      EXPECT_EQ(key, entry.key);
      EXPECT_EQ(value, entry.value);
    }

    void not_found(const int key) const {
      ASSERT_FALSE(called);
      called = true;
      EXPECT_FALSE(expect_success);
      EXPECT_EQ(this->key, key);
    }

    void added() const {
      ASSERT_FALSE(called);
      called = true;
      EXPECT_TRUE(expect_success);
    }

    void already_exists() const {
      ASSERT_FALSE(called);
      called = true;
      EXPECT_FALSE(expect_success);
    }

    const int key;
    const double value;
    const bool expect_success;
    mutable bool called = false;
  };

  const CallbackTest cbt1(2, 0.5);
  api.AsyncGet(TestAPI::T_KEY(2),
               std::bind(&CallbackTest::found, &cbt1, std::placeholders::_1),
               std::bind(&CallbackTest::not_found, &cbt1, std::placeholders::_1));
  while (!cbt1.called) {
    ;  // Spin lock.
  }

  // Add two more key-value pairs.
  api.UnsafeStream().Emplace(3, 0.33);
  api.UnsafeStream().Emplace(4, 0.25);

  while (api.EntriesSeen() < 3u) {
    // For the purposes of this test: Spin lock to ensure that the listener/MMQ consumer got the data published.
  }

  EXPECT_EQ(0.33, api.AsyncGet(TestAPI::T_KEY(3)).get().value);
  EXPECT_EQ(0.25, api.Get(TestAPI::T_KEY(4)).value);

  ASSERT_THROW(api.AsyncGet(TestAPI::T_KEY(5)).get(), TestAPI::T_KEY_NOT_FOUND_EXCEPTION);
  ASSERT_THROW(api.AsyncGet(TestAPI::T_KEY(5)).get(), yoda::KeyNotFoundCoverException);
  ASSERT_THROW(api.Get(TestAPI::T_KEY(6)), TestAPI::T_KEY_NOT_FOUND_EXCEPTION);
  ASSERT_THROW(api.Get(TestAPI::T_KEY(6)), yoda::KeyNotFoundCoverException);
  const CallbackTest cbt2(7, 0.0, false);
  api.AsyncGet(TestAPI::T_KEY(7),
               std::bind(&CallbackTest::found, &cbt2, std::placeholders::_1),
               std::bind(&CallbackTest::not_found, &cbt2, std::placeholders::_1));
  while (!cbt2.called) {
    ;  // Spin lock.
  }

  // Add three more key-value pairs, this time via the API.
  api.AsyncAdd(KeyValueEntry(5, 0.2)).wait();
  api.Add(KeyValueEntry(6, 0.17));
  const CallbackTest cbt3(7, 0.76);
  api.AsyncAdd(TestAPI::T_ENTRY(7, 0.76),
               std::bind(&CallbackTest::added, &cbt3),
               std::bind(&CallbackTest::already_exists, &cbt3));
  while (!cbt3.called) {
    ;  // Spin lock.
  }

  // Check that default policy doesn't allow overwriting on Add().
  ASSERT_THROW(api.AsyncAdd(KeyValueEntry(5, 1.1)).get(), TestAPI::T_KEY_ALREADY_EXISTS_EXCEPTION);
  ASSERT_THROW(api.AsyncAdd(KeyValueEntry(5, 1.1)).get(), yoda::KeyAlreadyExistsCoverException);
  ASSERT_THROW(api.Add(KeyValueEntry(6, 0.28)), TestAPI::T_KEY_ALREADY_EXISTS_EXCEPTION);
  ASSERT_THROW(api.Add(KeyValueEntry(6, 0.28)), yoda::KeyAlreadyExistsCoverException);
  const CallbackTest cbt4(7, 0.0, false);
  api.AsyncAdd(TestAPI::T_ENTRY(7, 0.0),
               std::bind(&CallbackTest::added, &cbt4),
               std::bind(&CallbackTest::already_exists, &cbt4));
  while (!cbt4.called) {
    ;  // Spin lock.
  }

  // Thanks to eventual consistency, we don't have to wait until the above calls fully propagate.
  // Even if the next two lines run before the entries are published into the stream,
  // the API will maintain the consistency of its own responses from its own in-memory state.
  EXPECT_EQ(0.20, api.AsyncGet(5).get().value);
  EXPECT_EQ(0.17, api.Get(6).value);

  ASSERT_THROW(api.AsyncGet(8).get(), TestAPI::T_KEY_NOT_FOUND_EXCEPTION);
  ASSERT_THROW(api.Get(9), TestAPI::T_KEY_NOT_FOUND_EXCEPTION);

  // Confirm that data updates have been pubished as stream entries as well.
  // This part is important since otherwise the API is no better than a wrapper over a hash map.
  KeyValueSubscriptionData data;
  KeyValueAggregateListener listener(data);
  listener.SetMax(6u);
  api.Subscribe(listener).Join();
  EXPECT_EQ(data.seen_, 6u);
  EXPECT_EQ("2=0.50,3=0.33,4=0.25,5=0.20,6=0.17,7=0.76", data.results_);

  // Test user function accessing the underlying container.
  typedef yoda::Container<yoda::KeyEntry<KeyValueEntry>> T_CONTAINER;
  double sum = 0.0;
  bool done = false;
  api.ApplyFunction([&](const T_CONTAINER& container) {
    for (auto i : container.data) {
      sum += i.second.value;
    }
    done = true;
  });
  while (!done) {
    ;  // Spin lock.
  }
  EXPECT_DOUBLE_EQ(2.21, sum);
}

struct MatrixCell {
  size_t row;
  std::string col;
  int value;

  MatrixCell(const size_t row = 0, const std::string& col = std::string("0"), const int value = 0)
      : row(row), col(col), value(value) {}

  template <typename A>
  void serialize(A& ar) {
    ar(CEREAL_NVP(row), CEREAL_NVP(col), CEREAL_NVP(value));
  }
};

TEST(Sherlock, NonPolymorphicMatrixEntryStorage) {
  typedef yoda::API<yoda::MatrixEntry<MatrixCell>> TestAPI;
  TestAPI api("non_polymorphic_matrix_yoda");

  // Add the first key-value pair.
  // Use `UnsafeStream()`, since generally the only way to access the underlying stream is to make API calls.
  api.UnsafeStream().Emplace(5, "x", -1);

  while (!api.CaughtUp()) {
    // Spin lock, for the purposes of this test.
    // Ensure that the data has reached the the processor that maintains the in-memory state of the API.
  }
 
  EXPECT_EQ(-1, api.AsyncGet(5, "x").get().value);
  EXPECT_EQ(-1, api.Get(5, "x").value);

  // Callback version.
  struct CallbackTest {
    explicit CallbackTest(const size_t row, const std::string& col, const int value, const bool expect_success = true)
        : row(row), col(col), value(value), expect_success(expect_success) {}

    void found(const MatrixCell& entry) const {
      ASSERT_FALSE(called);
      called = true;
      EXPECT_TRUE(expect_success);
      EXPECT_EQ(row, entry.row);
      EXPECT_EQ(col, entry.col);
      EXPECT_EQ(value, entry.value);
    }

    void not_found(const size_t row, const std::string& col) const {
      ASSERT_FALSE(called);
      called = true;
      EXPECT_FALSE(expect_success);
      EXPECT_EQ(this->row, row);
      EXPECT_EQ(this->col, col);
    }

    void added() const {
      ASSERT_FALSE(called);
      called = true;
      EXPECT_TRUE(expect_success);
    }

    void already_exists() const {
      ASSERT_FALSE(called);
      called = true;
      EXPECT_FALSE(expect_success);
    }

    const size_t row;
    const std::string col;
    const int value;
    const bool expect_success;
    mutable bool called = false;
  };

  const CallbackTest cbt1(5, "x", -1);
  api.AsyncGet(TestAPI::T_ROW(5),
               TestAPI::T_COL("x"),
               std::bind(&CallbackTest::found, &cbt1, std::placeholders::_1),
               std::bind(&CallbackTest::not_found, &cbt1, std::placeholders::_1, std::placeholders::_2));
  while (!cbt1.called) {
    ;  // Spin lock.
  }

  ASSERT_THROW(api.AsyncGet(5, "y").get(), TestAPI::T_CELL_NOT_FOUND_EXCEPTION);
  ASSERT_THROW(api.AsyncGet(5, "y").get(), yoda::CellNotFoundCoverException);
  ASSERT_THROW(api.Get(1, "x"), TestAPI::T_CELL_NOT_FOUND_EXCEPTION);
  ASSERT_THROW(api.Get(1, "x"), yoda::CellNotFoundCoverException);
  const CallbackTest cbt2(123, "no_entry", 0, false);
  api.AsyncGet(123, "no_entry",
               std::bind(&CallbackTest::found, &cbt2, std::placeholders::_1),
               std::bind(&CallbackTest::not_found, &cbt2, std::placeholders::_1, std::placeholders::_2));
  while (!cbt2.called) {
    ;  // Spin lock.
  }

  // Add three more key-value pairs, this time via the API.
  api.AsyncAdd(MatrixCell(5, "y", 15)).wait();
  api.Add(MatrixCell(1, "x", -9));
  const CallbackTest cbt3(42, "the_answer", 1);
  api.AsyncAdd(TestAPI::T_ENTRY(42, "the_answer", 1),
               std::bind(&CallbackTest::added, &cbt3),
               std::bind(&CallbackTest::already_exists, &cbt3));
  while (!cbt3.called) {
    ;  // Spin lock.
  }

  EXPECT_EQ(15, api.Get(5, "y").value);
  EXPECT_EQ(-9, api.Get(1, "x").value);
  EXPECT_EQ(1, api.Get(42, "the_answer").value);

  // Check that default policy doesn't allow overwriting on Add().
  ASSERT_THROW(api.AsyncAdd(MatrixCell(5, "y", 8)).get(), TestAPI::T_CELL_ALREADY_EXISTS_EXCEPTION);
  ASSERT_THROW(api.AsyncAdd(MatrixCell(5, "y", 100)).get(), yoda::CellAlreadyExistsCoverException);
  ASSERT_THROW(api.Add(MatrixCell(1, "x", 2)), TestAPI::T_CELL_ALREADY_EXISTS_EXCEPTION);
  ASSERT_THROW(api.Add(MatrixCell(1, "x", 2)), yoda::CellAlreadyExistsCoverException);
  const CallbackTest cbt4(42, "the_answer", 0, false);
  api.AsyncAdd(TestAPI::T_ENTRY(42, "the_answer", 0),
               std::bind(&CallbackTest::added, &cbt4),
               std::bind(&CallbackTest::already_exists, &cbt4));
  while (!cbt4.called) {
    ;  // Spin lock.
  }

   // Test user function accessing the underlying container.
  typedef yoda::Container<yoda::MatrixEntry<MatrixCell>> T_CONTAINER;
  size_t row_index_sum = 0;
  int value_sum = 0;
  bool done = false;
  api.ApplyFunction([&](const T_CONTAINER& container) {
    // Testing forward and transposed matrices.
    for (auto rit : container.forward) {
      row_index_sum += rit.first;
    }
    for (auto cit : container.transposed) {
      for (auto rit : cit.second) {
        value_sum += rit.second.value;
      }
    }
    done = true;
  });
  while (!done) {
    ;  // Spin lock.
  }
  EXPECT_EQ(48u, row_index_sum);
  EXPECT_EQ(6, value_sum);
}
