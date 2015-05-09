/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2015 Maxim Zhurovich <zhurovich@gmail.com>
          (c) 2015 Dmitry "Dima" Korolev <dmitry.korolev@gmail.com>

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

#ifndef SHERLOCK_YODA_API_MATRIX_H
#define SHERLOCK_YODA_API_MATRIX_H

#include <future>

#include "metaprogramming.h"

#include "../../types.h"
#include "../../metaprogramming.h"
#include "../../policy.h"
#include "../../exceptions.h"

namespace yoda {

// Exceptions for `MatrixEntry` type  of storage.
//
// Exception types for non-existent cells.
// Cover exception type for entry types and templated, narrowed down exception types, one per entry key type.
struct CellNotFoundCoverException : bricks::Exception {};

template <typename ENTRY>
struct CellNotFoundException : CellNotFoundCoverException {
  typedef ENTRY T_ENTRY;
  typedef ENTRY_ROW_TYPE<ENTRY> T_ROW;
  typedef ENTRY_COL_TYPE<ENTRY> T_COL;
  const T_ROW row;
  const T_COL col;
  explicit CellNotFoundException(const T_ROW& row, const T_COL& col) : row(row), col(col) {}
};

// Exception types for the existence of a particular cell being a runtime error.
// Cover exception type for all entry types and templated, narrowed down exception types, one per entry type.
struct CellAlreadyExistsCoverException : bricks::Exception {};

template <typename ENTRY>
struct CellAlreadyExistsException : CellAlreadyExistsCoverException {
  typedef ENTRY T_ENTRY;
  const ENTRY entry;
  explicit CellAlreadyExistsException(const ENTRY& entry) : entry(entry) {}
};

// The implementation for the logic of `allow_nonthrowing_get` for `MatrixEntry`
// type.
template <typename T_ENTRY, typename T_CELL_NOT_FOUND_EXCEPTION, bool>
struct MatrixEntrySetPromiseToNullEntryOrThrow {};

template <typename T_ENTRY, typename T_CELL_NOT_FOUND_EXCEPTION>
struct MatrixEntrySetPromiseToNullEntryOrThrow<T_ENTRY, T_CELL_NOT_FOUND_EXCEPTION, false> {
  typedef ENTRY_ROW_TYPE<T_ENTRY> T_ROW;
  typedef ENTRY_COL_TYPE<T_ENTRY> T_COL;
  static void DoIt(const T_ROW& row, const T_COL& col, std::promise<T_ENTRY>& pr) {
    pr.set_exception(std::make_exception_ptr(T_CELL_NOT_FOUND_EXCEPTION(row, col)));
  }
};

template <typename T_ENTRY, typename UNUSED_T_CELL_NOT_FOUND_EXCEPTION>
struct MatrixEntrySetPromiseToNullEntryOrThrow<T_ENTRY, UNUSED_T_CELL_NOT_FOUND_EXCEPTION, true> {
  typedef ENTRY_ROW_TYPE<T_ENTRY> T_ROW;
  typedef ENTRY_COL_TYPE<T_ENTRY> T_COL;
  static void DoIt(const T_ROW& row, const T_COL& col, std::promise<T_ENTRY>& pr) {
    T_ENTRY null_entry(NullEntry);
    SetRow(null_entry, row);
    SetCol(null_entry, col);
    pr.set_value(null_entry);
  }
};

// TODO(dkorolev): Rename `ActualContainer` into `Container` and move it up to `../..`.
template <typename ENTRY>
struct ActualContainer {};

// User type interface: Use `MatrixEntry<MyMatrixEntry>` in Yoda's type list for required storage types
// for Yoda to support key-entry (key-value) accessors over the type `MyMatrixEntry`.
template <typename ENTRY>
struct MatrixEntry {
  typedef ENTRY T_ENTRY;
  typedef ENTRY_ROW_TYPE<T_ENTRY> T_ROW;
  typedef ENTRY_COL_TYPE<T_ENTRY> T_COL;

  typedef std::function<void(const T_ENTRY&)> T_ENTRY_CALLBACK;
  typedef std::function<void(const T_ROW&, const T_COL&)> T_CELL_CALLBACK;
  typedef std::function<void()> T_VOID_CALLBACK;
  typedef std::function<void(const ActualContainer<MatrixEntry<ENTRY>>&)> T_USER_FUNCTION;

  typedef CellNotFoundException<T_ENTRY> T_CELL_NOT_FOUND_EXCEPTION;
  typedef CellAlreadyExistsException<T_ENTRY> T_CELL_ALREADY_EXISTS_EXCEPTION;
  typedef EntryShouldExistException<T_ENTRY> T_ENTRY_SHOULD_EXIST_EXCEPTION;
};

template <typename ENTRY>
struct ActualContainer<MatrixEntry<ENTRY>> {
  typedef MatrixEntry<ENTRY> YET;  // "Yoda entry type".

  // TODO(dkorolev)+TODO(mzhurovich): Eventually we'll think of storing each entry only once.
  T_MAP_TYPE<typename YET::T_ROW, T_MAP_TYPE<typename YET::T_COL, typename YET::T_ENTRY>> forward;
  T_MAP_TYPE<typename YET::T_COL, T_MAP_TYPE<typename YET::T_ROW, typename YET::T_ENTRY>> transposed;
};

template <typename YT, typename ENTRY>
struct YodaImpl<YT, MatrixEntry<ENTRY>> {
  static_assert(std::is_base_of<YodaTypesBase, YT>::value, "");
  typedef MatrixEntry<ENTRY> YET;  // "Yoda entry type".

  YodaImpl() = delete;
  explicit YodaImpl(typename YT::T_MQ& mq) : mq_(mq) {}

  struct MQMessageGet : YodaMMQMessage<YT> {
    const typename YET::T_ROW row;
    const typename YET::T_COL col;
    std::promise<typename YET::T_ENTRY> pr;
    typename YET::T_ENTRY_CALLBACK on_success;
    typename YET::T_CELL_CALLBACK on_failure;

    explicit MQMessageGet(const typename YET::T_ROW& row,
                          const typename YET::T_COL& col,
                          std::promise<typename YET::T_ENTRY>&& pr)
        : row(row), col(col), pr(std::move(pr)) {}
    explicit MQMessageGet(const typename YET::T_ROW& row,
                          const typename YET::T_COL& col,
                          typename YET::T_ENTRY_CALLBACK on_success,
                          typename YET::T_CELL_CALLBACK on_failure)
        : row(row), col(col), on_success(on_success), on_failure(on_failure) {}
    virtual void Process(YodaContainer<YT>& container, typename YT::T_STREAM_TYPE&) override {
      container(std::ref(*this));
    }
  };

  struct MQMessageAdd : YodaMMQMessage<YT> {
    const typename YET::T_ENTRY e;
    std::promise<void> pr;
    typename YET::T_VOID_CALLBACK on_success;
    typename YET::T_VOID_CALLBACK on_failure;

    explicit MQMessageAdd(const typename YET::T_ENTRY& e, std::promise<void>&& pr) : e(e), pr(std::move(pr)) {}
    explicit MQMessageAdd(const typename YET::T_ENTRY& e,
                          typename YET::T_VOID_CALLBACK on_success,
                          typename YET::T_VOID_CALLBACK on_failure)
        : e(e), on_success(on_success), on_failure(on_failure) {}

    // Important note: The entry added will eventually reach the storage via the stream.
    // Thus, in theory, `MQMessageAdd::Process()` could be a no-op.
    // This code still updates the storage, to have the API appear more lively to the user.
    // Since the actual implementation of `Add` pushes the `MQMessageAdd` message before publishing
    // an update to the stream, the final state will always be [eventually] consistent.
    // The practical implication here is that an API `Get()` after an api `Add()` may and will return data,
    // that might not yet have reached the storage, and thus relying on the fact that an API `Get()` call
    // reflects updated data is not reliable from the point of data synchronization.
    virtual void Process(YodaContainer<YT>& container, typename YT::T_STREAM_TYPE& stream) override {
      container(std::ref(*this), std::ref(stream));
    }
  };

  struct MQMessageFunction : YodaMMQMessage<YT> {
    const typename YET::T_USER_FUNCTION function;

    explicit MQMessageFunction(const typename YET::T_USER_FUNCTION function) : function(function) {}

    virtual void Process(YodaContainer<YT>& container, typename YT::T_STREAM_TYPE&) override {
      container(std::ref(*this));
    }
  };

  std::future<typename YET::T_ENTRY> operator()(apicalls::AsyncGet,
                                                const typename YET::T_ROW& row,
                                                const typename YET::T_COL& col) {
    std::promise<typename YET::T_ENTRY> pr;
    std::future<typename YET::T_ENTRY> future = pr.get_future();
    mq_.EmplaceMessage(new MQMessageGet(row, col, std::move(pr)));
    return future;
  }

  void operator()(apicalls::AsyncGet,
                  const typename YET::T_ROW& row,
                  const typename YET::T_COL& col,
                  typename YET::T_ENTRY_CALLBACK on_success,
                  typename YET::T_CELL_CALLBACK on_failure) {
    mq_.EmplaceMessage(new MQMessageGet(row, col, on_success, on_failure));
  }

  typename YET::T_ENTRY operator()(apicalls::Get,
                                   const typename YET::T_ROW& row,
                                   const typename YET::T_COL& col) {
    return operator()(apicalls::AsyncGet(),
                      std::forward<const typename YET::T_ROW>(row),
                      std::forward<const typename YET::T_COL>(col)).get();
  }

  std::future<void> operator()(apicalls::AsyncAdd, const typename YET::T_ENTRY& entry) {
    std::promise<void> pr;
    std::future<void> future = pr.get_future();

    mq_.EmplaceMessage(new MQMessageAdd(entry, std::move(pr)));
    return future;
  }

  void operator()(apicalls::AsyncAdd,
                  const typename YET::T_ENTRY& entry,
                  typename YET::T_VOID_CALLBACK on_success,
                  typename YET::T_VOID_CALLBACK on_failure = []() {}) {
    mq_.EmplaceMessage(new MQMessageAdd(entry, on_success, on_failure));
  }

  void operator()(apicalls::Add, const typename YET::T_ENTRY& entry) {
    operator()(apicalls::AsyncAdd(), entry).get();
  }

  void operator()(apicalls::AsyncCallFunction, const typename YET::T_USER_FUNCTION function) {
    mq_.EmplaceMessage(new MQMessageFunction(function));
  }

 private:
  typename YT::T_MQ& mq_;
};

template <typename YT, typename ENTRY>
struct Container<YT, MatrixEntry<ENTRY>> {
  static_assert(std::is_base_of<YodaTypesBase, YT>::value, "");
  typedef MatrixEntry<ENTRY> YET;

  ActualContainer<MatrixEntry<ENTRY>> container;

  // Event: The entry has been scanned from the stream.
  void operator()(const typename YET::T_ENTRY& entry) {
    container.forward[GetRow(entry)][GetCol(entry)] = entry;
    container.transposed[GetCol(entry)][GetRow(entry)] = entry;
  }

  // Event: `Get()`.
  void operator()(typename YodaImpl<YT, MatrixEntry<typename YET::T_ENTRY>>::MQMessageGet& msg) {
    bool cell_exists = false;
    const auto rit = container.forward.find(msg.row);
    if (rit != container.forward.end()) {
      const auto cit = rit->second.find(msg.col);
      if (cit != rit->second.end()) {
        cell_exists = true;
        // The entry has been found.
        if (msg.on_success) {
          // Callback semantics.
          msg.on_success(cit->second);
        } else {
          // Promise semantics.
          msg.pr.set_value(cit->second);
        }
      }
    }
    if (!cell_exists) {
      // The entry has not been found.
      if (msg.on_failure) {
        // Callback semantics.
        msg.on_failure(msg.row, msg.col);
      } else {
        // Promise semantics.
        MatrixEntrySetPromiseToNullEntryOrThrow<typename YET::T_ENTRY,
                                                typename YET::T_CELL_NOT_FOUND_EXCEPTION,
                                                false  // Was `T_POLICY::allow_nonthrowing_get>::DoIt(key, pr);`
                                                >::DoIt(msg.row, msg.col, msg.pr);
      }
    }
  }

  // Event: `Add()`.
  void operator()(typename YodaImpl<YT, MatrixEntry<typename YET::T_ENTRY>>::MQMessageAdd& msg,
                  typename YT::T_STREAM_TYPE& stream) {
    bool cell_exists = false;
    const auto rit = container.forward.find(GetRow(msg.e));
    if (rit != container.forward.end()) {
      cell_exists = static_cast<bool>(rit->second.count(GetCol(msg.e)));
    }
    if (cell_exists) {
      if (msg.on_failure) {  // Callback function defined.
        msg.on_failure();
      } else {  // Throw.
        msg.pr.set_exception(std::make_exception_ptr(typename YET::T_CELL_ALREADY_EXISTS_EXCEPTION(msg.e)));
      }
    } else {
      container.forward[GetRow(msg.e)][GetCol(msg.e)] = msg.e;
      container.transposed[GetCol(msg.e)][GetRow(msg.e)] = msg.e;
      stream.Publish(msg.e);
      if (msg.on_success) {
        msg.on_success();
      } else {
        msg.pr.set_value();
      }
    }
  }

  // Event: `Function()`.
  void operator()(typename YodaImpl<YT, MatrixEntry<typename YET::T_ENTRY>>::MQMessageFunction& msg) {
    msg.function(container);
  }
};

}  // namespace yoda

#endif  // SHERLOCK_YODA_API_MATRIX_H