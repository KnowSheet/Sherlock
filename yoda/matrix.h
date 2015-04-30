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

#ifndef SHERLOCK_YODA_MATRIX_H
#define SHERLOCK_YODA_MATRIX_H

#include <atomic>
#include <future>
#include <string>

#include "types.h"
#include "policy.h"
#include "exceptions.h"

#include "../sherlock.h"

#include "../../Bricks/mq/inmemory/mq.h"

namespace yoda {

// Here the data is stored. 
template <typename ENTRY>
struct Container<MatrixEntry<ENTRY>> {
  typedef ENTRY T_ENTRY;
  typedef ENTRY_ROW_TYPE<T_ENTRY> T_ROW;
  typedef ENTRY_COL_TYPE<T_ENTRY> T_COL;

  T_MAP_TYPE<T_ROW, T_MAP_TYPE<T_COL, T_ENTRY>> forward;
  T_MAP_TYPE<T_COL, T_MAP_TYPE<T_ROW, T_ENTRY>> transposed;
};

// Base type for messages specific for matrix entries.
template <typename ENTRY>
struct MQMessage<MatrixEntry<ENTRY>> {
  typedef sherlock::StreamInstance<ENTRY> T_STREAM_TYPE;
  virtual void DoIt(Container<MatrixEntry<ENTRY>>& container, T_STREAM_TYPE& stream) = 0;
};

template <typename ENTRY>
struct MQListener<MatrixEntry<ENTRY>> {
  typedef sherlock::StreamInstance<ENTRY> T_STREAM_TYPE;

  explicit MQListener(Container<MatrixEntry<ENTRY>>& container, T_STREAM_TYPE& stream)
      : container_(container), stream_(stream) {}

  // MMQ consumer call.
  void OnMessage(std::unique_ptr<MQMessage<MatrixEntry<ENTRY>>>& message, size_t dropped_count) {
    // TODO(dkorolev): Should use a non-dropping MMQ here, of course.
    static_cast<void>(dropped_count);  // TODO(dkorolev): And change the method's signature to remove this.
    message->DoIt(container_, stream_);
  }

  Container<MatrixEntry<ENTRY>>& container_;
  T_STREAM_TYPE& stream_;
};

template <typename ENTRY>
struct SherlockListener<MatrixEntry<ENTRY>> {
  typedef ENTRY T_ENTRY;
  typedef MMQ<MQListener<MatrixEntry<ENTRY>>, std::unique_ptr<MQMessage<MatrixEntry<ENTRY>>>> T_MQ;

  explicit SherlockListener(T_MQ& mq) : caught_up_(false), entries_seen_(0u), mq_(mq) {}

  struct MQMessageEntry : MQMessage<MatrixEntry<ENTRY>> {
    // TODO(dkorolev): A single entry is fine to copy, polymorphic ones should be std::move()-d.
    using typename MQMessage<MatrixEntry<ENTRY>>::T_STREAM_TYPE;
    T_ENTRY entry;

    explicit MQMessageEntry(const T_ENTRY& entry) : entry(entry) {}

    virtual void DoIt(Container<MatrixEntry<ENTRY>>& container, T_STREAM_TYPE&) override {
      // TODO(max+dima): Ensure that this storage update can't break
      // the actual state of the data.
      container.forward[GetRow(entry)][GetCol(entry)] = entry;
      container.transposed[GetCol(entry)][GetRow(entry)] = entry;
    }
  };

  // Sherlock stream listener call.
  bool Entry(T_ENTRY& entry, size_t index, size_t total) {
    // The logic of this API implementation is:
    // * Defer all API requests until the persistent part of the stream is fully replayed,
    // * Allow all API requests after that.

    // TODO(dkorolev): If that's the way to go, we should probably respond with HTTP 503 or 409 or 418?
    //                 (And add a `/statusz` endpoint to monitor the status wrt ready / not yet ready.)
    // TODO(dkorolev): What about an empty stream? :-)

    // Non-polymorphic usecase.
    // TODO(dkorolev): Eliminate the copy && code up the polymorphic scenario for this call. In another class.
    mq_.EmplaceMessage(new MQMessageEntry(entry));

    // This is primarily for unit testing purposes.
    ++entries_seen_;

    if (index + 1 == total) {
      caught_up_ = true;
    }

   return true;
  }

  // Sherlock stream listener call.
  void Terminate() {
    // TODO(dkorolev): Should stop serving API requests.
    // TODO(dkorolev): Should un-register HTTP endpoints, if they have been registered.
  }

  std::atomic_bool caught_up_;
  std::atomic_size_t entries_seen_;

 private:
  T_MQ& mq_;
};

template <typename ENTRY>
struct Storage<MatrixEntry<ENTRY>> {
  typedef ENTRY T_ENTRY;
  typedef ENTRY_ROW_TYPE<T_ENTRY> T_ROW;
  typedef ENTRY_COL_TYPE<T_ENTRY> T_COL;

  typedef std::function<void(const T_ENTRY&)> T_ENTRY_CALLBACK;
  typedef std::function<void(const T_ROW&, const T_COL&)> T_CELL_CALLBACK;
  typedef std::function<void()> T_VOID_CALLBACK;
  typedef std::function<void(const Container<MatrixEntry<ENTRY>>&)> T_USER_FUNCTION;

  typedef CellNotFoundException<T_ENTRY> T_CELL_NOT_FOUND_EXCEPTION;
  typedef CellAlreadyExistsException<T_ENTRY> T_CELL_ALREADY_EXISTS_EXCEPTION;
  typedef EntryShouldExistException<T_ENTRY> T_ENTRY_SHOULD_EXIST_EXCEPTION;

  typedef MMQ<MQListener<MatrixEntry<ENTRY>>, std::unique_ptr<MQMessage<MatrixEntry<ENTRY>>>> T_MQ;

  explicit Storage(T_MQ& mq) : mq_(mq) {}

  struct MQMessageGet : MQMessage<MatrixEntry<ENTRY>> {
    using typename MQMessage<MatrixEntry<ENTRY>>::T_STREAM_TYPE;

    const T_ROW row;
    const T_COL col;
    std::promise<T_ENTRY> pr;
    T_ENTRY_CALLBACK on_success;
    T_CELL_CALLBACK on_failure;

    explicit MQMessageGet(const T_ROW& row, const T_COL& col, std::promise<T_ENTRY>&& pr)
        : row(row), col(col), pr(std::move(pr)) {}
    explicit MQMessageGet(const T_ROW& row, const T_COL& col, T_ENTRY_CALLBACK on_success, T_CELL_CALLBACK on_failure)
        : row(row), col(col), on_success(on_success), on_failure(on_failure) {}

    virtual void DoIt(Container<MatrixEntry<ENTRY>>& container, T_STREAM_TYPE&) override {
      bool cell_exists = false;
      const auto rit = container.forward.find(row);
      if (rit != container.forward.end()) {
        const auto cit = rit->second.find(col);
        if (cit != rit->second.end()) {
          cell_exists = true;
          // The entry has been found.
          if (on_success) {
            // Callback semantics.
            on_success(cit->second);
          } else {
            // Promise semantics.
            pr.set_value(cit->second);
          }
        }
      }
      if (!cell_exists) {
        // The entry has not been found.
        if (on_failure) {
          // Callback semantics.
          on_failure(row, col);
        } else {
          // Promise semantics.
          MatrixEntrySetPromiseToNullEntryOrThrow<T_ENTRY, T_CELL_NOT_FOUND_EXCEPTION,
                                                  false  // Was `T_POLICY::allow_nonthrowing_get>::DoIt(key, pr);`
                                                  >::DoIt(row, col, pr);
        }
      }
    }
  };

  struct MQMessageAdd : MQMessage<MatrixEntry<ENTRY>> {
    using typename MQMessage<MatrixEntry<ENTRY>>::T_STREAM_TYPE;

    const T_ENTRY e;
    std::promise<void> pr;
    T_VOID_CALLBACK on_success;
    T_VOID_CALLBACK on_failure;

    explicit MQMessageAdd(const T_ENTRY& e, std::promise<void>&& pr) : e(e), pr(std::move(pr)) {}
    explicit MQMessageAdd(const T_ENTRY& e, T_VOID_CALLBACK on_success, T_VOID_CALLBACK on_failure)
        : e(e), on_success(on_success), on_failure(on_failure) {}

    // Important note: The entry added will eventually reach the storage via the stream.
    // Thus, in theory, `MQMessageAdd::DoIt()` could be a no-op.
    // This code still updates the storage, to have the API appear more lively to the user.
    // Since the actual implementation of `Add` pushes the `MQMessageAdd` message before publishing
    // an update to the stream, the final state will always be [evenually] consistent.
    // The practical implication here is that an API `Get()` after an api `Add()` may and will return data,
    // that might not yet have reached the storage, and thus relying on the fact that an API `Get()` call
    // reflects updated data is not reliable from the point of data synchronization.
    virtual void DoIt(Container<MatrixEntry<ENTRY>>& container, T_STREAM_TYPE& stream) override {
      bool cell_exists = false;
      const auto rit = container.forward.find(GetRow(e));
      if (rit != container.forward.end()) {
        cell_exists = static_cast<bool>(rit->second.count(GetCol(e)));
      }
      if (cell_exists) {
        if (on_failure) {  // Callback function defined.
          on_failure();
        } else {  // Throw.
          pr.set_exception(std::make_exception_ptr(T_CELL_ALREADY_EXISTS_EXCEPTION(e)));
        }
      } else {
        container.forward[GetRow(e)][GetCol(e)] = e;
        container.transposed[GetCol(e)][GetRow(e)] = e;
        stream.Publish(e);
        if (on_success) {
          on_success();
        } else {
          pr.set_value();
        }
      }
    }
  };

  struct MQMessageFunction : MQMessage<MatrixEntry<ENTRY>> {
    using typename MQMessage<MatrixEntry<ENTRY>>::T_STREAM_TYPE;

    T_USER_FUNCTION function;

    explicit MQMessageFunction(T_USER_FUNCTION function) : function(function) {}

    virtual void DoIt(Container<MatrixEntry<ENTRY>>& container, T_STREAM_TYPE&) override {
      function(container);
    }
  };

  std::future<T_ENTRY> AsyncGet(const T_ROW& row, const T_COL& col) {
    std::promise<T_ENTRY> pr;
    std::future<T_ENTRY> future = pr.get_future();
    mq_.EmplaceMessage(new MQMessageGet(row, col, std::move(pr)));
    return future;
  }

  void AsyncGet(const T_ROW& row, const T_COL& col, T_ENTRY_CALLBACK on_success, T_CELL_CALLBACK on_failure) {
    mq_.EmplaceMessage(new MQMessageGet(row, col, on_success, on_failure));
  }

  T_ENTRY Get(const T_ROW& row, const T_COL& col) { 
    return AsyncGet(std::forward<const T_ROW>(row), std::forward<const T_COL>(col)).get(); 
  }

  std::future<void> AsyncAdd(const T_ENTRY& entry) {
    std::promise<void> pr;
    std::future<void> future = pr.get_future();

    mq_.EmplaceMessage(new MQMessageAdd(entry, std::move(pr)));
    return future;
  }

  void AsyncAdd(const T_ENTRY& entry,
                T_VOID_CALLBACK on_success,
                T_VOID_CALLBACK on_failure = [](const T_ROW&, const T_COL&) {}) {
    mq_.EmplaceMessage(new MQMessageAdd(entry, on_success, on_failure));
  }

  void Add(const T_ENTRY& entry) { AsyncAdd(entry).get(); }

  void AsyncCallFunction(T_USER_FUNCTION function) {
    mq_.EmplaceMessage(new MQMessageFunction(function));
  }

 private:
  T_MQ& mq_;
};

template <typename ENTRY>
class API<MatrixEntry<ENTRY>> : public Storage<MatrixEntry<ENTRY>> {
 public:
  typedef ENTRY T_ENTRY;
  typedef ENTRY_ROW_TYPE<T_ENTRY> T_ROW;
  typedef ENTRY_COL_TYPE<T_ENTRY> T_COL;

  typedef sherlock::StreamInstance<T_ENTRY> T_STREAM_TYPE;
  typedef MMQ<MQListener<MatrixEntry<ENTRY>>, std::unique_ptr<MQMessage<MatrixEntry<ENTRY>>>> T_MQ;

  template <typename F>
  using T_STREAM_LISTENER_TYPE = typename sherlock::StreamInstanceImpl<T_ENTRY>::template ListenerScope<F>;

  API(const std::string& stream_name)
      : Storage<MatrixEntry<ENTRY>>(mq_),
        stream_(sherlock::Stream<T_ENTRY>(stream_name)),
        mq_listener_(container_, stream_),
        mq_(mq_listener_),
        sherlock_listener_(mq_),
        listener_scope_(stream_.Subscribe(sherlock_listener_)) {}

  T_STREAM_TYPE& UnsafeStream() { return stream_; }

  template <typename F>
  T_STREAM_LISTENER_TYPE<F> Subscribe(F& listener) {
    return std::move(stream_.Subscribe(listener));
  }

  // For testing purposes.
  bool CaughtUp() const { return sherlock_listener_.caught_up_; }
  size_t EntriesSeen() const { return sherlock_listener_.entries_seen_; }

 private:
  API() = delete;

  T_STREAM_TYPE stream_;
  Container<MatrixEntry<ENTRY>> container_;
  MQListener<MatrixEntry<ENTRY>> mq_listener_;
  T_MQ mq_;
  SherlockListener<MatrixEntry<ENTRY>> sherlock_listener_;
  T_STREAM_LISTENER_TYPE<SherlockListener<MatrixEntry<ENTRY>>> listener_scope_;
};

}  // namespace yoda

#endif  // SHERLOCK_YODA_MATRIX_H
