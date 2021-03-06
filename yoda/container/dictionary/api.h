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

// The implementation for `Dictionary` storage type.

#ifndef SHERLOCK_YODA_CONTAINER_DICTIONARY_API_H
#define SHERLOCK_YODA_CONTAINER_DICTIONARY_API_H

#include <future>

#include "exceptions.h"
#include "metaprogramming.h"

#include "../../metaprogramming.h"
#include "../../types.h"

#include "../../../../Bricks/template/pod.h"

namespace yoda {

using sfinae::ENTRY_KEY_TYPE;
using sfinae::T_MAP_TYPE;
using sfinae::GetKey;

// User type interface: Use `Dictionary<MyEntry>` in Yoda's type list for required storage types
// for Yoda to support key-entry (key-value) accessors over the type `MyEntry`.
template <typename ENTRY>
struct Dictionary {
  static_assert(std::is_base_of<Padawan, ENTRY>::value, "Entry type must be derived from `yoda::Padawan`.");

  typedef ENTRY T_ENTRY;
  typedef ENTRY_KEY_TYPE<T_ENTRY> T_KEY;

  typedef std::function<void(const T_ENTRY&)> T_ENTRY_CALLBACK;
  typedef std::function<void(const T_KEY&)> T_KEY_CALLBACK;
  typedef std::function<void()> T_VOID_CALLBACK;

  typedef KeyNotFoundException<T_ENTRY> T_KEY_NOT_FOUND_EXCEPTION;
  typedef KeyAlreadyExistsException<T_ENTRY> T_KEY_ALREADY_EXISTS_EXCEPTION;

  template <typename DATA>
  static decltype(std::declval<DATA>().template Accessor<Dictionary<ENTRY>>()) Accessor(DATA&& c) {
    return c.template Accessor<Dictionary<ENTRY>>();
  }

  template <typename DATA>
  static decltype(std::declval<DATA>().template Mutator<Dictionary<ENTRY>>()) Mutator(DATA&& c) {
    return c.template Mutator<Dictionary<ENTRY>>();
  }
};

template <typename YT, typename ENTRY>
struct Container<YT, Dictionary<ENTRY>> {
  static_assert(std::is_base_of<YodaTypesBase, YT>::value, "");

  template <typename T>
  using CF = bricks::copy_free<T>;

  typedef Dictionary<ENTRY> YET;

  YET operator()(type_inference::template YETFromE<typename YET::T_ENTRY>);
  YET operator()(type_inference::template YETFromK<typename YET::T_KEY>);
  YET operator()(type_inference::template YETFromK<std::tuple<typename YET::T_KEY>>);
  YET operator()(type_inference::template YETFromSubscript<typename YET::T_KEY>);
  YET operator()(type_inference::template YETFromSubscript<std::tuple<typename YET::T_KEY>>);

  // Event: The entry has been scanned from the stream.
  // Save a copy! Stream provides copies of entries, that are desined to be `std::move()`-d away.
  // TODO(dkorolev): For this parameter to be an `ENTRY&&`, we'd need to clean up Bricks wrt RTTI.
  void operator()(ENTRY& entry, size_t index) {
    EntryWithIndex<ENTRY>& placeholder = map_[GetKey(entry)];
    if (index > placeholder.index) {
      placeholder.Update(index, std::move(entry));
    }
  }

  class Accessor {
   public:
    Accessor() = delete;
    explicit Accessor(const Container<YT, YET>& container) : immutable_(container) {}

    bool Exists(CF<typename YET::T_KEY> key) const { return immutable_.map_.count(key); }

    // Non-throwing getter. Returns a wrapped null entry if not found.
    const EntryWrapper<ENTRY> Get(CF<typename YET::T_KEY> key) const {
      const auto cit = immutable_.map_.find(key);
      if (cit != immutable_.map_.end()) {
        return EntryWrapper<ENTRY>(cit->second.entry);
      } else {
        return EntryWrapper<ENTRY>();
      }
    }

    const EntryWrapper<ENTRY> Get(const std::tuple<typename YET::T_KEY>& key) const {
      return Get(std::get<0>(key));
    }

    // Throwing getter.
    CF<ENTRY> operator[](CF<typename YET::T_KEY> key) const {
      const auto cit = immutable_.map_.find(key);
      if (cit != immutable_.map_.end()) {
        return cit->second.entry;
      } else {
        throw typename YET::T_KEY_NOT_FOUND_EXCEPTION(key);
      }
    }

    // Iteration.
    struct Iterator final {
      typedef decltype(std::declval<Container<YT, YET>>().map_.cbegin()) T_ITERATOR;
      T_ITERATOR iterator;
      explicit Iterator(T_ITERATOR&& iterator) : iterator(std::move(iterator)) {}
      void operator++() { ++iterator; }
      bool operator==(const Iterator& rhs) const { return iterator == rhs.iterator; }
      bool operator!=(const Iterator& rhs) const { return !operator==(rhs); }
      const ENTRY& operator*() const { return iterator->second.entry; }
      const ENTRY* operator->() const { return &iterator->second.entry; }
    };

    Iterator begin() const { return Iterator(immutable_.map_.cbegin()); }
    Iterator end() const { return Iterator(immutable_.map_.cend()); }
    size_t size() const { return immutable_.map_.size(); }
    bool empty() const { return immutable_.map_.empty(); }

   private:
    const Container<YT, YET>& immutable_;
  };

  class Mutator : public Accessor {
   public:
    Mutator() = delete;
    Mutator(Container<YT, YET>& container, typename YT::T_STREAM_TYPE& stream)
        : Accessor(container), mutable_(container), stream_(stream) {}

    // Non-throwing adder. Silently overwrites if already exists.
    void Add(const ENTRY& entry) {
      const size_t index = stream_.Publish(entry);
      mutable_.map_[GetKey(entry)].Update(index, entry);
    }
    void Add(const std::tuple<ENTRY>& entry) { Add(std::get<0>(entry)); }

    // Throwing adder.
    Mutator& operator<<(const ENTRY& entry) {
      // TODO(dkorolev): Make one, not two lookups in `map`.
      auto key = GetKey(entry);
      if (mutable_.map_.count(key)) {
        throw typename YET::T_KEY_ALREADY_EXISTS_EXCEPTION(std::move(key));
      } else {
        Add(entry);
        return *this;
      }
    }

   private:
    Container<YT, YET>& mutable_;
    typename YT::T_STREAM_TYPE& stream_;
  };

  Accessor operator()(type_inference::RetrieveAccessor<YET>) const { return Accessor(*this); }

  Mutator operator()(type_inference::RetrieveMutator<YET>, typename YT::T_STREAM_TYPE& stream) {
    return Mutator(*this, std::ref(stream));
  }

 private:
  T_MAP_TYPE<typename YET::T_KEY, EntryWithIndex<typename YET::T_ENTRY>> map_;
};

}  // namespace yoda

#endif  // SHERLOCK_YODA_CONTAINER_DICTIONARY_API_H
