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

#ifndef SHERLOCK_YODA_EXCEPTIONS_H
#define SHERLOCK_YODA_EXCEPTIONS_H

#include "types.h"

#include "../../Bricks/exception.h"

namespace yoda {

// Exceptions for `KeyEntry` type of storage.
//
// Exception types for non-existent keys.
// Cover exception type for all entry types and templated, narrowed down exception types, one per entry type.
struct KeyNotFoundCoverException : bricks::Exception {};

template <typename ENTRY>
struct KeyNotFoundException : KeyNotFoundCoverException {
  typedef ENTRY T_ENTRY;
  typedef ENTRY_KEY_TYPE<ENTRY> T_KEY;
  const T_KEY key;
  explicit KeyNotFoundException(const T_KEY& key) : key(key) {}
};

// Exception types for the existence of a particular key being a runtime error.
// Cover exception type for all entry types and templated, narrowed down exception types, one per entry type.
struct KeyAlreadyExistsCoverException : bricks::Exception {};

template <typename ENTRY>
struct KeyAlreadyExistsException : KeyAlreadyExistsCoverException {
  typedef ENTRY T_ENTRY;
  const ENTRY entry;
  explicit KeyAlreadyExistsException(const ENTRY& entry) : entry(entry) {}
};

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

// Exceptions used for both `KeyEntry` and `MatrixEntry` types.
//
// Exception types for non-existence of a particular key being a runtime error.
// Cover exception type for all key types and templated, narrowed down exception types, one per entry key type.
struct EntryShouldExistCoverException : bricks::Exception {};

template <typename T_ENTRY>
struct EntryShouldExistException : EntryShouldExistCoverException {
  const T_ENTRY entry;
  explicit EntryShouldExistException(const T_ENTRY& entry) : entry(entry) {}
};

}  // namespace yoda

#endif  // SHERLOCK_YODA_EXCEPTIONS_H
