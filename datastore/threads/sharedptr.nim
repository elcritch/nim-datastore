#            Nim's Runtime Library
#        (c) Copyright 2021 Nim contributors

import std/atomics
import std/isolation
import std/typetraits
import std/strutils

export isolation

proc raiseNilAccess() {.noinline.} =
  raise newException(NilAccessDefect, "dereferencing nil smart pointer")

template checkNotNil(p: typed) =
  when compileOption("boundChecks"):
    {.line.}:
      if p.isNil:
        raiseNilAccess()

type
  SharedPtr*[T] = object
    ## Shared ownership reference counting pointer.
    cnt: ptr int
    buf*: ptr T

proc incr*[T](a: SharedPtr[T]) =
  let res = atomicAddFetch(a.cnt, 1, ATOMIC_RELAXED)
  echo "SIGNAL: manual incr: ", res

proc decr*[T](x: SharedPtr[T]) =
  if x.buf != nil and x.cnt != nil:
    let res = atomicSubFetch(x.cnt, 1, ATOMIC_ACQUIRE)
    if res == 0:
      echo "SIGNAL: FREE: ", repr x.buf.pointer, " ", x.cnt[]
      deallocShared(x.buf)
      deallocShared(x.cnt)
    else:
      echo "SIGNAL: decr: ", repr x.buf.pointer, " ", x.cnt[]

proc `=destroy`*[T](x: var SharedPtr[T]) =
  echo "SIGNAL: destroy: ", repr x.buf.pointer, " ", x.cnt.repr
  echo "SIGNAL: destroy:st: ", ($getStackTrace()).split("\n").join(";")
  decr(x)

proc `=dup`*[T](src: SharedPtr[T]): SharedPtr[T] =
  if src.val != nil:
    discard fetchAdd(src.cnt, 1, ATOMIC_RELAXED)
  result.val = src.val

proc `=copy`*[T](dest: var SharedPtr[T], src: SharedPtr[T]) =
  if src.val != nil:
    # echo "SharedPtr: copy: ", src.val.pointer.repr
    discard fetchAdd(src.val.counter, 1, ATOMIC_RELAXED)
  `=destroy`(dest)
  dest.val = src.val

proc newSharedPtr*[T](val: sink Isolated[T]): SharedPtr[T] {.nodestroy.} =
  ## Returns a shared pointer which shares
  ## ownership of the object by reference counting.
  result.val = cast[typeof(result.val)](allocShared(sizeof(result.val[])))
  int(result.val.counter) = 1
  result.val.value = extract val
  echo "SharedPtr: alloc: ", result.val.pointer.repr, " tp: ", $(typeof(T))

template newSharedPtr*[T](val: T): SharedPtr[T] =
  newSharedPtr(isolate(val))

proc newSharedPtr*[T](t: typedesc[T]): SharedPtr[T] =
  ## Returns a shared pointer. It is not initialized,
  ## so reading from it before writing to it is undefined behaviour!
  result.val = cast[typeof(result.val)](allocShared0(sizeof(result.val[])))
  int(result.val.counter) = 0
  echo "SharedPtr: allocT: ", result.val.pointer.repr, " tp: ", $(typeof(T))

proc isNil*[T](p: SharedPtr[T]): bool {.inline.} =
  p.val == nil

proc `[]`*[T](p: SharedPtr[T]): var T {.inline.} =
  checkNotNil(p)
  p.val.value

proc `[]=`*[T](p: SharedPtr[T], val: sink Isolated[T]) {.inline.} =
  checkNotNil(p)
  p.val.value = extract val

template `[]=`*[T](p: SharedPtr[T]; val: T) =
  `[]=`(p, isolate(val))

proc `$`*[T](p: SharedPtr[T]): string {.inline.} =
  if p.val == nil: "nil"
  else: "(val: " & $p.val.value & ")"