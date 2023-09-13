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
    val*: ptr T

proc incr*[T](a: var SharedPtr[T]) =
  if a.val != nil and a.cnt != nil:
    let res = atomicAddFetch(a.cnt, 1, ATOMIC_RELAXED)
    echo "SharedPtr: manual incr: ", res

proc decr*[T](x: var SharedPtr[T]) =
  if x.val != nil and x.cnt != nil:
    let res = atomicSubFetch(x.cnt, 1, ATOMIC_ACQUIRE)
    if res == 0:
      echo "SharedPtr: FREE: ", repr x.val.pointer, " ", x.cnt[], " tp: ", $(typeof(T))
      when compiles(`=destroy`(x.val)):
        echo "DECR FREE"
        `=destroy`(x.val)
      deallocShared(x.val)
      deallocShared(x.cnt)
      x.val = nil
      x.cnt = nil
    else:
      echo "SharedPtr: decr: ", repr x.val.pointer, " ", x.cnt[], " tp: ", $(typeof(T))

proc release*[T](x: var SharedPtr[T]) =
  x.decr()
  x.val = nil
  x.cnt = nil

proc `=destroy`*[T](x: var SharedPtr[T]) =
  if x.val != nil:
    echo "SharedPtr: destroy: ", repr x.val.pointer.repr, " cnt: ", x.cnt.pointer.repr, " tp: ", $(typeof(T))
  decr(x)

proc `=dup`*[T](src: SharedPtr[T]): SharedPtr[T] =
  if src.val != nil and src.cnt != nil:
    discard atomicAddFetch(src.cnt, 1, ATOMIC_RELAXED)
  result.val = src.val

proc `=copy`*[T](dest: var SharedPtr[T], src: SharedPtr[T]) =
  if src.val != nil and src.cnt != nil:
    # echo "SharedPtr: copy: ", src.val.pointer.repr
    discard atomicAddFetch(src.cnt, 1, ATOMIC_RELAXED)
  `=destroy`(dest)
  dest.val = src.val

proc newSharedPtr*[T](val: sink Isolated[T]): SharedPtr[T] {.nodestroy.} =
  ## Returns a shared pointer which shares,
  ## ownership of the object by reference counting.
  result.cnt = cast[ptr int](allocShared0(sizeof(result.cnt)))
  result.val = cast[typeof(result.val)](allocShared(sizeof(result.val[])))
  result.cnt[] = 0
  result.val[] = extract val
  echo "SharedPtr: alloc: ", result.val.pointer.repr, " tp: ", $(typeof(T))

template newSharedPtr*[T](val: T): SharedPtr[T] =
  newSharedPtr(isolate(val))

proc newSharedPtr*[T](t: typedesc[T]): SharedPtr[T] =
  ## Returns a shared pointer. It is not initialized,
  result.cnt = cast[ptr int](allocShared0(sizeof(result.cnt)))
  result.val = cast[typeof(result.val)](allocShared0(sizeof(result.val[])))
  result.cnt[] = 0
  echo "SharedPtr: alloc: ", result.val.pointer.repr, " tp: ", $(typeof(T))

proc isNil*[T](p: SharedPtr[T]): bool {.inline.} =
  p.val == nil

proc `[]`*[T](p: SharedPtr[T]): var T {.inline.} =
  checkNotNil(p)
  p.val[]

proc `[]=`*[T](p: SharedPtr[T], val: sink Isolated[T]) {.inline.} =
  checkNotNil(p)
  p.val[] = extract val

template `[]=`*[T](p: SharedPtr[T]; val: T) =
  `[]=`(p, isolate(val))

proc `$`*[T](p: SharedPtr[T]): string {.inline.} =
  if p.val == nil: "nil"
  else: "(val: " & $p.val[] & ")"
