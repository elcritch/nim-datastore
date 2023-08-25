# import std/atomics
import threading/smartptrs

type
  DataBufferHolder* = object
    buf: ptr UncheckedArray[byte]
    size: int
  
  DataBuffer* = SharedPtr[DataBufferHolder] ##\
    ## A fixed length data buffer using a SharedPtr.
    ## It is thread safe even with `refc` since
    ## it doesn't use string or seq types internally.
    ## 

  KeyBuffer* = DataBuffer
  ValueBuffer* = DataBuffer
  StringBuffer* = DataBuffer
  CatchableErrorBuffer* = object
    msg: StringBuffer

proc `=destroy`*(x: var DataBufferHolder) =
  ## copy pointer implementation
  if x.buf != nil:
    when isMainModule or true:
      echo "buffer: FREE: ", repr x.buf.pointer
    deallocShared(x.buf)

proc len*(a: DataBuffer): int = a[].size

proc new*(tp: typedesc[DataBuffer], size: int = 0): DataBuffer =
  ## allocate new buffer with given size
  newSharedPtr(DataBufferHolder(
    buf: cast[typeof(result[].buf)](allocShared0(size)),
    size: size,
  ))

proc new*[T: byte | char](tp: typedesc[DataBuffer], data: openArray[T]): DataBuffer =
  ## allocate new buffer and copies indata from openArray
  ## 
  result = DataBuffer.new(data.len)
  if data.len() > 0:
    copyMem(result[].buf, unsafeAddr data[0], data.len)

proc toSeq*[T: byte | char](a: DataBuffer, tp: typedesc[T]): seq[T] =
  ## convert buffer to a seq type using copy and either a byte or char
  result = newSeq[T](a.len)
  copyMem(addr result[0], unsafeAddr a[].buf[0], a.len)

proc toString*(data: DataBuffer): string =
  ## convert buffer to string type using copy
  result = newString(data.len())
  if data.len() > 0:
    copyMem(addr result[0], unsafeAddr data[].buf[0], data.len)

proc toCatchable*(err: CatchableErrorBuffer): ref CatchableError =
  ## convert back to a ref CatchableError
  result = (ref CatchableError)(msg: err.msg.toString())

proc toBuffer*(err: ref Exception): CatchableErrorBuffer =
  ## convert exception to an object with StringBuffer
  return CatchableErrorBuffer(
    msg: StringBuffer.new(err.msg)
  )
