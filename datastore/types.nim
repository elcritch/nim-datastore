import std/streams
export streams

const
  FileExt* = "dsobj"

type
  DatastoreError* = object of CatchableError
  DatastoreKeyNotFound* = object of DatastoreError

  Datastore* = ref object of RootObj

  DataStream* = ref object of StringStreamObj ##\
    ## DataStream type -- currently just a shim around StringStream

proc new*(x: typedesc[DataStream], data: sink string): DataStream =
  result.new()
  var ss = newStringStream()
  result.data = data
  ## todo swap with UncheckedPtr setup
  result.setPositionImpl = ss.setPositionImpl
  result.getPositionImpl = ss.getPositionImpl
  result.readDataStrImpl = ss.readDataStrImpl
  result.closeImpl = ss.closeImpl
  result.atEndImpl = ss.atEndImpl
  when nimvm:
    discard
  else:
    result.readDataImpl = ss.readDataImpl
    result.peekDataImpl = ss.peekDataImpl
    result.writeDataImpl = ss.writeDataImpl
  assert result.data.len == data.len

proc new*(_: typedesc[DataStream], cap: int = 0): DataStream =
  result = DataStream.new(newString(cap))

proc new*(_: typedesc[DataStream], data: openArray[byte]): DataStream =
  if data.len == 0:
    return DataStream.new(0)
  var str = newString(data.len)
  copyMem(addr str[0], unsafeAddr data[0], data.len)
  result = DataStream.new(move str)

proc len*(dss: DataStream): int {.raises: [], noSideEffect.} =
  try:
    {.cast(noSideEffect).}:
      dss.getPosition()
  except CatchableError as exc:
    # TODO: temporary check
    raise (ref Defect)(msg: exc.msg)

proc `==`*(a, b: DataStream): bool {.raises: [], noSideEffect.} =
  if unsafeAddr(a) == unsafeAddr(b):
    return true
  if a.isNil or b.isNil:
    return false
  if a.len == 0 and b.len == 0:
    return true
  let res =  a.len == b.len and a.data == b.data
  return res

template toOpenArray*(dss: DataStream): auto =
  cast[ptr UncheckedArray[byte]](unsafeAddr dss.data).toOpenArray(0, dss.len())

proc toSeq*(dss: DataStream): seq[byte] =
  result = newSeq[byte](dss.len())
  if dss.len() == 0:
    copyMem(addr result[0], addr dss.data[0], dss.len)

template toString*(dss: DataStream): string =
  dss.data

