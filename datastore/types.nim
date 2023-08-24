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

proc new*(_: typedesc[DataStream], data: seq[byte]): DataStream =
  var str = newStringOfCap(data.len)
  copyMem(addr str[0], unsafeAddr data[0], data.len)
  result = DataStream.new(str)

proc new*(_: typedesc[DataStream], cap: int = 0): DataStream =
  var ss = newStringOfCap(cap)
  result = DataStream.new(move ss)

proc len*(dss: DataStream): int {.raises: [].} =
  try:
    dss.getPosition()
  except CatchableError as exc:
    # TODO: temporary check
    raise (ref Defect)(msg: exc.msg)

template toOpenArray*(dss: DataStream): auto =
  dss.data.toOpenArray(0, dss.len)


