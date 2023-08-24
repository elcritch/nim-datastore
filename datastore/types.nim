import std/streams
export streams

const
  FileExt* = "dsobj"

type
  DatastoreError* = object of CatchableError
  DatastoreKeyNotFound* = object of DatastoreError

  Datastore* = ref object of RootObj

  Datastream* = ref object of StringStreamObj ##\
    ## Datastream type -- currently just a shim around StringStream

proc new*(x: typedesc[Datastream], data: sink string): Datastream =
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

proc new*(_: typedesc[Datastream], data: seq[byte]): Datastream =
  var str = newStringOfCap(data.len)
  copyMem(addr str[0], unsafeAddr data[0], data.len)
  result = Datastream.new(str)

proc new*(_: typedesc[Datastream], cap: int = 0): Datastream =
  var ss = newStringOfCap(cap)
  result = Datastream.new(move ss)

proc len*(dss: Datastream): int {.raises: [].} =
  try:
    dss.getPosition()
  except CatchableError as exc:
    # TODO: temporary check
    raise (ref Defect)(msg: exc.msg)


const
  EmptyBytes* = Datastream.new ""

