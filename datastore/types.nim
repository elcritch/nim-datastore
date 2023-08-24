import std/streams
export streams

const
  FileExt* = "dsobj"
  EmptyBytes* = newSeq[byte](0)

type
  DatastoreError* = object of CatchableError
  DatastoreKeyNotFound* = object of DatastoreError

  Datastore* = ref object of RootObj

type
  Datastream* = ref object of StringStreamObj ##\
    ## Datastream type -- currently just a shim around StringStream

proc new*(x: typedesc[Datastream], data: sink string): Datastream =
  let res = new x
  var ss = newStringStream()
  res.data = data
  ## todo swap with UncheckedPtr setup
  res.setPositionImpl = ss.setPositionImpl
  res.getPositionImpl = ss.getPositionImpl
  res.readDataStrImpl = ss.readDataStrImpl
  res.closeImpl = ss.closeImpl
  res.atEndImpl = ss.atEndImpl
  when nimvm:
    discard
  else:
    result.readDataImpl = ss.readDataImpl
    result.peekDataImpl = ss.peekDataImpl
    result.writeDataImpl = ss.writeDataImpl

proc new*(_: typedesc[Datastream], data: openArray[byte]): Datastream =
  var str = newStringOfCap(data.len)
  copyMem(addr str[0], unsafeAddr data[0], data.len)
  result = Datastream.new(str)

proc new*(_: typedesc[Datastream], cap: int = 0): Datastream =
  result = Datastream.new(newStringOfCap(cap))


proc len*(dss: Datastream): int {.raises: [].} =
  try:
    dss.getPosition()
  except CatchableError as exc:
    # TODO: temporary check
    raise (ref Defect)(msg: exc.msg)


