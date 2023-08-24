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
  DataStream* = ref object of StringStreamObj ##\
    ## DataStream type -- currently just a shim around StringStream

proc new*(x: typedesc[DataStream], data: sink string): DataStream =
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

proc new*(_: typedesc[DataStream], cap: int = 0): DataStream =
  result = DataStream.new(newStringOfCap(cap))

proc len*(dss: DataStream): int {.raises: [].} =
  try:
    dss.getPosition()
  except CatchableError as exc:
    # TODO: temporary check
    raise (ref Defect)(msg: exc.msg)


