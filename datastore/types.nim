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
  DataStream* = ref object of StringStreamObj
