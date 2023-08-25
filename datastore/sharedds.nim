import std/tables

import pkg/chronos
import pkg/chronos/threadsync
import pkg/questionable
import pkg/questionable/results
import pkg/upraises
import pkg/taskpools

import ./key
import ./query
import ./datastore
import ./threadbackend

export key, query

push: {.upraises: [].}

type

  SharedDatastore* = ref object of Datastore
    # stores*: Table[Key, SharedDatastore]
    tds: ThreadDatastorePtr

template newSignal(): auto =
  ThreadSignalPtr.new().valueOr:
    return failure newException(DatastoreError, "error creating signal")

method has*(
  self: SharedDatastore,
  key: Key
): Future[?!bool] {.async.} =
  return success(true)

method delete*(
  self: SharedDatastore,
  key: Key
): Future[?!void] {.async.} =
  return success()

method delete*(
  self: SharedDatastore,
  keys: seq[Key]
): Future[?!void] {.async.} =
  return success()

method get*(
  self: SharedDatastore,
  key: Key
): Future[?!seq[byte]] {.async.} =
  return success(newSeq[byte]())

method put*(
  self: SharedDatastore,
  key: Key,
  data: seq[byte]
): Future[?!void] {.async.} =

  let signal = ThreadSignalPtr.new().valueOr:
    return failure newException(DatastoreError, "error creating signal")

  await wait(signal)
  return success()

method put*(
  self: SharedDatastore,
  batch: seq[BatchEntry]
): Future[?!void] {.async.} =
  raiseAssert("Not implemented!")

method close*(
  self: SharedDatastore
): Future[?!void] {.async.} =

  # TODO: how to handle failed close?
  return success()

func new*[S: ref Datastore](
  T: typedesc[SharedDatastore],
  backend: ThreadBackend,
): ?!SharedDatastore =

  var
    self = SharedDatastore()
    signal = newSignal()
    res = TResult[ThreadDatastore].new()
  self.tds = ThreadDatastore.new(signal, backend, res)

  success self
