import std/sequtils

import pkg/chronos
import pkg/questionable
import pkg/questionable/results
from pkg/stew/results as stewResults import get, isErr
import pkg/upraises

import ./datastore

export datastore

push: {.upraises: [].}

type
  TieredDatastore* = ref object of Datastore
    stores: seq[Datastore]

proc new*(
  T: type TieredDatastore,
  stores: varargs[Datastore]): ?!T =

  if stores.len == 0:
    failure "stores must contain at least one Datastore"
  else:
    success T(stores: @stores)

proc stores*(self: TieredDatastore): seq[Datastore] =
  self.stores

method contains*(
  self: TieredDatastore,
  key: Key): Future[?!bool] {.async, locks: "unknown".} =

  for store in self.stores:
    let
      containsRes = await store.contains(key)

    if containsRes.isErr: return containsRes
    if containsRes.get == true: return success true

  return success false

method delete*(
  self: TieredDatastore,
  key: Key): Future[?!void] {.async, locks: "unknown".} =

  let
    pending = await allFinished(self.stores.mapIt(it.delete(key)))

  for fut in pending:
    if fut.read().isErr: return fut.read()

  return success()

method delete*(
  self: TieredDatastore,
  keys: seq[Key]): Future[?!void] {.async, locks: "unknown".} =

  for key in keys:
    let
      pending = await allFinished(self.stores.mapIt(it.delete(key)))

    for fut in pending:
      if fut.read().isErr: return fut.read()

  return success()

method get*(
  self: TieredDatastore,
  key: Key): Future[?!seq[byte]] {.async, locks: "unknown".} =

  var
    bytes: seq[byte]

  for store in self.stores:
    without bytes =? (await store.get(key)):
      continue

    if bytes.len <= 0:
      continue

    # put found data into stores logically in front of the current store
    for s in self.stores:
      if s == store: break
      if(
        let res = (await s.put(key, bytes));
        res.isErr):
        return failure res.error

    return success bytes

method put*(
  self: TieredDatastore,
  key: Key,
  data: seq[byte]): Future[?!void] {.async, locks: "unknown".} =

  let
    pending = await allFinished(self.stores.mapIt(it.put(key, data)))

  for fut in pending:
    if fut.read().isErr: return fut.read()

  return success()

method put*(
  self: TieredDatastore,
  batch: seq[BatchEntry]): Future[?!void] {.async, locks: "unknown".} =

  for entry in batch:
    let
      pending = await allFinished(self.stores.mapIt(it.put(entry.key, entry.data)))

    for fut in pending:
      if fut.read().isErr: return fut.read()

  return success()

# method query*(
#   self: TieredDatastore,
#   query: ...): Future[?!(?...)] {.async, locks: "unknown".} =
#
#   return success ....some
