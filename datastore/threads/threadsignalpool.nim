import pkg/chronos/threadsync
import pkg/threading/smartptrs
import pkg/chronos
import std/locks
import std/sets

import ./databuffer

export databuffer
export smartptrs
export threadsync

const
  SignalPoolSize {.intdefine.} = 1024
  SignalPoolRetries {.intdefine.} = 100

var
  signalPoolLock: Lock
  signalPoolFree: HashSet[ThreadSignalPtr]
  signalPoolUsed: HashSet[ThreadSignalPtr]

proc initSignalPool() =
  signalPoolLock.initLock()
  for i in 1..SignalPoolSize:
    let signal = ThreadSignalPtr.new().get()
    signalPoolFree.incl(signal)

initSignalPool()

proc getThreadSignal*(): Future[ThreadSignalPtr] {.async, raises: [].} =
  ## Get's a ThreadSignalPtr from the pool in a thread-safe way.
  ## 
  ## This provides a simple backpressue mechanism for the
  ## number of requests in flight (not for the file operations themselves).
  ## 
  ## This setup provides two benefits:
  ##  - backpressure on the number of disk IO requests
  ##  - prevents leaks in ThreadSignalPtr's from exhausting the 
  ##      processes IO descriptor limit, which results in bad
  ##      and unpredictable failure modes.
  ## 
  ## This could be put onto its own thread and use it's own set ThreadSignalPtr, 
  ## but the sleepAsync should prove if this is useful for not.
  ## 
  {.cast(gcsafe).}:
    var cnt = SignalPoolRetries
    while cnt > 0:
      cnt.dec()
      signalPoolLock.acquire()
      try:
        if signalPoolFree.len() > 0:
          let res = signalPoolFree.pop()
          signalPoolUsed.incl(res)
          # echo "get:signalPoolUsed:size: ", signalPoolUsed.len()
          return res
      except KeyError:
        discard
      finally:
        signalPoolLock.release()
      # echo "wait:signalPoolUsed: "
      await sleepAsync(10.milliseconds)
    raise newException(DeadThreadDefect, "reached limit trying to acquire a ThreadSignalPtr")

proc release*(sig: ThreadSignalPtr) {.raises: [].} =
  ## Release ThreadSignalPtr back to the pool in a thread-safe way.
  {.cast(gcsafe).}:
    withLock(signalPoolLock):
      signalPoolUsed.excl(sig)
      signalPoolFree.incl(sig)
      # echo "free:signalPoolUsed:size: ", signalPoolUsed.len()
