{-|
Module: Database.Lib.Sync
Description: Synchronizes an IO action on a timed loop.
Copyright: (c) Real Folk Inc. 2021
Maintainer: admin@realfolk.com
Stability: experimental
Portability: POSIX
-}
module Database.Lib.Sync
    ( Sync
    , acquireWriteLock
    , releaseWriteLock
    , releaseWriteLockAndWait
    , start
    , stop
    ) where

import           Control.Concurrent     (ThreadId, forkIO, killThread,
                                         threadDelay)
import qualified Control.Concurrent.STM as STM
import           Control.Monad          (forever)
import           Pouch.Concurrent.Lock    (Lock)
import qualified Pouch.Concurrent.Lock    as Lock

data Sync
  = Sync !Lock !Queue !ThreadId

type Queue = STM.TQueue Lock

start :: Int -> IO () -> IO Sync
start interval flush = do
  writeLock <- Lock.new
  queue <- STM.newTQueueIO
  threadId <- forkIO $ daemon writeLock interval flush queue
  return $ Sync writeLock queue threadId

stop :: Sync -> IO ()
stop sync@(Sync _ _ threadId) =
  acquireWriteLock sync >> releaseWriteLockAndWait sync >> killThread threadId

acquireWriteLock :: Sync -> IO ()
acquireWriteLock (Sync l _ _) = Lock.acquire l

releaseWriteLock :: Sync -> IO ()
releaseWriteLock (Sync l _ _) = Lock.release l

releaseWriteLockAndWait :: Sync -> IO ()
releaseWriteLockAndWait (Sync l q _) = Lock.release l >> joinQueue q

-- * Internal Helpers

daemon :: Lock -> Int -> IO () -> Queue -> IO ()
daemon writeLock interval action queue =
  forever $ do
    threadDelay interval
    -- Acquire the write lock before running the action
    -- to avoid potential race conditions.
    Lock.acquire writeLock
    action
    allLeaveQueue queue
    Lock.release writeLock

joinQueue :: Queue -> IO ()
joinQueue q = do
  lock <- Lock.newAcquired
  STM.atomically $ STM.writeTQueue q lock
  Lock.acquire lock

allLeaveQueue :: Queue -> IO ()
allLeaveQueue q = do
  locks <- STM.atomically $ STM.flushTQueue q
  mapM_ Lock.release locks
