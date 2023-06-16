{-# LANGUAGE RecordWildCards #-}

{-|
Module: Database.Store.Persistent.LMDB.Synced
Copyright: (c) Real Folk Inc. 2022
Maintainer: admin@realfolk.com
Stability: experimental
Portability: POSIX
-}

module Database.Store.Persistent.LMDB.Synced
    ( Connection
    , Key
    , Tx
    , Tx.Error (..)
    , Tx.ReadOnly
    , Tx.ReadWrite
    , TxState
    , Value
    , connect
    , create
    , delete
    , disconnect
    , exists
    , get
    , put
    , readOnly
    , readOnlyInterface
    , readWrite
    , readWriteInterface
    ) where

import qualified Control.Monad.State                 as State
import qualified Data.ByteString.Lazy                as LazyByteString
import qualified Database.Lib.SmartValue             as SmartValue
import qualified Database.Lib.Sync                   as Sync
import           Database.Lib.Tx                     (RunTransaction,
                                                      TransactionI (TransactionI))
import qualified Database.Lib.Tx                     as Tx
import           Database.Store.Persistent.LMDB.Base (Error (..), ReadOnly,
                                                      ReadWrite)
import qualified Database.Store.Persistent.LMDB.Base as Base

-- * Constants

syncInterval :: Int
syncInterval = 20000 -- 20ms

-- * Core Types

type Key = Base.Key

type Value = Base.Value

-- * Connections

data Connection
  = Connection !Base.Connection !Sync.Sync

connect :: FilePath -> Int -> IO Connection
connect envPath maxReaders = do
  base <- Base.connect envPath maxReaders
  sync <- Sync.start syncInterval (Base.flush base)
  return $ Connection base sync

disconnect :: Connection -> IO ()
disconnect (Connection base sync) = do
  Sync.stop sync
  Base.disconnect base

-- * Transactions

type Tx mode a = Tx.Tx mode TxState Connection a

data TxState
  = TxState !Connection !Base.TxState

-- ** Read-only Transactions

readOnly :: RunTransaction ReadOnly TxState Connection a
readOnly = Tx.transact readOnlyInterface

readOnlyInterface :: TransactionI ReadOnly TxState Connection
readOnlyInterface = TransactionI {..}
  where
    _beginTx lmdb@(Connection base _) = do
      baseState <- Tx._beginTx Base.readOnlyInterface base
      return $ TxState lmdb baseState
    _commitTx (TxState _ baseState) = Tx._commitTx Base.readOnlyInterface baseState
    _abortTx (TxState _ baseState) = Tx._abortTx Base.readOnlyInterface baseState
    _precommitTx = fromBase $ Tx._precommitTx Base.readOnlyInterface

-- ** Read/Write Transactions

readWrite :: RunTransaction ReadWrite TxState Connection a
readWrite = Tx.transact readWriteInterface

readWriteInterface :: TransactionI ReadWrite TxState Connection
readWriteInterface = TransactionI {..}
  where
    _beginTx lmdb@(Connection base sync) = do
      Sync.acquireWriteLock sync
      baseState <- Tx._beginTx Base.readWriteInterface base
      return $ TxState lmdb baseState
    _commitTx (TxState (Connection _ sync) baseState) = do
      Tx._commitTx Base.readWriteInterface baseState
      Sync.releaseWriteLockAndWait sync
    _abortTx (TxState (Connection _ sync) baseState) = do
      Tx._abortTx Base.readWriteInterface baseState
      Sync.releaseWriteLock sync
    _precommitTx = fromBase $ Tx._precommitTx Base.readWriteInterface

-- * Actions

exists :: Key -> Tx mode Bool
exists = fromBase . Base.exists

get :: Key -> Tx mode Base.Value
get = fromBase . Base.get

put :: Key -> Value -> Tx ReadWrite ()
put k v = fromBase $ Base.put k v

delete :: Key -> Tx ReadWrite ()
delete = fromBase . Base.delete

create :: Tx ReadWrite Key -> Value -> Tx ReadWrite Key
create keygen value = do
  (TxState lmdb@(Connection base sync) _) <- State.get
  fromBase $ Base.create (toBase lmdb keygen) value

-- * 'SmartValue' Interface

smartValueInterface :: SmartValue.StoreInterface Key mode TxState Connection
smartValueInterface =
  SmartValue.StoreInterface
    { SmartValue._siGet = fmap LazyByteString.fromStrict . get
    , SmartValue._siDelete = delete
    , SmartValue._siPut = \k -> put k . LazyByteString.toStrict
    }

-- * Internal Helpers

fromBase :: Base.Tx mode a -> Tx mode a
fromBase = Tx.cast getBaseState setBaseState
  where
    getBaseState (TxState _ baseState) = baseState
    setBaseState (TxState lmdb _) baseState = TxState lmdb baseState

toBase :: Connection -> Tx mode a -> Base.Tx mode a
toBase (Connection base sync) childTx =
  Tx.cast (getSyncedState sync) setSyncedState childTx
    where
      getSyncedState sync baseState = TxState (Connection base sync) baseState
      setSyncedState _ (TxState _ baseTxState) = baseTxState
