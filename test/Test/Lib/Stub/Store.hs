{-# LANGUAGE RecordWildCards #-}
module Test.Lib.Stub.Store
    ( Connection
    , Database
    , Key
    , ReadOnly
    , ReadWrite
    , Tx
    , Tx.Error (..)
    , TxState
    , Value
    , connect
    , delete
    , empty
    , get
    , put
    , readOnly
    , readOnlyInterface
    , readWrite
    , readWriteInterface
    , smartValueInterface
    ) where

import           Control.Concurrent.MVar (MVar)
import qualified Control.Concurrent.MVar as MVar
import           Control.Monad           (when)
import qualified Control.Monad.Except    as Except
import qualified Control.Monad.State     as State
import qualified Data.Binary             as Binary
import           Data.ByteString.Lazy    (ByteString)
import qualified Data.ByteString.Lazy    as ByteString.Lazy
import           Data.Map                (Map)
import qualified Data.Map                as Map
import           Data.Maybe              (isJust, isNothing)
import           Database.Lib.SmartValue (StoreInterface (..))
import           Database.Lib.Tx         (ReadOnly, ReadWrite,
                                          TransactionI (..))
import qualified Database.Lib.Tx         as Tx

-- * Core Types

type Key = Word

type Value = ByteString

type Database = Map Key Value

-- ** Connection

type Connection = MVar Database

empty :: IO Connection
empty = MVar.newMVar Map.empty

connect :: Database -> IO Connection
connect = MVar.newMVar

-- ** Transactions

type Tx mode o = Tx.Tx mode TxState Connection o

data TxState
  = TxState
      { _txConnection :: Connection
      , _txDatabase   :: Database
      , _txBackup     :: Database
      }

readOnlyInterface :: TransactionI ReadOnly TxState Connection
readOnlyInterface =
  TransactionI
    { _beginTx = \connection -> do
        db <- MVar.readMVar connection
        return $ TxState connection db db
    , _precommitTx = return ()
    , _commitTx = const $ return ()
    , _abortTx = const $ return ()
    }

readOnly = Tx.transact readOnlyInterface

readWriteInterface :: TransactionI ReadWrite TxState Connection
readWriteInterface =
  TransactionI
    { _beginTx = \connection -> do
      db <- MVar.takeMVar connection
      return $ TxState connection db db
    , _precommitTx = return ()
    , _commitTx = \TxState{..} ->
        MVar.putMVar _txConnection _txDatabase
    , _abortTx = \TxState{..} ->
        MVar.putMVar _txConnection _txBackup
    }

readWrite = Tx.transact readWriteInterface

-- * Actions

get :: Key -> Tx mode Value
get key = do
  TxState {..} <- State.get
  maybe (throwKeyDoesNotExist key) return $ Map.lookup key _txDatabase

put :: Key -> Value -> Tx ReadWrite ()
put key value = do
  TxState {..} <- State.get
  let maybeExisting = Map.lookup key _txDatabase
  when (isJust maybeExisting) $ Except.throwError Tx.KeyAlreadyExists
  State.modify (\txState@TxState {..} -> txState { _txDatabase = Map.insert key value _txDatabase })

delete :: Key -> Tx ReadWrite ()
delete key = do
  TxState {..} <- State.get
  let maybeExisting = Map.lookup key _txDatabase
  when (isNothing maybeExisting) $ throwKeyDoesNotExist key
  State.modify (\txState@TxState {..} -> txState { _txDatabase = Map.delete key _txDatabase })


-- * Smart Value Store Interface

smartValueInterface :: StoreInterface Key mode TxState Connection
smartValueInterface =
  StoreInterface
    { _siGet = get
    , _siDelete = delete
    , _siPut = put
    }

-- * Internal Helpers

throwKeyDoesNotExist :: Key -> Tx mode a
throwKeyDoesNotExist = Except.throwError . Tx.KeyDoesNotExist . ByteString.Lazy.toStrict . Binary.encode
