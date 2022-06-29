{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RecordWildCards  #-}

module Database.Store.Persistent.Trie.LMDB
    ( Connection
    , Data (..)
    , ID (..)
    , LMDB.Error (..)
    , ManageIDManagerInTxState
    , Node (..)
    , ReadOnly
    , ReadWrite
    , Tx
    , TxState
    , connect
    , create
    , delete
    , disconnect
    , flush
    , get
    , getChildNodeID
    , readOnly
    , readOnlyInterface
    , readWrite
    , readWriteInterface
    , replace
    , rootNodeID
    ) where

import           Control.Applicative                          ((<|>))
import           Control.Concurrent                           (getNumCapabilities)
import           Control.Monad                                (unless)
import           Control.Monad.IO.Class                       (liftIO)
import qualified Control.Monad.State                          as State
import           Data.Binary                                  (Binary)
import qualified Data.Binary                                  as Binary
import qualified Data.Map.Strict                              as Map
import           Data.Maybe                                   (fromMaybe)
import           Database.Lib.IDManager                       (IDManager)
import qualified Database.Lib.IDManager                       as IDManager
import qualified Database.Lib.SmartValue                      as SmartValue
import           Database.Lib.Tx                              (ReadOnly,
                                                               ReadWrite,
                                                               RunTransaction,
                                                               TransactionI (TransactionI))
import qualified Database.Lib.Tx                              as Tx
import qualified Database.Store.Persistent.LMDB.Base          as LMDB
import           Database.Store.Persistent.Trie.LMDB.Internal
import qualified Lib.Binary                                   as Binary
import qualified Lib.Ref                                      as Ref

-- * Connections

connect :: FilePath -> IO Connection
connect location = do
  maxReaders <- getNumCapabilities
  _connLMDBConnection <- LMDB.connect location maxReaders
  outcome <- LMDB.readWrite _connLMDBConnection $ do
    createRootNode
    nodeIDManager <- getSmartValue nodeIDManagerID $ IDManager.empty defaultNodeIDStart
    _connNodeIDManagerRef <- liftIO $ Ref.new nodeIDManager
    dataIDManager <- getSmartValue dataIDManagerID $ IDManager.empty defaultDataIDStart
    _connDataIDManagerRef <- liftIO $ Ref.new dataIDManager
    return $ Connection {..}
  either (const $ error "Unable to initialize database.") return outcome
    where
      getSmartValue :: (Binary value, Binary (ID value)) => ID value -> value -> LMDB.Tx mode value
      getSmartValue id' fallback =
        fmap SmartValue.value (SmartValue.get LMDB.smartValueInterface (Binary.encodeStrict id'))
          <|> return fallback
      -- Create the trie root node only if it doesn't already exist in the database.
      createRootNode :: LMDB.Tx ReadWrite ()
      createRootNode = do
        exists <- LMDB.exists encodedRootNodeID
        unless exists $ LMDB.put encodedRootNodeID $ Binary.encodeStrict emptyRootNode
          where
            encodedRootNodeID = Binary.encodeStrict rootNodeID
            emptyRootNode = Node Map.empty Nothing

disconnect :: Connection -> IO ()
disconnect Connection {..} = LMDB.disconnect _connLMDBConnection

-- * Transactions

-- ** Read-Only

readOnly :: RunTransaction ReadOnly TxState Connection o
readOnly = Tx.transact readOnlyInterface

readOnlyInterface :: TransactionI ReadOnly TxState Connection
readOnlyInterface = TransactionI {..}
  where
    _beginTx _txConnection@Connection {..} = do
      _txLMDBState <- Tx._beginTx LMDB.readOnlyInterface _connLMDBConnection
      _txNodeIDManager <- Ref.get _connNodeIDManagerRef
      _txDataIDManager <- Ref.get _connDataIDManagerRef
      return $ TxState {..}
    _precommitTx = fromBase $ Tx._precommitTx LMDB.readOnlyInterface
    _commitTx = Tx._commitTx LMDB.readOnlyInterface . _txLMDBState
    _abortTx = Tx._abortTx LMDB.readOnlyInterface . _txLMDBState

-- ** Read/Write

readWrite :: RunTransaction ReadWrite TxState Connection o
readWrite = Tx.transact readWriteInterface

readWriteInterface :: TransactionI ReadWrite TxState Connection
readWriteInterface = TransactionI {..}
  where
    _beginTx _txConnection@Connection {..} = do
      _txLMDBState <- Tx._beginTx LMDB.readWriteInterface _connLMDBConnection
      _txNodeIDManager <- Ref.get _connNodeIDManagerRef
      _txDataIDManager <- Ref.get _connDataIDManagerRef
      return $ TxState {..}
    _precommitTx = do
      TxState {..} <- State.get
      let Connection {..} = _txConnection
      fromBase $ do
        SmartValue.update LMDB.smartValueInterface (Binary.encodeStrict nodeIDManagerID) _txNodeIDManager
        SmartValue.update LMDB.smartValueInterface (Binary.encodeStrict dataIDManagerID) _txDataIDManager
        Tx._precommitTx LMDB.readWriteInterface
    _commitTx TxState {..} = do
      let Connection {..} = _txConnection
      Tx._commitTx LMDB.readWriteInterface _txLMDBState
      Ref.set _connNodeIDManagerRef _txNodeIDManager
      Ref.set _connDataIDManagerRef _txDataIDManager
    _abortTx = Tx._abortTx LMDB.readWriteInterface . _txLMDBState

-- * Actions

create
  :: (Binary value, Binary (ID value), Enum (ID value), ManageIDManagerInTxState value)
  => value
  -> Tx ReadWrite (ID value)
create value = do
  idManager <- getIDManager
  let (nextID, newIDManager) = IDManager.next idManager
  setIDManager newIDManager
  put nextID value
  return nextID

get :: (Binary value, Binary (ID value)) => ID value -> Tx mode value
get id' =
  fromBase (LMDB.get (Binary.encodeStrict id')) >>= Tx.decodeStrictByteStringOrThrow LMDB.InvalidValue

delete
  :: (Binary value, Binary (ID value), ManageIDManagerInTxState value)
  => ID value
  -> Tx ReadWrite ()
delete id' = do
  fromBase $ LMDB.delete $ Binary.encodeStrict id'
  idManager <- getIDManager
  setIDManager $ fromMaybe idManager $ IDManager.recycle id' idManager

-- | Replace a @value@ in the database without recycling its @'ID' value@ in the 'IDManager'.
replace
  :: (Binary value, Binary (ID value), ManageIDManagerInTxState value)
  => ID value
  -> value
  -> Tx ReadWrite ()
replace id' value = fromBase $ do
  LMDB.delete encodedID
  LMDB.put encodedID $ Binary.encodeStrict value
    where
      encodedID = Binary.encodeStrict id'

-- * Helpers

-- | A simple wrapper around 'LMDB.flush'.
flush :: Connection -> IO ()
flush = LMDB.flush . _connLMDBConnection
