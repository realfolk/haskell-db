{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Database.Store.Persistent.Trie
    ( AppendFlattened
    , Connection
    , Key
    , LMDB.Error (..)
    , ReadOnly
    , ReadWrite
    , Tx
    , TxState
    , Value
    , connect
    , delete
    , disconnect
    , exists
    , get
    , insert
    , readOnly
    , readOnlyInterface
    , readWrite
    , readWriteInterface
    , search
    ) where

import           Control.Monad                           (when)
import qualified Control.Monad.Except                    as Except
import           Data.ByteString                         (ByteString)
import qualified Data.ByteString                         as ByteString
import           Data.Map.Strict                         (Map)
import qualified Data.Map.Strict                         as Map
import qualified Data.Maybe                              as Maybe
import           Data.Word                               (Word8)
import qualified Database.Lib.Sync                       as Sync
import           Database.Lib.Tx                         (ReadOnly, ReadWrite,
                                                          RunTransaction,
                                                          TransactionI (TransactionI))
import qualified Database.Lib.Tx                         as Tx
import           Database.Store.Persistent.Trie.Internal
import           Database.Store.Persistent.Trie.LMDB     (Data (..), ID (..),
                                                          Node (..))
import qualified Database.Store.Persistent.Trie.LMDB     as LMDB
import qualified Database.Store.Persistent.Trie.Path     as Path
import qualified Pouch.Binary                            as Binary

-- * Core Types

type Key = ByteString

type Value = ByteString

-- * Connections

connect :: FilePath -> Int -> IO Connection
connect location syncInterval = do
  _connLMDBConnection <- LMDB.connect location
  _connSync <- Sync.start syncInterval (LMDB.flush _connLMDBConnection)
  return $ Connection {..}

disconnect :: Connection -> IO ()
disconnect Connection {..} = do
  Sync.stop _connSync
  LMDB.disconnect _connLMDBConnection

-- * Transactions

-- ** Read-only Transactions

readOnly :: RunTransaction ReadOnly TxState Connection o
readOnly = Tx.transact readOnlyInterface

readOnlyInterface :: TransactionI ReadOnly TxState Connection
readOnlyInterface = TransactionI {..}
  where
    _beginTx _txConnection@Connection {..} = do
      _txLMDBState <- Tx._beginTx LMDB.readOnlyInterface _connLMDBConnection
      return $ TxState {..}
    _precommitTx = fromLMDB $ Tx._precommitTx LMDB.readOnlyInterface
    _commitTx = Tx._commitTx LMDB.readOnlyInterface . _txLMDBState
    _abortTx = Tx._abortTx LMDB.readOnlyInterface . _txLMDBState

-- ** Read/Write Transactions

readWrite :: RunTransaction ReadWrite TxState Connection o
readWrite = Tx.transact readWriteInterface

readWriteInterface :: TransactionI ReadWrite TxState Connection
readWriteInterface = TransactionI {..}
  where
    _beginTx _txConnection@Connection {..} = do
      Sync.acquireWriteLock _connSync
      _txLMDBState <- Tx._beginTx LMDB.readWriteInterface _connLMDBConnection
      return $ TxState {..}
    _precommitTx = fromLMDB $ Tx._precommitTx LMDB.readWriteInterface
    _commitTx TxState {..} = do
      Tx._commitTx LMDB.readWriteInterface _txLMDBState
      let Connection {..} = _txConnection
      Sync.releaseWriteLockAndWait _connSync
    _abortTx TxState {..} = do
      Tx._abortTx LMDB.readWriteInterface _txLMDBState
      let Connection {..} = _txConnection
      Sync.releaseWriteLock _connSync

-- * Actions

exists :: Key -> Tx mode Bool
exists key = checkExistence `Except.catchError` onError
  where
    checkExistence = do
      path <- fromLMDB $ Path.resolve LMDB.rootNodeID key
      return $ Path.isComplete path
    onError e =
      case e of
        LMDB.KeyDoesNotExist _ -> return False
        _                      -> Except.throwError e

get :: Key -> Tx mode Value
get key = do
  fromLMDB $ do
    path <- Path.resolve LMDB.rootNodeID key `Except.catchError` handleError
    case path of
      Path.Complete dataID _ -> do
        Data {..} <- LMDB.get dataID
        return _dataValue
      _ -> throwKeyDoesNotExist key
    where
      handleError :: LMDB.Error mode -> LMDB.Tx mode a
      handleError e =
        case e of
          LMDB.KeyDoesNotExist _ -> throwKeyDoesNotExist key
          _                      -> Except.throwError e


-- | A useful helper type for implementing 'search'. It helps avoid unnecessary @O(n)@ operations when running a query.
type AppendFlattened = Map Key Value -> Map Key Value

-- TODO Return an in-memory trie data structure instead of a map?
search :: Key -> Tx mode (Map Key Value)
search key = fromLMDB $ do
  path <- Path.resolve LMDB.rootNodeID key
  makeFlattened <- case path of
    -- Fully matched the key.
    Path.Incomplete "" (Path.Parent _ node _) ->
      flattenNode key node
    -- Fully matched the key.
    Path.Complete _ (Path.Parent _ node _) ->
      flattenNode key node
    -- The key does not exist in the trie.
    Path.Incomplete _ _ ->
      throwKeyDoesNotExist key
  return $ makeFlattened Map.empty
    where
      flattenNode :: Key -> Node -> LMDB.Tx mode AppendFlattened
      flattenNode nodeKey node = do
        appendNodeData <-
          case _nodeData node of
            Nothing     -> return id
            Just dataID -> do
              data' <- LMDB.get dataID
              return $ Map.insert nodeKey $ _dataValue data'
        appendChildren <- Map.foldrWithKey (flattenChild . ByteString.snoc nodeKey) (return id) (_nodeChildren node)
        return (appendChildren . appendNodeData)
      flattenChild :: Key -> ID Node -> LMDB.Tx mode AppendFlattened -> LMDB.Tx mode AppendFlattened
      flattenChild childKey childNodeID accTx = do
        childNode <- LMDB.get childNodeID
        appendChild <- flattenNode childKey childNode
        appendAcc <- accTx
        return (appendAcc . appendChild)

insert :: Key -> Value -> Tx ReadWrite ()
insert key value = fromLMDB $ do
  path <- Path.resolve LMDB.rootNodeID key
  case path of
    -- Fully matched the key and data already exists.
    Path.Complete _ _ ->
      Except.throwError Tx.KeyAlreadyExists
    -- Otherwise...
    Path.Incomplete remainingKey parent ->
      case (ByteString.unsnoc remainingKey, parent) of
        -- Fully matched the key and data already exists.
        -- This shouldn't happen, but it is a possible state.
        (Nothing, Path.Parent _ (Node _ (Just _)) _) ->
          Except.throwError Tx.KeyAlreadyExists
        -- Fully matched the key and data DOES NOT already exist.
        -- Update the existing node with the data.
        (Nothing, Path.Parent nodeID node@(Node _ Nothing) _) -> do
          -- Create the new data value.
          dataID <- LMDB.create $ Data value
          -- Replace the existing node with the data reference.
          LMDB.replace nodeID $ node { _nodeData = Just dataID }
        -- Key not fully matched, must insert missing parent nodes,
        -- and the data itself.
        (Just (remainingKeyInit, lastNodeIndex), Path.Parent deepestExistingNodeID deepestExistingNode _) -> do
          -- Create the new data value.
          dataID <- LMDB.create $ Data value
          -- Create the "last" node that references the new data value.
          lastNodeID <- LMDB.create $ Node Map.empty (Just dataID)
          -- Process key one 'Word8' at a time, right to left, returning the newly created 'Node' and 'Word8'.
          -- This loop will return the "first" node in the path of missing nodes.
          (firstNodeID, firstNodeIndex) <- insertMissingNodes remainingKeyInit lastNodeIndex lastNodeID
          -- Update the deepest existing node to point to the above "first" node in its children.
          let updatedChildren = Map.insert firstNodeIndex firstNodeID $ _nodeChildren deepestExistingNode
          LMDB.replace deepestExistingNodeID $ deepestExistingNode { _nodeChildren = updatedChildren }
  where
    insertMissingNodes :: ByteString -> Word8 -> ID Node -> LMDB.Tx ReadWrite (ID Node, Word8)
    insertMissingNodes remainingKey childNodeIndex childNodeID =
      case ByteString.unsnoc remainingKey of
        -- Exhausted the remainingKey, so terminate the loop.
        Nothing ->
          return (childNodeID, childNodeIndex)
        -- Insert the next node.
        Just (remainingKeyInit, nodeIndex) -> do
          nodeID <- LMDB.create $ Node (Map.singleton childNodeIndex childNodeID) Nothing
          insertMissingNodes remainingKeyInit nodeIndex nodeID

delete :: Key -> Tx ReadWrite ()
delete key = fromLMDB $ do
  path <- Path.resolve LMDB.rootNodeID key
  case path of
    -- Fully matched the key and data DOES NOT already exist.
    -- Therefore, the key doesn't exist, nothing to delete.
    Path.Incomplete "" _ ->
      throwKeyDoesNotExist key
    -- The key doesn't exist at all, nothing to delete.
    Path.Incomplete _ _ ->
      throwKeyDoesNotExist key
    -- Fully matched the key and data exists. Proceed with deletion.
    -- Delete the data and any "empty" parent nodes.
    Path.Complete dataID (Path.Parent parentNodeID parentNode grandparent) -> do
      -- Delete the data.
      LMDB.delete dataID
      -- Update the parent to remove its data reference.
      let updatedParentNode = parentNode { _nodeData = Nothing }
      let updatedParent = Path.Parent parentNodeID updatedParentNode grandparent
      -- Check if the parent can be deleted.
      if canDeleteParent False updatedParent
         -- If the parent can be deleted, then delete it and any "empty" grandparents.
         then LMDB.delete parentNodeID >> deleteGrandparents grandparent
         -- If the parent cannot be deleted, then simply replace it with the
         -- updated parent node without the data reference.
         else LMDB.replace parentNodeID updatedParentNode
  where
    deleteGrandparents :: Maybe Path.Parent -> LMDB.Tx ReadWrite ()
    deleteGrandparents maybeParent =
      case maybeParent of
        Nothing ->
          return ()
        Just parent@(Path.Parent nodeID _ grandparent) ->
          when (canDeleteParent True parent) $ do
            LMDB.delete nodeID
            deleteGrandparents grandparent
    -- A parent or grandparent can only be deleted when (1) its node has
    -- the correct number of children (see comment below), (2) its node doesn't
    -- reference any data, and (3) it is not the root node.
    canDeleteParent :: Bool -> Path.Parent -> Bool
    canDeleteParent isGrandparent parent@(Path.Parent nodeID node _) =
      -- When the provided parent is the direct parent of the data
      -- that is being deleted (isGrandparent=False), then it must
      -- not have any child nodes to allow its deletion. Grandaprents
      -- (isGrandparent=True), however, can have up to a maximum of 1
      -- child (the path connection) to be eligible for deletion.
      let maxChildren = if isGrandparent then 1 else 0
       in numChildren parent <= maxChildren
       && Maybe.isNothing (_nodeData node)
       && nodeID /= LMDB.rootNodeID
    numChildren :: Path.Parent -> Int
    numChildren (Path.Parent _ Node {..} _) =
      Map.size _nodeChildren

-- * Internal Helpers

throwKeyDoesNotExist :: Key -> LMDB.Tx mode a
throwKeyDoesNotExist = Except.throwError . Tx.KeyDoesNotExist
