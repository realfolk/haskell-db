{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Database.Store.Persistent.Trie.Path
    ( Parent (..)
    , Path (..)
    , getNextChildNodeID
    , isComplete
    , isIncomplete
    , resolve
    ) where

import           Data.ByteString                     (ByteString)
import qualified Data.ByteString                     as ByteString
import qualified Data.Maybe                          as Maybe
import           Data.Word                           (Word8)
import           Database.Store.Persistent.Trie.LMDB (Data, ID,
                                                      Node (_nodeData))
import qualified Database.Store.Persistent.Trie.LMDB as LMDB

-- * Paths

data Path
  = Complete (ID Data) Parent
  -- ^ The 'Path' fully terminates at a 'Node' with 'Data'.
  | Incomplete ByteString Parent
  -- ^ The 'Path' either fully terminates at a 'Node' without 'Data', or does not fully terminate.
  deriving (Eq, Show)

-- ** Introspection

isComplete :: Path -> Bool
isComplete path =
  case path of
    Complete _ _ -> True
    _            -> False

isIncomplete :: Path -> Bool
isIncomplete path =
  case path of
    Incomplete _ _ -> True
    _              -> False

-- ** Actions

resolve :: ID Node -> ByteString -> LMDB.Tx mode Path
resolve rootNodeID key = do
  root <- retrieveParent rootNodeID Nothing
  loop root key
  where
    loop :: Parent -> ByteString -> LMDB.Tx mode Path
    loop parent@(Parent _ parentNode _) remainingKey = do
      case (remainingKey, _nodeData parentNode, getNextChildNodeID remainingKey parentNode) of
        ("", Just dataID, _) -> do
          return $ Complete dataID parent
        (_, _, Nothing) -> do
          return $ Incomplete remainingKey parent
        (_, _, Just (childNodeID, _, newRemainingKey)) -> do
          nextParent <- retrieveParent childNodeID (Just parent)
          loop nextParent newRemainingKey

-- * Parents

data Parent
  = Parent (ID Node) Node (Maybe Parent)
  deriving (Eq, Show)

-- ** Internal Actions

retrieveParent :: ID Node -> Maybe Parent -> LMDB.Tx mode Parent
retrieveParent nodeID maybeParent = do
  node <- LMDB.get nodeID
  return $ Parent nodeID node maybeParent

-- * Misc.

getNextChildNodeID :: ByteString -> Node -> Maybe (ID Node, Word8, ByteString)
getNextChildNodeID key node = do
  (index, remainingKey) <- ByteString.uncons key
  childNodeID <- LMDB.getChildNodeID index node
  return (childNodeID, index, remainingKey)
