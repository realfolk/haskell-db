{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs             #-}

module Database.Store.Persistent.Trie.LMDB.Internal
    ( Connection (..)
    , Data (..)
    , ID (..)
    , ManageIDManagerInTxState (..)
    , Node (..)
    , Tx
    , TxState (..)
    , dataIDManagerID
    , defaultDataIDStart
    , defaultNodeIDStart
    , fromBase
    , getChildNodeID
    , nodeIDManagerID
    , put
    , rootNodeID
    ) where

import qualified Control.Monad.State                 as State
import           Data.Binary                         (Binary)
import qualified Data.Binary                         as Binary
import           Data.ByteString                     (ByteString)
import           Data.Hashable                       (Hashable)
import qualified Data.Hashable                       as Hashable
import           Data.Map.Strict                     (Map)
import qualified Data.Map.Strict                     as Map
import           Data.Word                           (Word8)
import           Database.Lib.IDManager              (IDManager)
import qualified Database.Lib.IDManager              as IDManager
import           Database.Lib.Tx                     (ReadWrite)
import qualified Database.Lib.Tx                     as Tx
import qualified Database.Store.Persistent.LMDB.Base as LMDB
import qualified Lib.Binary                          as Binary
import           Lib.Ref                             (Ref)

-- * Constants

nodeIDManagerID = IDManagerID 0 :: ID (IDManager (ID Node))
dataIDManagerID = IDManagerID 1 :: ID (IDManager (ID Data))
rootNodeID = NodeID 0
defaultNodeIDStart = NodeID 1
defaultDataIDStart = DataID 0

-- * Connections

data Connection
  = Connection
      { _connLMDBConnection   :: !LMDB.Connection
      , _connNodeIDManagerRef :: !(Ref (IDManager (ID Node)))
      , _connDataIDManagerRef :: !(Ref (IDManager (ID Data)))
      }

-- * Transactions

type Tx mode o = Tx.Tx mode TxState Connection o

-- ** TxState

data TxState
  = TxState
      { _txConnection    :: !Connection
      , _txLMDBState     :: !LMDB.TxState
      , _txNodeIDManager :: !(IDManager (ID Node))
      , _txDataIDManager :: !(IDManager (ID Data))
      }

class ManageIDManagerInTxState value where
  getIDManager :: Tx mode (IDManager (ID value))
  setIDManager :: IDManager (ID value) -> Tx mode ()

instance ManageIDManagerInTxState Node where
  getIDManager = State.gets _txNodeIDManager
  setIDManager newIDManager = State.modify (\txState -> txState { _txNodeIDManager = newIDManager })

instance ManageIDManagerInTxState Data where
  getIDManager = State.gets _txDataIDManager
  setIDManager newIDManager = State.modify (\txState -> txState { _txDataIDManager = newIDManager })

-- ** Keyspace

data ID value where
  IDManagerID :: Word -> ID (IDManager value)
  NodeID :: Word -> ID Node
  DataID :: Word -> ID Data

instance Eq (ID value) where
  (==) (NodeID id1) (NodeID id2)           = id1 == id2
  (==) (DataID id1) (DataID id2)           = id1 == id2
  (==) (IDManagerID id1) (IDManagerID id2) = id1 == id2

instance Ord (ID value) where
  compare (NodeID id1) (NodeID id2)           = compare id1 id2
  compare (DataID id1) (DataID id2)           = compare id1 id2
  compare (IDManagerID id1) (IDManagerID id2) = compare id1 id2

instance Show (ID value) where
  show (NodeID id)      = "NodeID " <> show id
  show (DataID id)      = "DataID " <> show id
  show (IDManagerID id) = "IDManagerID " <> show id

instance Binary (ID (IDManager (ID value))) where
  get = do
    tag <- Binary.getWord8
    case tag of
      0 -> IDManagerID <$> Binary.get
      _ -> fail "Unable to decode IDManagerID"
  put (IDManagerID n) =
    Binary.putWord8 0 >> Binary.put n

instance Binary (ID Node) where
  get = do
    tag <- Binary.getWord8
    case tag of
      1 -> NodeID <$> Binary.get
      _ -> fail "Unable to decode NodeID"
  put (NodeID n) =
    Binary.putWord8 1 >> Binary.put n

instance Binary (ID Data) where
  get = do
    tag <- Binary.getWord8
    case tag of
      2 -> DataID <$> Binary.get
      _ -> fail "Unable to decode DataID"
  put (DataID n) =
    Binary.putWord8 2 >> Binary.put n

instance Enum (ID (IDManager (ID value))) where
  toEnum n = IDManagerID $ toEnum n
  fromEnum (IDManagerID w) = fromEnum w

instance Enum (ID Node) where
  toEnum n = NodeID $ toEnum n
  fromEnum (NodeID w) = fromEnum w

instance Enum (ID Data) where
  toEnum n = DataID $ toEnum n
  fromEnum (DataID w) = fromEnum w

instance Hashable (ID (IDManager (ID value))) where
  hashWithSalt salt (IDManagerID w) =
    Hashable.hashWithSalt (Hashable.hashWithSalt salt (0 :: Word8)) w

instance Hashable (ID Node) where
  hashWithSalt salt (NodeID w) =
    Hashable.hashWithSalt (Hashable.hashWithSalt salt (1 :: Word8)) w

instance Hashable (ID Data) where
  hashWithSalt salt (DataID w) =
    Hashable.hashWithSalt (Hashable.hashWithSalt salt (2 :: Word8)) w

-- * Values

-- ** Nodes

data Node
  = Node
      { _nodeChildren :: !(Map Word8 (ID Node))
      , _nodeData     :: !(Maybe (ID Data))
      }
  deriving (Eq, Show)

instance Binary Node where
  get = do
    tag <- Binary.getWord8
    case tag of
      0 -> Node <$> Binary.get <*> Binary.get
      _ -> fail "Unable to decode Node"
  put node = do
    Binary.putWord8 0
    Binary.put $ _nodeChildren node
    Binary.put $ _nodeData node

getChildNodeID :: Word8 -> Node -> Maybe (ID Node)
getChildNodeID w node = Map.lookup w $ _nodeChildren node

-- ** Data

newtype Data
  = Data { _dataValue :: ByteString }
  deriving (Eq, Show)

instance Binary Data where
  get = do
    tag <- Binary.getWord8
    case tag of
      0 -> Data <$> Binary.get
      _ -> fail "Unable to decode Data"
  put data' = do
    Binary.putWord8 0
    Binary.put $ _dataValue data'

-- * Actions

-- | Do not allow external puts into the database to avoid data corruption
-- in the future due to conflicts with the 'IDManager'. This can happen
-- if a user puts a value into the database at an @'ID' value@ greater
-- than or equal to the corresponding 'IDManager's @_nextID@.
put
  :: (Binary value, Binary (ID value), ManageIDManagerInTxState value)
  => ID value
  -> value
  -> Tx ReadWrite ()
put id' value = do
  fromBase $ LMDB.put (Binary.encodeStrict id') $ Binary.encodeStrict value
  -- Unrecycle the provided ID from the IDManager in case it
  -- was previously recycled.
  idManager <- getIDManager
  setIDManager $ IDManager.unrecycle id' idManager

-- * Helpers

fromBase :: LMDB.Tx mode o -> Tx mode o
fromBase = Tx.cast _txLMDBState setConnectionState
  where
    setConnectionState txState newLMDBState = txState { _txLMDBState = newLMDBState }
