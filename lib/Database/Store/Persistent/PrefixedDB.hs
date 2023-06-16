{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TupleSections     #-}

module Database.Store.Persistent.PrefixedDB
    ( Connection
    , Key
    , MakeIndex
    , Name
    , Prefix
    , PrefixedDB
    , Tx
    , Tx.Error (..)
    , Tx.ReadOnly
    , Tx.ReadWrite
    , TxState
    , connect
    , createValue
    , deleteLookupByName
    , deleteName
    , deleteValue
    , disconnect
    , existsKey
    , existsValue
    , getKey
    , getKeysStartWith
    , getValue
    , getValueWithKey
    , getValuesStartWith
    , makeIndex
    , makeIndexWithPrefix
    , putNameForKey
    , putPrefixForKey
    , readOnly
    , readOnlyTxi
    , readWrite
    , readWriteTxi
    , updateValue
    ) where

import           Control.Applicative                 ((<|>))
import           Control.Concurrent                  (getNumCapabilities)
import           Control.Monad                       (unless, void, when)
import qualified Control.Monad.Except                as E
import qualified Control.Monad.State                 as S
import           Data.Binary                         (Binary)
import qualified Data.Binary                         as Binary
import           Data.ByteString                     (ByteString)
import qualified Data.ByteString.Lazy                as LBS
import           Data.Hashable                       (Hashable, hash)
import           Data.Maybe                          (fromMaybe)
import qualified Data.Set                            as Set
import qualified Data.Text                           as T
import           Data.Word                           (Word64)
import qualified Database.Lib.Data.Dict              as Dict
import qualified Database.Lib.Data.Trie.Text         as Trie
import           Database.Lib.IDManager              (IDManager)
import qualified Database.Lib.IDManager              as IDManager
import qualified Database.Lib.SmartValue             as SmartValue
import           Database.Lib.Sync                   (Sync)
import qualified Database.Lib.Sync                   as Sync
import           Database.Lib.Tx                     (Error, ReadOnly,
                                                      ReadWrite, RunTransaction,
                                                      TransactionI (..))
import qualified Database.Lib.Tx                     as Tx
import qualified Database.Store.Persistent.LMDB.Base as LMDB
import qualified Pouch.Binary                          as Binary
import qualified Pouch.Either                          as Either
import           Pouch.Ref                             (Ref)
import qualified Pouch.Ref                             as Ref
import           MDRN.Data                           (Data)
import qualified MDRN.Data                           as Data
import qualified MDRN.Data.Decode                    as Decode
import           MDRN.Data.Encode                    (ToData (..))

type Connection = PrefixedDB

data PrefixedDB
  = PrefixedDB
      { _pdbLMDBConnection :: !LMDB.Connection
      , _pdbPrefixIndexRef :: !(Ref PrefixIndex)
      , _pdbNameIndexRef   :: !(Ref NameIndex)
      , _pdbIDManagerRef   :: !(Ref (IDManager Key))
      , _pdbSync           :: !Sync
      }

type PrefixIndex = Trie.Trie Key

type NameIndex = Dict.Dict Name Key

type Name = T.Text

type Key = Word64

type Prefix = T.Text

data TxState
  = TxState
      { _txConnection  :: !PrefixedDB
      , _txLMDBTxState :: !LMDB.TxState
      , _txPrefixIndex :: !PrefixIndex
      , _txNameIndex   :: !NameIndex
      , _txIDManager   :: !(IDManager Key)
      }

type Tx mode a = Tx.Tx mode TxState PrefixedDB a

-- Reserved keys in the backing 'LMDB' store. Used internally by 'PrefixedDB'.
lmdbKeyIDManager     = 0   :: Key -- Key used to retrieve and persist the ID manager.
lmdbKeyPrefixIndex   = 1   :: Key -- Key used to retrieve and persist the prefix index.
lmdbKeyNameIndex     = 2   :: Key -- Key used to retrieve and persist the name index.
defaultKeyStartIndex = 256 :: Key -- Default starting key index for backing 'LMDB' store. Keys 0 through 255 are reserved for internal use by 'PrefixedDB'.

connect :: FilePath -> IO Connection
connect location = do
  maxReaders <- getNumCapabilities
  _pdbLMDBConnection <- LMDB.connect location maxReaders
  _pdbSync <- Sync.start syncInterval (LMDB.flush _pdbLMDBConnection)
  outcome <- Tx.transact LMDB.readOnlyInterface _pdbLMDBConnection $ do
    -- Retrieve the necessary indexes and ID manager.
    prefixIndex <- getSmartValue lmdbKeyPrefixIndex Trie.empty
    _pdbPrefixIndexRef <- S.liftIO $ Ref.new prefixIndex
    nameIndex <- getSmartValue lmdbKeyNameIndex Dict.empty
    _pdbNameIndexRef <- S.liftIO $ Ref.new nameIndex
    idManager <- getSmartValue lmdbKeyIDManager (IDManager.empty defaultKeyStartIndex)
    _pdbIDManagerRef <- S.liftIO $ Ref.new idManager
    -- Return the PrefixedDB
    return $ PrefixedDB {..}
  either (const $ error "Unable to initialize database index.") return outcome
    where
      getSmartValue :: Binary a => Key -> a -> LMDB.Tx ReadOnly a
      getSmartValue key fallback =
        fmap SmartValue.value (SmartValue.get LMDB.smartValueInterface (Binary.encodeStrict key))
          <|> return fallback

syncInterval :: Int
syncInterval = 20000 -- 20ms

disconnect :: Connection -> IO ()
disconnect PrefixedDB {..} = do
  Sync.stop _pdbSync
  LMDB.disconnect _pdbLMDBConnection

readOnly :: RunTransaction ReadOnly TxState PrefixedDB a
readOnly = Tx.transact readOnlyTxi

readOnlyTxi :: TransactionI ReadOnly TxState PrefixedDB
readOnlyTxi =
  TransactionI
    { _beginTx = beginReadOnlyTx
    , _commitTx = commitReadOnlyTx
    , _abortTx = abortReadOnlyTx
    , _precommitTx = precommitReadOnlyTx
    }

readWrite :: RunTransaction ReadWrite TxState PrefixedDB a
readWrite = Tx.transact readWriteTxi

readWriteTxi :: TransactionI ReadWrite TxState PrefixedDB
readWriteTxi =
  TransactionI
    { _beginTx = beginReadWriteTx
    , _commitTx = commitReadWriteTx
    , _abortTx = abortReadWriteTx
    , _precommitTx = precommitReadWriteTx
    }

beginReadOnlyTx :: PrefixedDB -> IO TxState
beginReadOnlyTx _txConnection@PrefixedDB {..} = do
  _txLMDBTxState <- _beginTx LMDB.readOnlyInterface _pdbLMDBConnection
  _txPrefixIndex <- Ref.get _pdbPrefixIndexRef
  _txNameIndex <- Ref.get _pdbNameIndexRef
  _txIDManager <- Ref.get _pdbIDManagerRef
  return $ TxState {..}

commitReadOnlyTx :: TxState -> IO ()
commitReadOnlyTx TxState {..}  =
  _commitTx LMDB.readOnlyInterface _txLMDBTxState

abortReadOnlyTx :: TxState -> IO ()
abortReadOnlyTx TxState {..} =
  _abortTx LMDB.readOnlyInterface _txLMDBTxState

precommitReadOnlyTx :: Tx ReadOnly ()
precommitReadOnlyTx =
  fromBase $ _precommitTx LMDB.readOnlyInterface

beginReadWriteTx :: PrefixedDB -> IO TxState
beginReadWriteTx _txConnection@PrefixedDB {..} = do
  Sync.acquireWriteLock _pdbSync
  _txLMDBTxState <- _beginTx LMDB.readWriteInterface _pdbLMDBConnection
  _txPrefixIndex <- Ref.get _pdbPrefixIndexRef
  _txNameIndex <- Ref.get _pdbNameIndexRef
  _txIDManager <- Ref.get _pdbIDManagerRef
  return $ TxState {..}

precommitReadWriteTx :: Tx ReadWrite ()
precommitReadWriteTx = do
  TxState {..} <- S.get
  let PrefixedDB {..} = _txConnection
  fromBase $ do
    SmartValue.update LMDB.smartValueInterface (Binary.encodeStrict lmdbKeyPrefixIndex) _txPrefixIndex
    SmartValue.update LMDB.smartValueInterface (Binary.encodeStrict lmdbKeyNameIndex) _txNameIndex
    SmartValue.update LMDB.smartValueInterface (Binary.encodeStrict lmdbKeyIDManager) _txIDManager
    _precommitTx LMDB.readWriteInterface

commitReadWriteTx :: TxState -> IO ()
commitReadWriteTx state@TxState {..} = do
  let PrefixedDB {..} = _txConnection
  _commitTx LMDB.readWriteInterface _txLMDBTxState
  Ref.set _pdbPrefixIndexRef _txPrefixIndex
  Ref.set _pdbNameIndexRef _txNameIndex
  Ref.set _pdbIDManagerRef _txIDManager
  Sync.releaseWriteLockAndWait _pdbSync

abortReadWriteTx :: TxState -> IO ()
abortReadWriteTx TxState {..} = do
  let PrefixedDB {..} = _txConnection
  _abortTx LMDB.readWriteInterface _txLMDBTxState
  Sync.releaseWriteLock _pdbSync

-- | Retrieve the value corresponding to the provided name.
getValue :: Decode.FromData a => Name -> Tx mode a
getValue name = do
  key <- nameIndexGet name
  raw <- fromBase $ lmdbGet key
  either (const $ E.throwError Tx.KeyDoesNotExist) return $ Decode.fromData raw

-- | Retrieve all values that begin with the given prefix.
getValuesStartWith :: Decode.FromData a => Prefix -> Tx mode [a]
getValuesStartWith prefix = do
  keys <- prefixIndexStartsWith prefix
  raw <- fromBase $ mapM lmdbGet keys
  either (const $ E.throwError Tx.KeyDoesNotExist) return $ mapM Decode.fromData raw

-- | Retrieve the key corresponding to the given name.
getKey :: Name -> Tx mode Key
getKey = nameIndexGet

-- | Retrieve all keys that begin with the given prefix.
getKeysStartWith :: Prefix -> Tx mode [Key]
getKeysStartWith = prefixIndexStartsWith

-- | Retrieve the value with the given key.
getValueWithKey :: Decode.FromData a => Key -> Tx mode a
getValueWithKey key = do
  raw <- fromBase $ lmdbGet key
  either (const $ E.throwError Tx.KeyDoesNotExist) return $ Decode.fromData raw

-- | Check for existence of a value with the given name.
existsValue :: Name -> Tx mode Bool
existsValue name = do
  keyExists <- nameIndexExists name
  if keyExists
     then do
       key <- nameIndexGet name
       fromBase $ lmdbExists key
     else return False

-- | Check for existence of a key with the given name.
existsKey :: Name -> Tx mode Bool
existsKey = nameIndexExists

-- | Puts a value in the store and assigns the given name. Generates a unique key.
createValue :: ToData a => Name -> a -> Tx ReadWrite Key
createValue name value = do
  key <- getNextKey
  fromBase $! lmdbPut key $! toData value
  nameIndexPut name key
  return key

-- | Sets a prefix for the given key. Used for more fine grained control over how prefixes are set.
putPrefixForKey :: Prefix -> Key -> Tx ReadWrite ()
putPrefixForKey = prefixIndexInsert

-- | Set a name index for the given key.
putNameForKey :: Name -> Key -> Tx ReadWrite ()
putNameForKey = nameIndexPut

updateValue :: ToData a => Name -> a -> Tx ReadWrite ()
updateValue name value = do
  deleteValue name
  void $ createValue name value

-- DELETE ACTIONS

-- | Deletes a value from the store with the given name.
deleteValue :: Name -> Tx ReadWrite ()
deleteValue name = do
  key <- nameIndexGet name
  fromBase $ lmdbDelete key
  nameIndexDelete name
  -- FIXME: Don't we also have to delete all the other names that map to the key?
  when (key >= defaultKeyStartIndex) (recycleKey key)

-- | Deletes a name lookup only without attempting to delete the mapped value from the store.
deleteName :: Name -> Tx ReadWrite ()
deleteName = nameIndexDelete

-- | Deletes a lookup prefix for the given name.
deleteLookupByName :: Name -> Prefix -> Tx ReadWrite ()
deleteLookupByName name prefix = do
  key <- getKey name
  deleteLookupByKey key prefix

-- | Deletes a lookup prefix for the given key value.
deleteLookupByKey :: Key -> Prefix -> Tx ReadWrite ()
deleteLookupByKey = flip prefixIndexDeleteOne

-- MAKE INDEX HELPERS

type MakeIndex = [T.Text] -> Name

-- | Given a list of 'Text', constructs an 'Text' index from the 'Text' parts and the prefix separator.
makeIndex :: MakeIndex
makeIndex = T.intercalate prefixSeparator

-- | Performs the same function as 'makeIndex', but also includes an additional 'Prefix'.
makeIndexWithPrefix :: Prefix -> MakeIndex
makeIndexWithPrefix prefix parts = makeIndex (prefix : parts)

prefixSeparator :: T.Text
prefixSeparator = ":"

-- PREFIX INDEX HELPERS

prefixIndexStartsWith :: Prefix -> Tx mode [Key]
prefixIndexStartsWith prefix = do
  TxState {..} <- S.get
  return $ Set.toList $ Trie.startsWith prefix _txPrefixIndex

prefixIndexInsert :: Prefix -> Key -> Tx ReadWrite ()
prefixIndexInsert prefix key = do
  txState <- S.get
  let newPrefixIndex = Trie.insert prefix key $ _txPrefixIndex txState
  S.put $ txState { _txPrefixIndex = newPrefixIndex }

prefixIndexDeleteOne :: Prefix -> Key -> Tx ReadWrite ()
prefixIndexDeleteOne prefix key = do
  txState <- S.get
  maybe (E.throwError Tx.KeyDoesNotExist) S.put $ do
    newPrefixIndex <- Trie.deleteOne prefix key $ _txPrefixIndex txState
    return $ txState { _txPrefixIndex = newPrefixIndex }

-- NAME INDEX HELPERS

nameIndexGet :: Name -> Tx mode Key
nameIndexGet name = do
  TxState {..} <- S.get
  maybe (E.throwError Tx.KeyDoesNotExist) return $ Dict.get name _txNameIndex

nameIndexExists :: Name -> Tx mode Bool
nameIndexExists name = do
  TxState{..} <- S.get
  return $ Dict.exists name _txNameIndex

nameIndexPut :: Name -> Key -> Tx ReadWrite ()
nameIndexPut name key = do
  txState <- S.get
  maybe (E.throwError Tx.KeyAlreadyExists) S.put $ do
    newNameIndex <- Dict.put name key $ _txNameIndex txState
    return $ txState { _txNameIndex = newNameIndex }

nameIndexDelete :: Name -> Tx ReadWrite ()
nameIndexDelete name = do
  txState <- S.get
  maybe (E.throwError Tx.KeyDoesNotExist) S.put $ do
    newNameIndex <- Dict.delete name $ _txNameIndex txState
    return $ txState { _txNameIndex = newNameIndex }

-- ID MANAGEMENT HELPERS

-- | Recycles a recently-freed key to the database's 'IDManager'.
recycleKey :: Key -> Tx ReadWrite ()
recycleKey key =
  mapTxIDManager (\idm -> ((), fromMaybe idm (IDManager.recycle key idm)))

-- | Gets the next available 'Key' from the database's 'IDManager'.
getNextKey :: Tx ReadWrite Key
getNextKey = mapTxIDManager IDManager.next

-- | Map over the database's 'IDManager' inside a 'Tx'.
mapTxIDManager :: (IDManager Key -> (a, IDManager Key)) -> Tx ReadWrite a
mapTxIDManager f = do
  txState <- S.get
  let (result, newIDManager) = f $ _txIDManager txState
  S.put $ txState { _txIDManager = newIDManager }
  return result

-- LMDB BASE HELPERS

fromBase :: LMDB.Tx mode a -> Tx mode a
fromBase = Tx.cast getLMDBTxState setLMDBTxState
  where
    getLMDBTxState = _txLMDBTxState
    setLMDBTxState txState newLMDBTxState = txState { _txLMDBTxState = newLMDBTxState }

lmdbPut :: ToData a => Key -> a -> LMDB.Tx ReadWrite ()
lmdbPut k v =
  LMDB.put (toLMDBKey k)
    $ LBS.toStrict
    $ Data.toByteString
    $ toData v

lmdbGet :: Key -> LMDB.Tx mode Data
lmdbGet k = do
  raw <- LMDB.get $ toLMDBKey k
  either (const $ E.throwError Tx.KeyDoesNotExist) return $ Decode.decodeByteString $ LBS.fromStrict raw

lmdbExists :: Key -> LMDB.Tx mode Bool
lmdbExists = LMDB.exists . toLMDBKey

lmdbDelete :: Key -> LMDB.Tx ReadWrite ()
lmdbDelete = LMDB.delete . toLMDBKey

toLMDBKey :: Key -> LMDB.Key
toLMDBKey = Binary.encodeStrict
