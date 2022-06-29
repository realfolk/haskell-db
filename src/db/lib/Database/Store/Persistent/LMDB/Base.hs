{-# LANGUAGE RecordWildCards #-}

{-|
Module: Database.Store.Persistent.LMDB.Base
Copyright: (c) Real Folk Inc. 2022
Maintainer: admin@realfolk.com
Stability: experimental
Portability: POSIX
-}

module Database.Store.Persistent.LMDB.Base
    ( Connection
    , Error (..)
    , Key
    , ReadOnly
    , ReadWrite
    , Stats (..)
    , Tx
    , TxState
    , Value
    , connect
    , create
    , delete
    , disconnect
    , exists
    , flush
    , get
    , getEnvironment
    , getStats
    , put
    , readOnly
    , readOnlyInterface
    , readWrite
    , readWriteInterface
    , smartValueInterface
    ) where

import           Control.Monad           (unless)
import qualified Control.Monad.Except    as Except
import           Control.Monad.IO.Class  (liftIO)
import qualified Control.Monad.State     as State
import qualified Data.Binary             as Binary
import           Data.ByteString         (ByteString)
import qualified Data.ByteString         as ByteString
import qualified Data.ByteString.Lazy    as LazyByteString
import qualified Data.ByteString.Unsafe  as UnsafeByteString
import           Data.Functor            (($>))
import           Data.Word               (Word32, Word64)
import           Database.LMDB.Raw
import qualified Database.Lib.SmartValue as SmartValue
import           Database.Lib.Tx         (Error (..), ReadOnly, ReadWrite,
                                          RunTransaction,
                                          TransactionI (TransactionI))
import qualified Database.Lib.Tx         as Tx
import           Foreign.C.Types         (CSize)
import           Foreign.Ptr             (castPtr)
import           GHC.Generics            (Generic)
import           System.Directory        (createDirectory, doesDirectoryExist)

-- * Core Types

type Key = ByteString

type Value = ByteString

-- * Connections

data Connection
  = Connection !MDB_env !MDB_dbi

connect :: FilePath -> Int -> IO Connection
connect envPath maxReaders = do
  (env, pageSize) <- createEnvironment envPath maxReaders
  db <- openDatabase "root" env
  return $ Connection env db

disconnect :: Connection -> IO ()
disconnect (Connection env db) = do
  mdb_dbi_close env db
  mdb_env_close env

-- * Transactions

type Tx mode a = Tx.Tx mode TxState Connection a

data TxState
  = TxState !Connection !MDB_txn

-- ** Read-only Transactions

readOnly :: RunTransaction ReadOnly TxState Connection a
readOnly = Tx.transact readOnlyInterface

readOnlyInterface :: TransactionI ReadOnly TxState Connection
readOnlyInterface = TransactionI {..}
  where
    _beginTx = beginTx True
    _commitTx (TxState _ txn) = mdb_txn_abort txn
    _abortTx (TxState _ txn) = mdb_txn_abort txn
    _precommitTx = return ()

-- ** Read/Write Transactions

readWrite :: RunTransaction ReadWrite TxState Connection a
readWrite = Tx.transact readWriteInterface

readWriteInterface :: TransactionI ReadWrite TxState Connection
readWriteInterface = TransactionI {..}
  where
    _beginTx = beginTx False
    _commitTx (TxState _ txn) = mdb_txn_commit txn
    _abortTx (TxState _ txn) = mdb_txn_abort txn
    _precommitTx = return ()

-- * Actions

exists :: Key -> Tx mode Bool
exists k = (get k $> True) `Except.catchError` handleError
  where
    handleError :: Error mode -> Tx mode Bool
    handleError e =
      case e of
        KeyDoesNotExist -> return False
        _               -> Except.throwError e

get :: Key -> Tx mode Value
get k = do
  (TxState lmdb@(Connection _ db) txn) <- State.get
  result <- liftIO $! do
    keyMDB <- makeMDBKey k
    valMDB <- mdb_get txn db keyMDB
    maybe (return Nothing) (fmap Just . getValueFromMDB) valMDB
  maybe (Except.throwError KeyDoesNotExist) return result

create :: Tx ReadWrite Key -> Value -> Tx ReadWrite Key
create keygen value = do
  key <- keygen
  put key value
  return key

put :: Key -> Value -> Tx ReadWrite ()
put k v = do
  (TxState lmdb@(Connection env db) txn) <- State.get
  result <- liftIO $! do
    keyMDB <- makeMDBKey k
    valMDB <- makeMDBVal v
    mdb_put writeFlags txn db keyMDB valMDB
  unless result $ Except.throwError KeyAlreadyExists

delete :: Key -> Tx ReadWrite ()
delete k = do
  (TxState lmdb@(Connection _ db) txn) <- State.get
  result <- liftIO $! do
    keyMDB <- makeMDBKey k
    mdb_del txn db keyMDB Nothing
  unless result $ Except.throwError KeyDoesNotExist

data Stats
  = Stats
      { sPageSize      :: !Word32
      , sDepth         :: !Word32
      , sBranchPages   :: !Word64
      , sLeafPages     :: !Word64
      , sOverflowPages :: !Word64
      , sEntries       :: !Word64
      }
  deriving (Show)

getStats :: Tx ReadOnly Stats
getStats = do
  (TxState (Connection _ db) txn) <- State.get
  statsMDB <- liftIO $! mdb_stat txn db
  return $
    Stats
      { sPageSize = fromIntegral $ ms_psize statsMDB
      , sDepth = fromIntegral $ ms_depth statsMDB
      , sBranchPages = fromIntegral $ ms_branch_pages statsMDB
      , sLeafPages = fromIntegral $ ms_leaf_pages statsMDB
      , sOverflowPages = fromIntegral $ ms_overflow_pages statsMDB
      , sEntries = fromIntegral $ ms_entries statsMDB
      }

-- * 'SmartValue' Interface

smartValueInterface :: SmartValue.StoreInterface Key mode TxState Connection
smartValueInterface =
  SmartValue.StoreInterface
    { SmartValue._siGet = fmap LazyByteString.fromStrict . get
    , SmartValue._siDelete = delete
    , SmartValue._siPut = \k -> put k . LazyByteString.toStrict
    }

-- * Helpers

getEnvironment :: Connection -> MDB_env
getEnvironment (Connection env _) = env

flush :: Connection -> IO ()
flush (Connection env _) = mdb_env_sync_flush env

-- ** Internal Helpers

beginTx :: Bool -> Connection -> IO TxState
beginTx ro lmdb@(Connection env _) = do
  mdbTxn <- mdb_txn_begin env Nothing ro
  return $ TxState lmdb mdbTxn

-- *** Raw LMDB Interface

-- | Create an LMDB environment.
createEnvironment ::
     FilePath          -- ^ The database location on disk.
  -> Int               -- ^ Maximum number of readers.
  -> IO (MDB_env, Int) -- ^ The returned LMDB environment and its page size in bytes.
createEnvironment envPath maxReaders = do
  exists <- doesDirectoryExist envPath
  unless exists $ createDirectory envPath
  env <- mdb_env_create
  mdb_env_set_maxdbs env numDBs
  mdb_env_set_maxreaders env maxReaders
  mdb_env_set_mapsize env maxDBSize
  mdb_env_open env envPath [MDB_NOLOCK, MDB_NOMETASYNC, MDB_NOSYNC, MDB_NORDAHEAD]
  envStats <- mdb_env_stat env
  let pageSize = fromIntegral $ ms_psize envStats
  return (env, pageSize)
  where
    maxDBSize = 10 * (1024 ^ 4) -- 10 terabytes
    numDBs = 1

-- | Open a connection to an LMDB database.
openDatabase ::
     String -- ^ Name of the database to open.
  -> MDB_env -- ^ LMDB environment containing the database to open.
  -> IO MDB_dbi -- ^ The returned, opened LMDB database.
openDatabase dbName env = do
  txn <- mdb_txn_begin env Nothing False
  db <- mdb_dbi_open txn (Just dbName) [MDB_CREATE]
  mdb_txn_commit txn
  return db

-- | Flags used for a standard write operation to LMDB.
writeFlags = compileWriteFlags [MDB_NOOVERWRITE]

-- | Convert a ByteString into an LMDB value.
makeMDBVal :: ByteString -> IO MDB_val
{-# INLINE makeMDBVal #-}
makeMDBVal bs =
  UnsafeByteString.unsafeUseAsCStringLen
    bs
    (\(ptr, len) -> return (MDB_val (fromIntegral len) (castPtr ptr)))

-- | Make an LMDB key from the given database key.
makeMDBKey :: Key -> IO MDB_val
{-# INLINE makeMDBKey #-}
makeMDBKey = makeMDBVal

-- | Retrieve a strict ByteString value from the given LMDB value.
getValueFromMDB :: MDB_val -> IO ByteString
{-# INLINE getValueFromMDB #-}
getValueFromMDB (MDB_val size value) =
  ByteString.packCStringLen (castPtr value, fromIntegral size)
