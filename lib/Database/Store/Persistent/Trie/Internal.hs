{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Database.Store.Persistent.Trie.Internal
    ( Connection (..)
    , Tx
    , TxState (..)
    , fromLMDB
    ) where

import           Database.Lib.Sync                   (Sync)
import qualified Database.Lib.Tx                     as Tx
import qualified Database.Store.Persistent.Trie.LMDB as LMDB

-- * Core Types

data Connection
  = Connection
      { _connSync           :: !Sync
      , _connLMDBConnection :: !LMDB.Connection
      }

data TxState
  = TxState
      { _txConnection :: !Connection
      , _txLMDBState  :: !LMDB.TxState
      }

type Tx mode o = Tx.Tx mode TxState Connection o

-- * Helpers

fromLMDB :: LMDB.Tx mode o -> Tx mode o
fromLMDB = Tx.cast _txLMDBState setConnectionState
  where
    setConnectionState txState newLMDBState =
      txState { _txLMDBState = newLMDBState }
