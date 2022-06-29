{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE RecordWildCards            #-}

module Database.Lib.Tx
    ( Error (..)
    , ReadOnly
    , ReadWrite
    , RunTransaction
    , TransactionI (..)
    , Tx
    , cast
    , decodeByteStringOrThrow
    , decodeStrictByteStringOrThrow
    , transact
    ) where

import           Control.Applicative    (Alternative)
import           Control.Exception      (bracketOnError)
import           Control.Monad.Except   (ExceptT, MonadError)
import qualified Control.Monad.Except   as Except
import           Control.Monad.IO.Class (MonadIO (liftIO))
import           Control.Monad.State    (MonadState, StateT)
import qualified Control.Monad.State    as State
import           Data.Binary            (Binary)
import qualified Data.Binary            as Binary
import qualified Data.ByteString        as ByteString
import           Data.ByteString.Lazy   (ByteString)
import qualified Data.ByteString.Lazy   as LazyByteString
import           Data.Text              (Text)
import qualified Data.Text              as Text
import qualified Lib.Either             as Either
import qualified Lib.Tuple              as Tuple
import qualified MDRN.Data              as Data
import qualified MDRN.Data.Decode       as Decode
import           MDRN.Data.Encode       (ToData (..))

-- * Transactions

newtype Tx mode state db a
  = Tx (ExceptT (Error mode) (StateT state IO) a)
  deriving
    ( Alternative
    , Applicative
    , Functor
    , Monad
    , MonadError (Error mode)
    , MonadIO
    , MonadState state
    )

data ReadOnly
data ReadWrite

data TransactionI mode state db
  = TransactionI
      { _beginTx     :: db -> IO state
      , _commitTx    :: state -> IO ()
      , _abortTx     :: state -> IO ()
      , _precommitTx :: Tx mode state db ()
      }

type RunTransaction mode state db a = db -> Tx mode state db a -> IO (Either (Error mode) a)

transact :: TransactionI mode state db -> RunTransaction mode state db a
transact TransactionI {..} db domainTx =
  bracketOnError (_beginTx db) _abortTx runTx
    where
      tx = domainTx <* _precommitTx
      runTx txState = do
        (outcome, txStateEnd) <- execTx tx txState
        either (const _abortTx) (const _commitTx) outcome txStateEnd
        return outcome

cast
  :: (parentState -> childState)
  -> (parentState -> childState -> parentState)
  -> Tx mode childState child a
  -> Tx mode parentState parent a
cast getChildState setChildState childTx = do
  parentState <- State.get
  (outcome, newChildState) <- liftIO $ execTx childTx (getChildState parentState)
  State.modify (`setChildState` newChildState)
  Except.liftEither outcome

execTx :: Tx mode state db a -> state -> IO (Either (Error mode) a, state)
execTx (Tx m) = State.runStateT (Except.runExceptT m)

-- * Error

data Error mode where
  KeyDoesNotExist :: Error mode
  InvalidValue :: Error mode
  Failure :: Text -> Error mode
  UnsuccessfulWrite :: Error ReadWrite
  KeyAlreadyExists :: Error ReadWrite
  EmptyValue :: Error ReadWrite
  Multiple :: [Error mode] -> Error mode

instance Eq (Error mode) where
  (==) KeyDoesNotExist KeyDoesNotExist     = True
  (==) InvalidValue InvalidValue           = True
  (==) (Failure a) (Failure b)             = a == b
  (==) UnsuccessfulWrite UnsuccessfulWrite = True
  (==) KeyAlreadyExists KeyAlreadyExists   = True
  (==) EmptyValue EmptyValue               = True
  (==) (Multiple es1) (Multiple es2)       = es1 == es2
  (==) a b                                 = False

instance Show (Error mode) where
  show err =
    case err of
      KeyDoesNotExist   -> "KeyDoesNotExist"
      InvalidValue      -> "InvalidValue"
      Failure s         -> "Failure " <> Text.unpack s
      UnsuccessfulWrite -> "UnsuccessfulWrite"
      KeyAlreadyExists  -> "KeyAlreadyExists"
      EmptyValue        -> "EmptyValue"
      Multiple es       -> show es

instance ToData (Error mode) where
  toData err =
    case err of
      Failure s         -> Data.list [Data.symbol "failure", Data.text s]
      KeyDoesNotExist   -> Data.symbol "key-does-not-exist"
      InvalidValue      -> Data.symbol "invalid-value"
      UnsuccessfulWrite -> Data.symbol "unsuccessful-write"
      KeyAlreadyExists  -> Data.symbol "key-already-exists"
      EmptyValue        -> Data.symbol "empty-value"
      Multiple es       -> Data.list (map toData es)

instance Decode.FromData (Error ReadOnly) where
  decoder =
    Decode.oneOf
      [ KeyDoesNotExist <$ Decode.symbolEq "key-does-not-exist"
      , InvalidValue <$ Decode.symbolEq "invalid-value"
      , Multiple <$> Decode.list Decode.decoder
      ]

instance Decode.FromData (Error ReadWrite) where
  decoder =
    Decode.oneOf
      [ KeyDoesNotExist <$ Decode.symbolEq "key-does-not-exist"
      , InvalidValue <$ Decode.symbolEq "invalid-value"
      , Failure <$ Decode.symbolEq "failure" <*> Decode.text
      , UnsuccessfulWrite <$ Decode.symbolEq "unsuccessful-write"
      , KeyAlreadyExists <$ Decode.symbolEq "key-already-exists"
      , EmptyValue <$ Decode.symbolEq "empty-value"
      , Multiple <$> Decode.list Decode.decoder
      ]

instance Semigroup (Error mode) where
  (<>) (Multiple es0) (Multiple es1) = Multiple $ es0 ++ es1
  (<>) e (Multiple es)               = Multiple $ e : es
  (<>) (Multiple es) e               = Multiple $ es ++ [e]
  (<>) e0 e1                         = Multiple [e0, e1]

instance Monoid (Error mode) where
  mempty = Multiple []

-- * Helpers

decodeByteStringOrThrow :: Binary a => Error mode -> LazyByteString.ByteString -> Tx mode state db a
decodeByteStringOrThrow e bs =
  either (const (Except.throwError e)) (return . Tuple.third) $ Binary.decodeOrFail bs

decodeStrictByteStringOrThrow :: Binary a => Error mode -> ByteString.ByteString -> Tx mode state db a
decodeStrictByteStringOrThrow e bs = decodeByteStringOrThrow e $ LazyByteString.fromStrict bs
