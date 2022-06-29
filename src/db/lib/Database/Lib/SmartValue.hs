module Database.Lib.SmartValue
    ( Hash
    , SmartValue
    , StoreInterface (..)
    , get
    , hash
    , new
    , update
    , value
    ) where

import           Control.Monad        (when)
import qualified Control.Monad.Except as Except
import           Data.Binary          (Binary)
import qualified Data.Binary          as Binary
import           Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Lazy as ByteString
import           Data.Functor         (($>))
import           Data.Hashable        (Hashable)
import qualified Data.Hashable        as Hashable
import           Database.Lib.Tx      (Tx)
import qualified Database.Lib.Tx      as Tx
import qualified Lib.Either           as Either

-- * Smart Values

-- | A @'SmartValue' a@ represents a value of type @a@ and its associated 'Hash'.
-- It is useful for representing large values that need to be persisted in databases,
-- as the associated 'Hash' prevents unnecessary writes to the database when the
-- internal value has not changed, as indicated by its hash.
data SmartValue a
  = SmartValue
      { hash  :: Hash
      , value :: a
      }
  deriving (Eq, Show)

instance Binary a => Binary (SmartValue a) where
  get = SmartValue <$> Binary.get <*> Binary.get
  put a = Binary.put (hash a) >> Binary.put (value a)

-- | Create a @'SmartValue' a@ for a given value of type @a@. This function
-- internally hashes the value.
new :: Hashable a => a -> SmartValue a
new value = SmartValue (Hashable.hash value) value

-- | Get a @'SmartValue' a' from a store given its 'StoreInterface@.
-- This function throws the 'Tx.InvalidValue' error if the 'SmartValue'
-- cannot be decoded from a 'ByteString'. It also calls '_siGet' without
-- a preceding existence check, so if '_siGet' may throw an error depending
-- on the underlying store's implementation.
get :: Binary a => StoreInterface key mode txState db -> key -> Tx mode txState db (SmartValue a)
get interface key = do
  -- Don't do an existence check, just throw an error if the key doesn't exist.
  rawValue <- _siGet interface key
  maybe (Except.throwError Tx.InvalidValue) return $ do
    (_, _, smartValue) <- Either.toMaybe $ Binary.decodeOrFail rawValue
    return smartValue

-- | Update a @'SmartValue' a' in a store given its 'StoreInterface@.
-- This function uses 'get' internally to retrieve the 'SmartValue''s current
-- hash. If the call to 'get' succeeds, this function overwrites the 'SmartValue' in
-- the underlying store only if the new value's hash differs from the existing one.
-- If the call to 'get' throws a 'Tx.KeyNotFound' error, then the new value is
-- inserted into the store as a new 'SmartValue'. All other errors thrown by
-- 'get' are re-thrown to avoid overwriting the wrong data unintentionally.
update :: (Binary a, Hashable a) => StoreInterface key Tx.ReadWrite txState db -> key -> a -> Tx Tx.ReadWrite txState db (SmartValue a)
update interface key newValue = do
  -- Get the existing 'SmartValue' and wrap it in a 'Maybe'.
  -- If 'Nothing', it doesn't exist and needs to simply be "put."
  -- If 'Just', it does exist and needs to be updated if the hashes differ.
  --
  -- Also, we get the entire smart value from the database, even though
  -- we only need its hash in this function. The theory is that lazy evaluation will allow
  -- us to disregard the actual value, and only evaluate the hash into memory.
  -- The Binary instance was designed this way (hash comes before value).
  -- Proper benchmarking is the only way to know for sure.
  -- The benefit of this approach is that the underlying store is the
  -- "source of truth" for the hash, reducing the need to track full smart values and
  -- their hashes in memory.
  maybeExistingSmartValue <- get' interface key newValue `Except.catchError` handleGetError interface
  let newSmartValue = new newValue
  case maybeExistingSmartValue of
    -- If nothing exists at the specified @key@ in the database,
    -- simply put the @newSmartValue@.
    Nothing -> put interface key newSmartValue $> newSmartValue
    -- Otherwise, updated the smart value on disk if needed.
    Just existingSmartValue -> do
      -- Only replace the smart value in the database if the the hashes of the
      -- @existingSmartValue@ and @newSmartValue@ don't match.
      when (hash existingSmartValue /= hash newSmartValue) (replace interface key newSmartValue)
      return newSmartValue
  where
    -- Pass @a@ as an argument only to satisfy the compiler.
    get' :: Binary a => StoreInterface key mode txState db -> key -> a -> Tx mode txState db (Maybe (SmartValue a))
    get' interface key _ = Just <$> get interface key
    -- Pass 'StoreInterface' as an argument only to satisfy the compiler.
    handleGetError :: Binary a => StoreInterface key Tx.ReadWrite txState db -> Tx.Error Tx.ReadWrite -> Tx Tx.ReadWrite txState db (Maybe (SmartValue a))
    handleGetError _ e =
      case e of
        -- Only return 'Nothing' when nothing exists at the specified key.
        Tx.KeyDoesNotExist -> return Nothing
        -- Otherwise, passthrough the 'Tx.Error' to avoid overwriting data accidentally.
        _                  -> Except.throwError e
    put :: Binary a => StoreInterface key Tx.ReadWrite txState db -> key -> SmartValue a -> Tx Tx.ReadWrite txState db ()
    put interface key smartValue = _siPut interface key $ Binary.encode smartValue
    replace :: Binary a => StoreInterface key Tx.ReadWrite txState db -> key -> SmartValue a -> Tx Tx.ReadWrite txState db ()
    replace interface key smartValue = _siDelete interface key >> put interface key smartValue

-- * Hashes

-- | The 'Hash' stored in a 'SmartValue'. This is an alias for 'Int' to
-- ensure compatibility with the 'Hashable' typeclass.
type Hash = Int

-- * Store Instances

-- | A @'StoreInterface' key mode txState db@ allows database stores to
-- implement how 'SmartValue's can be stored and accessed. An interface is
-- used instead of a typeclass for simplicity given the number of type parameters.
--
-- This module assumes underlying stores persist values as 'ByteString's.
-- Other value types are not currently supported.
data StoreInterface key mode txState db
  = StoreInterface
      { _siGet    :: key -> Tx mode txState db ByteString
      , _siDelete :: key -> Tx Tx.ReadWrite txState db ()
      , _siPut    :: key -> ByteString -> Tx Tx.ReadWrite txState db ()
      }
