{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Database.Lib.Data.Dict
  ( Dict,
    delete,
    empty,
    exists,
    get,
    put,
  )
where

import Data.Binary (Binary)
import Data.Binary qualified as Binary
import Data.HashMap.Strict qualified as HashMap
import Data.Hashable (Hashable)

newtype Dict k v
  = Dict (HashMap.HashMap k v)
  deriving (Hashable, Eq)

instance (Eq k, Hashable k, Binary k, Binary v) => Binary (Dict k v) where
  put (Dict d) = Binary.put $ HashMap.toList d
  get = Dict . HashMap.fromList <$> Binary.get

-- | An empty dictionary.
empty :: Dict k v
empty = Dict HashMap.empty

-- | Returns 'True' if and only if the given key is present in the dictionary.
exists :: (Eq k, Hashable k) => k -> Dict k v -> Bool
exists k (Dict d) = HashMap.member k d

-- | Returns the value corresponding to the given key.
-- 'Nothing' if the dictionary does not contain the given key.
get :: (Eq k, Hashable k) => k -> Dict k v -> Maybe v
get k (Dict d) = HashMap.lookup k d

-- | Associates the given value with the given key.
-- If the dictionary previously contained the given key then 'Nothing' is returned.
put :: (Eq k, Hashable k) => k -> v -> Dict k v -> Maybe (Dict k v)
put k v (Dict d) =
  if HashMap.member k d
    then Nothing
    else Just $ Dict $ HashMap.insert k v d

-- | Removes the given key. Returns 'Nothing' if the dictionary does not contain the given key.
delete :: (Eq k, Hashable k) => k -> Dict k v -> Maybe (Dict k v)
delete k (Dict d) =
  if HashMap.member k d
    then Just $ Dict $ HashMap.delete k d
    else Nothing
