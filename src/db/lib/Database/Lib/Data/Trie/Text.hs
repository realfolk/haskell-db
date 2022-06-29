{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Database.Lib.Data.Trie.Text
  ( Trie,
    deleteOne,
    empty,
    insert,
    startsWith,
  )
where

import Data.Binary (Binary)
import Data.Binary qualified as Binary
import Data.Hashable
import Data.Set (Set)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Trie.Text qualified as Trie

newtype Trie a
  = Trie (Trie.Trie (Set.Set a))
  deriving (Eq, Binary)

-- TODO A 'Hashable' instance for 'Set' does not exist in the hashable-1.3.0.0 pkg.
-- This instance can be removed in favour of deriving Hashable using GeneralizedNewTypeDeriving
-- after upgrading the hashable package to a newer version.
instance (Binary a, Eq a) => Hashable (Trie a) where
  hashWithSalt salt (Trie t) = hashWithSalt salt $ Binary.encode t

-- | An empty trie.
empty :: Trie a
empty = Trie Trie.empty

-- | Returns the set of values that starts with the given prefix.
startsWith :: Ord a => Text -> Trie a -> Set.Set a
startsWith k (Trie t) = Set.unions $ Trie.elems $ Trie.submap k t

-- | Inserts the given value into the trie by adding it to the set of values associated with the given key.
insert :: Ord a => Text -> a -> Trie a -> Trie a
insert k v (Trie t) =
  case Trie.lookup k t of
    Just vs ->
      Trie $ Trie.insert k (Set.insert v vs) t
    Nothing ->
      Trie $ Trie.insert k (Set.singleton v) t

-- | Removes the given value from the set of values associated with the given key.
-- 'Nothing' is returned if the given key does not exist.
deleteOne :: Ord a => Text -> a -> Trie a -> Maybe (Trie a)
deleteOne k v (Trie t) = Trie.lookup k t >>= del
  where
    del vs = Just $ Trie $ Trie.insert k (Set.delete v vs) t
