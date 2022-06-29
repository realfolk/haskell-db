{-|
Module: Database.Lib.IDManager
Description: Helpers to manage auto-succeeding and recycled IDs for a database.
Copyright: (c) Real Folk Inc. 2021
Maintainer: admin@realfolk.com
Stability: experimental
Portability: POSIX
-}

module Database.Lib.IDManager
    ( IDManager
    , empty
    , next
    , recycle
    , unrecycle
    ) where

import           Data.Binary   (Binary)
import qualified Data.Binary   as Binary
import           Data.Hashable (Hashable (hashWithSalt))
import           Data.Set      (Set)
import qualified Data.Set      as Set

-- * Core Types

-- | An @'IDManager' id@ is useful for managing a database's ID-space.
-- It does this by tracking its next available ID via '_nextID' and a
-- 'Set' of recycled IDs for re-use via '_recycledIDs'.
--
-- This is useful when using non-cryptographic IDs in a database like
-- 'Data.Word.Word64'. For example, if you are storing blog posts in
-- your database, you may have the first blog post have an ID of
-- @1 :: 'Data.Word.Word64'@. The next blog post that is inserted into
-- the database could have an ID of @2@, and so on. An @'IDManager' 'Data.Word.Word64'@
-- would help alleviate the burden of tracking these IDs, incrementing
-- them and recycling the IDs of deleted blog posts.
--
-- The 'Binary' and 'Hashable' instances of 'IDManager' are also
-- intended to assist with the efficient serialization and persistence
-- of the 'IDManager' to a database.
data IDManager id
  = IDManager
      { _nextID      :: !id
      , _recycledIDs :: !(Set id)
      }
  deriving (Eq, Show)

instance Binary id => Binary (IDManager id) where
  get = IDManager <$> Binary.get <*> Binary.get
  put (IDManager nextID recycled) = Binary.put nextID >> Binary.put recycled

instance Hashable id => Hashable (IDManager id) where
  hashWithSalt salt (IDManager nextID recycled) =
    hashWithSalt (hashWithSalt salt nextID) (Set.toList recycled)

-- * Helpers

-- | Create an 'IDManager' with a starting @id@ and an empty set of recycled @id@s.
empty :: id -> IDManager id
empty startingID = IDManager startingID Set.empty

-- | Recycle an @id@ by inserting it into the 'Set' of recycled @id@s in an 'IDManager'.
-- It is not possible to recycle an @id@ that is lower than the 'IDManager's @nextID@
-- to avoid overwriting future IDs.
recycle :: Ord id => id -> IDManager id -> Maybe (IDManager id)
recycle id' (IDManager nextID recycled) =
  if id' >= nextID
     then Nothing
     else Just $ IDManager nextID $ Set.insert id' recycled

-- | Unrecycle an @id@ from the 'IDManager'. If the @id@ has not previously been recycled, the 'IDManager' remains unchanged.
unrecycle :: Ord id => id -> IDManager id -> IDManager id
unrecycle id' (IDManager nextID recycled) = IDManager nextID $ Set.delete id' recycled

-- | The @'next' idManager@ function is equivalent to @'nextWith' succ idManager@
-- and is useful when an @'IDManager' id@ uses an @id@ that is an instance of 'Enum'.
next :: Enum id => IDManager id -> (id, IDManager id)
next = nextWith succ

-- ** Internal Helpers

-- | The @'nextWith' succ' idManager@ function gets the next available
-- @id@ from an 'IDManager'. If the 'IDManager' has any recycled @id@s
-- available for re-use, it will return the smallest recycled @id@.
-- Otherwise, it will return the next \"new\" @id@.
--
-- The @succ'@ argument determines how to succeed (i.e. increment) the
-- next \"new\" @id@ if it is consumed from the 'IDManager'.
--
-- @
-- -- Example 0
-- exampleIDManager0 = newEmptyIDManager 0 :: IDManager Word64
-- exampleNextID0 = nextWith (+1) exampleIDManager0 -- returns (0, IDManager 1 Set.empty)
-- -- Example 1
-- exampleIDManager1 = newIDManager 0 (Set.fromList [2,4,6]) :: IDManager Word64
-- exampleNextID1 = nextWith (+1) exampleIDManager1 -- returns (2, IDManager 0 (Set.fromList [4,6]))
-- @
--
-- This function is not exported to help users avoid shooting themselves in the foot
-- by, for example, succeeding the @id@ to an already-used @id@, which can lead to
-- data corruption.
nextWith :: (id -> id) -> IDManager id -> (id, IDManager id)
nextWith succ' (IDManager nextID recycled) =
  case Set.minView recycled of
    Nothing -> (nextID, IDManager (succ' nextID) recycled)
    Just (recycledID, remainingRecycled) -> (recycledID, IDManager nextID remainingRecycled)
