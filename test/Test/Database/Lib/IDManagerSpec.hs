{-# LANGUAGE OverloadedStrings #-}

module Test.Database.Lib.IDManagerSpec
    ( spec
    ) where

import qualified Data.Bifunctor         as Bifunctor
import           Data.ByteString.Lazy   (ByteString)
import           Data.Function          ((&))
import           Data.List.NonEmpty     (NonEmpty (..))
import qualified Data.List.NonEmpty     as NonEmpty
import           Data.Maybe             (isJust)
import           Database.Lib.IDManager (IDManager)
import qualified Database.Lib.IDManager as IDManager
import           Test.Hspec
import qualified Test.Lib.Spec.Binary   as BinarySpec
import qualified Test.Lib.Spec.Hashable as HashableSpec

-- * Main

spec :: Spec
spec = do
  nextSpec
  recycleSpec
  unrecycleSpec
  BinarySpec.create "IDManager" idManagerWithRecycledIDs idManagerWithRecycledIDsByteString
  HashableSpec.create "IDManager" idManagerWithRecycledIDs 999 2363842136275519587

-- * Mock Data

emptyIDManager :: IDManager Word
emptyIDManager = IDManager.empty 0

(Just idManagerWithRecycledIDs)
    = IDManager.empty (5 :: Word)
    & IDManager.recycle 2
  >>= IDManager.recycle 1
  >>= IDManager.recycle 3

idManagerWithRecycledIDsByteString :: ByteString
idManagerWithRecycledIDsByteString = "\NUL\NUL\NUL\NUL\NUL\NUL\NUL\ENQ\NUL\NUL\NUL\NUL\NUL\NUL\NUL\ETX\NUL\NUL\NUL\NUL\NUL\NUL\NUL\SOH\NUL\NUL\NUL\NUL\NUL\NUL\NUL\STX\NUL\NUL\NUL\NUL\NUL\NUL\NUL\ETX"

-- * Specs

nextSpec :: Spec
nextSpec =
  describe "next" $ do
    context "when the IDManager is empty" $ do
      context "and is requested for only one next ID" $
        beforeAll (return (IDManager.next emptyIDManager)) $ do
          it "returns the starting ID" $ \(id', _) ->
            id' `shouldBe` 0
          it "updates the IDManager's internal state correctly" $ \(_, idManager) ->
            idManager `shouldBe` IDManager.empty 1
      context "and is requested for several next IDs" $
        beforeAll (return (nextMany 4 emptyIDManager)) $ do
          it "returns the correct ID successor" $ \(ids, _) ->
            Just ids `shouldBe` NonEmpty.nonEmpty [0, 1, 2, 3]
          it "updates the IDManager's internal state correctly" $ \(_, idManager) ->
            idManager `shouldBe` IDManager.empty 4
    context "when the IDManager has recycled IDs" $ do
      context "and is requested for only one next ID" $
          beforeAll (return (IDManager.next idManagerWithRecycledIDs)) $ do
        it "returns the the smallest recycled ID" $ \(id', _) ->
          id' `shouldBe` 1
        it "updates the IDManager's internal state correctly" $ \(_, idManager) ->
          shouldBe (Just idManager)
              $ IDManager.empty 5
              & IDManager.recycle 2
            >>= IDManager.recycle 3
      context "and is requested for several next IDs" $ do
        context "and the requests do not exhaust all recycled IDs" $
          beforeAll (return (nextMany 2 idManagerWithRecycledIDs)) $ do
            it "returns the recycled IDs in the correct order" $ \(ids, _) ->
              Just ids `shouldBe` NonEmpty.nonEmpty [1, 2]
            it "updates the IDManager's internal next ID correctly" $ \(_, idManager) ->
              shouldBe (Just idManager)
                $ IDManager.empty 5
                & IDManager.recycle 3
        context "and the requests do exhaust all recycled IDs" $
          beforeAll (return (nextMany 5 idManagerWithRecycledIDs)) $ do
            it "returns the recycled and new IDs in the correct order" $ \(ids, _) ->
              Just ids `shouldBe` NonEmpty.nonEmpty [1, 2, 3, 5, 6]
            it "updates the IDManager's internal next ID correctly" $ \(_, idManager) ->
              idManager `shouldBe` IDManager.empty 7

recycleSpec :: Spec
recycleSpec =
  describe "recycle" $ do
    context "when the IDManager is empty" $ do
      context "and its starting ID is the minBound" $
        context "when recycling a single ID" $
          it "always returns Nothing" $ do
            let idManager = IDManager.empty (minBound :: Word)
            IDManager.recycle minBound idManager `shouldBe` Nothing
            IDManager.recycle 10 idManager `shouldBe` Nothing
            IDManager.recycle 298379 idManager `shouldBe` Nothing
            IDManager.recycle maxBound idManager `shouldBe` Nothing
      context "and its starting ID is greater than the minBound and lower than the maxBound" $
        beforeAll (return (IDManager.empty (minBound + 10 :: Word))) $ do
          context "when recycling a single ID" $ do
            context "that is lower than the starting ID" $ do
              context "once" $
                it "successfully recycles the ID" $ \idManager -> do
                  let maybeRecycledID = do
                        idManager' <- IDManager.recycle (minBound + 5) idManager
                        return $ fst $ IDManager.next idManager'
                  maybeRecycledID `shouldBe` Just (minBound + 5)
              context "repeatedly" $ do
                it "successfully recycles the ID once without duplication" $ \idManager -> do
                  let recyclableID = minBound + 5
                  let idManager'
                          = idManager
                          & IDManager.recycle recyclableID
                        >>= IDManager.recycle recyclableID
                  idManager' `shouldBe` IDManager.recycle recyclableID idManager
            context "that is greater than the starting ID" $
                it "returns Nothing" $ \idManager ->
                  IDManager.recycle (minBound + 11) idManager `shouldBe` Nothing
            context "that is equal to the starting ID" $
                it "returns Nothing" $ \idManager ->
                  IDManager.recycle (minBound + 10) idManager `shouldBe` Nothing
    context "when the IDManager already has recycled IDs" $ do
      context "when recycling a single ID" $
        context "that is lower than the nextID" $ do
          context "and is lower than all existing recycled IDs" $
            beforeAll (return (IDManager.recycle 0 idManagerWithRecycledIDs)) $ do
              it "successfully recycles the ID" $ \idManager ->
                idManager `shouldSatisfy` isJust
              it "retrieves the newly-recycled ID when calling next" $ \idManager ->
                fmap (fst . IDManager.next) idManager `shouldBe` Just 0
          context "and is greater than all existing recycled IDs" $
            beforeAll (return (IDManager.recycle 4 idManagerWithRecycledIDs)) $ do
              it "successfully recycles the ID" $ \idManager ->
                idManager `shouldSatisfy` isJust
              it "retrieves the newly-recycled ID when calling next" $ \idManager ->
                fmap (fst . nextMany 4) idManager `shouldBe` NonEmpty.nonEmpty [1, 2, 3, 4]
          context "and is equal to an existing recycled ID" $
            beforeAll (return (IDManager.recycle 2 idManagerWithRecycledIDs)) $ do
              it "successfully recycles the ID" $ \idManager ->
                idManager `shouldSatisfy` isJust
              it "does not duplicate the ID in the ID manager" $ \idManager ->
                idManager `shouldBe` Just idManagerWithRecycledIDs

unrecycleSpec :: Spec
unrecycleSpec =
  describe "unrecycle" $ do
    context "when the IDManager is empty" $ do
      it "is always a no-op" $ do
        IDManager.unrecycle minBound emptyIDManager `shouldBe` emptyIDManager
        IDManager.unrecycle 10 emptyIDManager `shouldBe` emptyIDManager
        IDManager.unrecycle 298379 emptyIDManager `shouldBe` emptyIDManager
        IDManager.unrecycle maxBound emptyIDManager `shouldBe` emptyIDManager
    context "when the IDManager already has recycled IDs" $ do
      context "and unrecycling a previously-recycled single ID" $
        it "successfully unrecycles the ID" $
          shouldBe (Just (IDManager.unrecycle 2 idManagerWithRecycledIDs))
              $ IDManager.empty 5
              & IDManager.recycle 1
            >>= IDManager.recycle 3
      context "and unrecycling a single ID that has never been recycled before" $
        it "is always a no-op" $ do
          IDManager.unrecycle minBound idManagerWithRecycledIDs `shouldBe` idManagerWithRecycledIDs
          IDManager.unrecycle 10 idManagerWithRecycledIDs `shouldBe` idManagerWithRecycledIDs
          IDManager.unrecycle 298379 idManagerWithRecycledIDs `shouldBe` idManagerWithRecycledIDs
          IDManager.unrecycle maxBound idManagerWithRecycledIDs `shouldBe` idManagerWithRecycledIDs


-- * Helpers

nextMany :: Enum a => Word -> IDManager a -> (NonEmpty a, IDManager a)
nextMany n idManager
  | n == 0 = error "Invalid argument: n must be greater than or equal to 1"
  | n == 1 = (nextID :| [], newIDManager)
  | otherwise = Bifunctor.first (NonEmpty.cons nextID) $ nextMany (n - 1) newIDManager
    where
      (nextID, newIDManager) = IDManager.next idManager
