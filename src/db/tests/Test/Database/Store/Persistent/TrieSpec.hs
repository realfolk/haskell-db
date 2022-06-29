{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TupleSections     #-}

module Test.Database.Store.Persistent.TrieSpec
    ( spec
    ) where

import           Control.Exception                       (bracket)
import           Control.Monad                           (join)
import qualified Control.Monad.State                     as State
import qualified Data.Binary                             as Binary
import           Data.ByteString                         (ByteString)
import qualified Data.ByteString                         as ByteString
import           Data.Function                           ((&))
import qualified Data.Map.Strict                         as Map
import           Data.Maybe                              (fromMaybe)
import qualified Database.Store.Persistent.LMDB.Base     as LMDBBase
import qualified Database.Store.Persistent.Trie          as Trie
import qualified Database.Store.Persistent.Trie.Internal as TrieInternal
import qualified Database.Store.Persistent.Trie.LMDB     as LMDB
import           System.IO.Temp                          (withSystemTempDirectory)
import           Test.Hspec

-- * Constants

tempDirectoryName = "persistent-trie"
syncInterval = 20000

-- * Main

spec :: Spec
spec = do
  around withEmptyDB $ do
    insertSpec
  around withSeededDB $ do
    existsSpec
    getSpec
    searchSpec
    deleteSpec

-- * Mock Data

(mockRootKey, mockRootValue) = (ByteString.empty, ByteString.pack [10,9,8])
(mockKey0, mockValue0) = (ByteString.pack [0,1,2], ByteString.pack [9,8,7])
(mockKey1, mockValue1) = (ByteString.pack [0,1,2,3], ByteString.pack [8,7,6])
(mockKey2, mockValue2) = (ByteString.pack [0,1,2,4,5], ByteString.pack [7,6,5])
(mockKey3, mockValue3) = (ByteString.pack [0,1,3,4], ByteString.pack [6,5,4])
mockNonExistentKey = ByteString.pack [1,2,3]

-- * Specs

--TODO potentially introspect the underlying LMDB database to ensure
--parent nodes are created and updated correctly.
insertSpec :: SpecWithEmptyDB
insertSpec =
  describe "insert" $ do
    context "when a value does not already exist" $ do
      context "and none of the parent nodes exist" $
        it "persists the value correctly" $ \db -> do
          insertOutcome <- Trie.readWrite db $ Trie.insert mockKey0 mockValue0
          insertOutcome `shouldBe` Right ()
          getOutcome <- Trie.readOnly db $ Trie.get mockKey0
          getOutcome `shouldBe` Right mockValue0
      context "and all of the parent nodes exist" $
        it "persists the value correctly" $ \db -> do
          Right _ <- Trie.readWrite db $ Trie.insert mockKey0 mockValue0
          insertOutcome <- Trie.readWrite db $ Trie.insert mockKey1 mockValue1
          insertOutcome `shouldBe` Right ()
          getOutcome <- Trie.readOnly db $ Trie.get mockKey1
          getOutcome `shouldBe` Right mockValue1
      context "and some of the parent nodes exist" $
        it "persists the value correctly" $ \db -> do
          Right _ <- Trie.readWrite db $ Trie.insert mockKey0 mockValue0
          insertOutcome <- Trie.readWrite db $ Trie.insert mockKey2 mockValue2
          insertOutcome `shouldBe` Right ()
          getOutcome <- Trie.readOnly db $ Trie.get mockKey2
          getOutcome `shouldBe` Right mockValue2
      context "and the key is an empty ByteString" $
        it "persists the value correctly" $ \db -> do
          Right _ <- Trie.readWrite db $ Trie.insert mockRootKey mockRootValue
          getOutcome <- Trie.readOnly db $ Trie.get mockRootKey
          getOutcome `shouldBe` Right mockRootValue
    context "when a value already exists" $
      it "throws the correct error" $ \db -> do
        Right _ <- Trie.readWrite db $ Trie.insert mockKey0 mockValue0
        outcome <- Trie.readWrite db $ Trie.insert mockKey0 mockValue0
        outcome `shouldBe` Left Trie.KeyAlreadyExists

existsSpec :: SpecWithSeededDB
existsSpec =
  describe "exists" $ do
    context "when the value exists" $ do
      context "and it is a leaf node" $
        it "returns the correct value" $ \db -> do
          outcome <- Trie.readOnly db $ Trie.exists mockKey2
          outcome `shouldBe` Right True
      context "and it is not a leaf node" $
        it "returns the correct value" $ \db -> do
          outcome <- Trie.readOnly db $ Trie.exists mockKey0
          outcome `shouldBe` Right True
      context "and it is the root node" $
        it "returns the correct value" $ \db -> do
          outcome <- Trie.readOnly db $ Trie.exists mockRootKey
          outcome `shouldBe` Right True
    context "when the value does not exist" $
      it "returns the correct value" $ \db -> do
        outcome <- Trie.readOnly db $ Trie.exists mockNonExistentKey
        outcome `shouldBe` Right False

getSpec :: SpecWithSeededDB
getSpec =
  describe "get" $ do
    context "when the value exists" $ do
      context "and it is a leaf node" $
        it "returns the correct value" $ \db -> do
          outcome <- Trie.readOnly db $ Trie.get mockKey2
          outcome `shouldBe` Right mockValue2
      context "and it is not a leaf node" $
        it "returns the correct value" $ \db -> do
          outcome <- Trie.readOnly db $ Trie.get mockKey0
          outcome `shouldBe` Right mockValue0
      context "and it is the root node" $
        it "returns the correct value" $ \db -> do
          outcome <- Trie.readOnly db $ Trie.get mockRootKey
          outcome `shouldBe` Right mockRootValue
    context "when the value does not exist" $
      it "throws the correct error" $ \db -> do
        outcome <- Trie.readOnly db $ Trie.get mockNonExistentKey
        outcome `shouldBe` Left Trie.KeyDoesNotExist

searchSpec :: SpecWithSeededDB
searchSpec =
  describe "search" $ do
    context "when the query successfully matches a node" $ do
      context "and it is a leaf node" $ do
        it "returns only the matched value" $ \db -> do
          outcome <- Trie.readOnly db $ Trie.search mockKey2
          outcome `shouldBe` Right (Map.singleton mockKey2 mockValue2)
      context "and it is a parent node" $
        it "returns all descendant values" $ \db -> do
          outcome <- Trie.readOnly db $ Trie.search mockKey0
          Map.empty
            & Map.insert mockKey0 mockValue0
            & Map.insert mockKey1 mockValue1
            & Map.insert mockKey2 mockValue2
            & Right
            & shouldBe outcome
      context "and it is the root node" $
        it "returns all database values" $ \db -> do
          outcome <- Trie.readOnly db $ Trie.search mockRootKey
          Map.empty
            & Map.insert mockRootKey mockRootValue
            & Map.insert mockKey0 mockValue0
            & Map.insert mockKey1 mockValue1
            & Map.insert mockKey2 mockValue2
            & Map.insert mockKey3 mockValue3
            & Right
            & shouldBe outcome
    context "when the query does not match any node" $
      it "throws the correct error" $ \db -> do
        outcome <- Trie.readOnly db $ Trie.search mockNonExistentKey
        outcome `shouldBe` Left Trie.KeyDoesNotExist

--TODO potentially introspect the underlying LMDB database to ensure
--parent nodes are deleted and updated correctly.
deleteSpec :: SpecWithSeededDB
deleteSpec =
  describe "delete" $ do
    context "when the value exists" $ do
      context "and it is a leaf node" $
        it "deletes the value" $ \db -> do
          deleteOutcome <- Trie.readWrite db $ Trie.delete mockKey2
          deleteOutcome `shouldBe` Right ()
          getOutcome <- Trie.readOnly db $ Trie.get mockKey2
          getOutcome `shouldBe` Left Trie.KeyDoesNotExist
      context "and it is not a leaf node" $ do
        it "deletes the value" $ \db -> do
          deleteOutcome <- Trie.readWrite db $ Trie.delete mockKey0
          deleteOutcome `shouldBe` Right ()
          getOutcome <- Trie.readOnly db $ Trie.get mockKey0
          getOutcome `shouldBe` Left Trie.KeyDoesNotExist
        it "retains all descendant values" $ \db -> do
          outcome <- Trie.readWrite db $ Trie.delete mockKey0
          outcome `shouldBe` Right ()
          Trie.readOnly db (Trie.get mockKey1) `shouldReturn` Right mockValue1
          Trie.readOnly db (Trie.get mockKey2) `shouldReturn` Right mockValue2
        it "retains all sibling values" $ \db -> do
          outcome <- Trie.readWrite db $ Trie.delete mockKey0
          outcome `shouldBe` Right ()
          Trie.readOnly db (Trie.get mockKey3) `shouldReturn` Right mockValue3
      context "and it is the root node" $ do
        it "deletes the value" $ \db -> do
          deleteOutcome <- Trie.readWrite db $ Trie.delete mockRootKey
          deleteOutcome `shouldBe` Right ()
          getOutcome <- Trie.readOnly db $ Trie.get mockRootKey
          getOutcome `shouldBe` Left Trie.KeyDoesNotExist
        it "retains all descendant values" $ \db -> do
          outcome <- Trie.readWrite db $ Trie.delete mockRootKey
          outcome `shouldBe` Right ()
          Trie.readOnly db (Trie.get mockKey0) `shouldReturn` Right mockValue0
          Trie.readOnly db (Trie.get mockKey1) `shouldReturn` Right mockValue1
          Trie.readOnly db (Trie.get mockKey2) `shouldReturn` Right mockValue2
          Trie.readOnly db (Trie.get mockKey3) `shouldReturn` Right mockValue3
    context "when the value does not exist" $
      it "throws the correct error" $ \db -> do
        outcome <- Trie.readWrite db $ Trie.delete mockNonExistentKey
        outcome `shouldBe` Left Trie.KeyDoesNotExist

-- * Helpers

type SpecWithEmptyDB = SpecWith (Arg (Trie.Connection -> IO ()))

withEmptyDB :: (Trie.Connection -> IO a) -> IO a
withEmptyDB action =
  withSystemTempDirectory tempDirectoryName $ \dir -> do
    bracket (Trie.connect dir syncInterval) Trie.disconnect action

type SpecWithSeededDB = SpecWith (Arg (Trie.Connection -> IO ()))

withSeededDB :: (Trie.Connection -> IO a) -> IO a
withSeededDB action =
  withEmptyDB $ \connection -> do
    Right _ <- Trie.readWrite connection seedMockData
    action connection

-- ** Transaction Helpers

seedMockData :: Trie.Tx Trie.ReadWrite ()
seedMockData = do
  Trie.insert mockRootKey mockRootValue
  Trie.insert mockKey0 mockValue0
  Trie.insert mockKey1 mockValue1
  Trie.insert mockKey2 mockValue2
  Trie.insert mockKey3 mockValue3
