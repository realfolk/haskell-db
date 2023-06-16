{-# LANGUAGE OverloadedStrings #-}

module Test.Database.Store.Persistent.LMDB.BaseSpec
    ( spec
    ) where

import           Control.Concurrent                  (getNumCapabilities)
import           Control.Exception                   (bracket)
import qualified Database.Store.Persistent.LMDB.Base as DB
import           System.IO.Temp                      (withSystemTempDirectory)
import           Test.Hspec

spec :: Spec
spec = around (withDB "base") $ do
  existsSpec
  getSpec
  putSpec
  deleteSpec

existsSpec :: SpecC
existsSpec =
  describe "exists" $ do
    context "when the key exists" $ do
      it "returns True" $ \db -> do
        result <- DB.readWrite db $ do
          DB.put "foo" "bar"
          DB.exists "foo"
        result `shouldBe` Right True

    context "when the key does not exist" $ do
      it "returns False" $ \db -> do
        DB.readWrite db (DB.exists "foo") `shouldReturn` Right False

getSpec :: SpecC
getSpec =
  describe "get" $ do
    context "when the key exists" $ do
      it "returns the associated value" $ \db -> do
        result <- DB.readWrite db $ do
          DB.put "foo" "bar"
          DB.get "foo"
        result `shouldBe` Right "bar"

    context "when the key does not exist" $ do
      it "fails with KeyDoesNotExist" $ \db -> do
        DB.readWrite db (DB.get "foo") `shouldReturn` Left DB.KeyDoesNotExist

putSpec :: SpecC
putSpec =
  describe "put" $ do
    context "with a key that has already been set" $ do
      context "in the same transaction" $ do
        it "fails with KeyAlreadyExists" $ \db -> do
          result <- DB.readWrite db $ do
            DB.put "foo" "bar"
            DB.put "foo" "baz"
          result `shouldBe` Left DB.KeyAlreadyExists

      context "in a separate transaction" $ do
        it "fails with KeyAlreadyExists" $ \db -> do
          DB.readWrite db $ DB.put "foo" "bar"
          DB.readWrite db (DB.put "foo" "baz") `shouldReturn` Left DB.KeyAlreadyExists

    context "with multiple key/value insertions" $ do
      context "when each key is read in a separate transaction" $ do
        it "returns the associated values" $ \db -> do
          result <- DB.readWrite db $ do
            DB.put "foo" "bar"
            DB.put "baz" "bang"
            DB.put "lorem" "ipsum"
          result `shouldBe` Right ()

          DB.readOnly db (DB.get "foo") `shouldReturn` Right "bar"
          DB.readOnly db (DB.get "baz") `shouldReturn` Right "bang"
          DB.readOnly db (DB.get "lorem") `shouldReturn` Right "ipsum"

    context "when the transaction is aborted" $ do
      it "does not persist the value" $ \db -> do
        result <- DB.readWrite db $ do
          DB.put "foo" "bar"
          DB.get "baz" -- This will cause the transaction to be aborted
        result `shouldBe` Left DB.KeyDoesNotExist

        DB.readOnly db (DB.get "foo") `shouldReturn` Left DB.KeyDoesNotExist

deleteSpec :: SpecC
deleteSpec =
  describe "delete" $ do
    context "when the key exists" $ do
      it "deletes the key" $ \db -> do
        result1 <- DB.readWrite db $ do
          DB.put "foo" "bar"
          DB.exists "foo"
        result1 `shouldBe` Right True

        result2 <- DB.readWrite db $ do
          DB.delete "foo"
          DB.exists "foo"
        result2 `shouldBe` Right False

    context "when the key does not exist" $ do
      it "fails with KeyDoesNotExist" $ \db -> do
        DB.readWrite db (DB.delete "foo") `shouldReturn` Left DB.KeyDoesNotExist

-- HELPERS

type SpecC = SpecWith (Arg (DB.Connection -> IO ()))

withDB :: String -> (DB.Connection -> IO ()) -> IO ()
withDB name action =
  withSystemTempDirectory name $ \dir -> do
    maxReaders <- getNumCapabilities
    bracket (DB.connect dir maxReaders) DB.disconnect action
