{-# LANGUAGE OverloadedStrings #-}

module Test.Database.Store.Persistent.PrefixedDBSpec
    ( spec
    ) where

import           Control.Exception                    (bracket)
import qualified Data.Set                             as Set
import qualified Data.Text                            as T
import qualified Database.Store.Persistent.PrefixedDB as DB
import           System.IO.Temp                       (withSystemTempDirectory)
import           Test.Hspec

spec :: Spec
spec = around (withDB "base") $ do
  existsKeySpec
  existsValueSpec
  getKeysStartWithSpec

existsKeySpec :: SpecC
existsKeySpec =
  describe "existsKey" $ do
    context "when the name is associated with a key" $ do
      it "returns True" $ \db -> do
        DB.readWrite db $ DB.createValue "foo" value
        DB.readOnly db (DB.existsKey "foo") `shouldReturn` Right True

    context "when the name is not associated with a key" $ do
      it "returns False" $ \db -> do
        DB.readOnly db (DB.existsKey "foo") `shouldReturn` Right False

existsValueSpec :: SpecC
existsValueSpec =
  describe "existsValue" $ do
    context "when a value is associated with the name" $ do
      it "returns True" $ \db -> do
        DB.readWrite db $ DB.createValue "foo" value
        DB.readOnly db (DB.existsValue "foo") `shouldReturn` Right True

    context "when a value is not associated with the name" $ do
        it "returns False" $ \db -> do
          DB.readWrite db $ do
            key <- DB.createValue "foo" value
            DB.putNameForKey "lorem" key
            DB.deleteValue "lorem"
          DB.readOnly db (DB.existsValue "foo") `shouldReturn` Right False

getKeysStartWithSpec :: SpecC
getKeysStartWithSpec =
  describe "getKeysStartWith" $ do
    it "returns all the keys that begin with the given prefix" $ \db -> do
      Right [key1, key2, key3] <- DB.readWrite db $ do
        key1 <- DB.createValue "foo" value
        key2 <- DB.createValue "bar" value
        key3 <- DB.createValue "baz" value
        DB.putPrefixForKey "abc" key1
        DB.putPrefixForKey "xyz" key1
        DB.putPrefixForKey "abc" key2
        DB.putPrefixForKey "xyz" key3
        return [key1, key2, key3]

      Right result <- DB.readOnly db (DB.getKeysStartWith "abc")
      Set.fromList result `shouldBe` Set.fromList [key1, key2]

      Right result <- DB.readOnly db (DB.getKeysStartWith "xyz")
      Set.fromList result `shouldBe` Set.fromList [key1, key3]

      Right result <- DB.readOnly db (DB.getKeysStartWith "foo")
      Set.fromList result `shouldSatisfy` Set.null

-- CONSTANTS

value :: T.Text
value = "value"

-- HELPERS

type SpecC = SpecWith (Arg (DB.Connection -> IO ()))

withDB :: String -> (DB.Connection -> IO ()) -> IO ()
withDB name action =
  withSystemTempDirectory name $ \dir ->
    bracket (DB.connect dir) DB.disconnect action
