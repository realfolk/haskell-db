{-# LANGUAGE OverloadedStrings #-}

module Test.Database.Lib.SmartValueSpec
    ( spec
    ) where

import qualified Data.Binary             as Binary
import           Data.ByteString.Lazy    (ByteString)
import           Data.Function           ((&))
import qualified Data.Hashable           as Hashable
import           Database.Lib.SmartValue (SmartValue)
import qualified Database.Lib.SmartValue as SmartValue
import           Test.Hspec
import qualified Test.Lib.Spec.Binary    as BinarySpec
import qualified Test.Lib.Stub.Store     as Store

-- * Main

spec :: Spec
spec = do
  newSpec
  getSpec
  updateSpec
  BinarySpec.create "SmartValue" mockSmartValue0 mockSmartValue0ByteString

-- * Mock Data

mockSmartValue0 :: SmartValue Word
mockSmartValue0 = SmartValue.new 256

mockSmartValue0ByteString :: ByteString
mockSmartValue0ByteString = "\NUL\NUL\NUL\NUL\NUL\NUL\SOH\NUL\NUL\NUL\NUL\NUL\NUL\NUL\SOH\NUL"

mockSmartValue1 :: SmartValue Word
mockSmartValue1 = SmartValue.new 257

-- * Specs

newSpec :: Spec
newSpec =
  describe "new" $
    context "when creating a smart value" $ do
      it "correctly stores the provided value" $
        SmartValue.value mockSmartValue0 `shouldBe` 256
      it "correctly stores the hash of the provided value" $
        SmartValue.hash mockSmartValue0 `shouldBe` Hashable.hash (256 :: Word)

getSpec :: Spec
getSpec =
  describe "get" $
    before Store.empty $ do
      context "when the value exists" $ do
        context "and is a validly-encoded SmartValue" $
          it "succeeds by returning the SmartValue" $ \connection -> do
            (Right _) <- Store.readWrite connection $ Store.put 0 $ Binary.encode mockSmartValue0
            getSmartValue Store.smartValueInterface 0
              & Store.readOnly connection
              & shouldReturn
              $ Right mockSmartValue0
        context "but is not a validly-encoded SmartValue" $
          it "fails by throwing an error" $ \connection -> do
            (Right _) <- Store.readWrite connection $ Store.put 0 "this is invalid"
            getSmartValue Store.smartValueInterface 0
              & Store.readOnly connection
              & shouldReturn
              $ Left Store.InvalidValue
      context "when the underlying store interface throws an error" $
        it "fails by throwing the same error" $ \connection ->
          -- Get a smart value that doesn't exist in the store.
          getSmartValue Store.smartValueInterface 0
            & Store.readOnly connection
            & shouldReturn
            $ Left Store.KeyDoesNotExist

updateSpec :: Spec
updateSpec =
  describe "update" $
    before Store.empty $ do
      context "when a SmartValue already exists at the provided key" $
        context "and the hashes are different" $
          it "replaces the value in the store" $ \connection -> do
            (Right _) <- Store.readWrite connection $ Store.put 0 $ Binary.encode mockSmartValue0
            SmartValue.value mockSmartValue1
              & updateSmartValue Store.smartValueInterface 0
              & Store.readWrite connection
              & shouldReturn
              $ Right mockSmartValue1
      context "when a SmartValue does not exist at the provided key" $
        it "succeeds by writing the SmartValue to the store" $ \connection -> do
          SmartValue.value mockSmartValue0
            & updateSmartValue Store.smartValueInterface 0
            & Store.readWrite connection
            & shouldReturn
            $ Right mockSmartValue0
      context "when a non-SmartValue already exists at the provided key" $
        it "fails by throwing an error" $ \connection -> do
          -- Fails because the existing value is not a validly-encoded SmartValue.
          (Right _) <- Store.readWrite connection $ Store.put 0 "this is invalid"
          SmartValue.value mockSmartValue0
            & updateSmartValue Store.smartValueInterface 0
            & Store.readWrite connection
            & shouldReturn
            $ Left Store.InvalidValue

-- * Helpers

getSmartValue
  :: SmartValue.StoreInterface Store.Key mode Store.TxState Store.Connection
  -> Store.Key
  -> Store.Tx mode (SmartValue Word)
getSmartValue = SmartValue.get

updateSmartValue
  :: SmartValue.StoreInterface Store.Key Store.ReadWrite Store.TxState Store.Connection
  -> Store.Key
  -> Word
  -> Store.Tx Store.ReadWrite (SmartValue Word)
updateSmartValue = SmartValue.update
