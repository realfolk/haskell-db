{-# LANGUAGE OverloadedStrings #-}

module Test.Integration.ServerSpec
    ( spec
    ) where

import           Control.Exception                    (bracket)
import           Data.Either                          (fromRight)
import           Data.Text                            as T
import qualified Database.Store.Persistent.PrefixedDB as DB
import           MDRN.Data                            (bool, byteString, unit)
import           MDRN.Data.Parser                     (parseData)
import           MDRN.Language.Expr                   (Data, Expr (..), Map)
import qualified MDRN.Language.Map                    as Map
import qualified MDRN.Lib.ByteString                  as BS
import qualified MDRN.Network.Server.Error            as SE
import qualified MDRN.Network.Server.Handler          as Handler
import           System.IO.Temp                       (withSystemTempDirectory)
import           Test.Hspec
import           Test.Integration.Server.Module       (createStoreModule)

spec :: Spec
spec = around (withDB "server") $ do
  describe "exists" $ do
    context "when the key exists" $ do
      it "returns True" $ \db -> do
        sendRequest db putFooBar
        sendRequest db existsFoo `shouldReturn` Right (bool True)

  describe "get" $ do
    context "when the key exists" $ do
      it "returns the associated value" $ \db -> do
        sendRequest db putFooBar
        sendRequest db getFoo `shouldReturn` Right (byteString "bar")

  describe "put" $ do
    context "when the key does not exist" $ do
      it "inserts the key with the associated value" $ \db -> do
        sendRequest db putFooBar `shouldReturn` Right unit

  describe "delete" $ do
    context "when the key exists" $ do
      it "deletes the key" $ \db -> do
        sendRequest db putFooBar
        sendRequest db deleteFoo
        sendRequest db existsFoo `shouldReturn` Right (bool False)

-- REQUESTS

existsFoo :: Data
existsFoo = makeData "(store.exists \"foo\")"

getFoo :: Data
getFoo = makeData "(store.get \"foo\")"

putFooBar :: Data
putFooBar = makeData $ "(store.put \"foo\" " <> BS.toHex "bar" <> ")"

deleteFoo :: Data
deleteFoo = makeData "(store.delete \"foo\")"

-- HELPERS

withDB :: String -> (DB.Connection -> IO ()) -> IO ()
withDB name action =
  withSystemTempDirectory name $ \dir ->
    bracket (DB.connect dir) DB.disconnect action

sendRequest :: DB.Connection -> Data -> IO (Either SE.Error Data)
sendRequest = Handler.handleRequest . appLib

appLib :: DB.Connection -> Map
appLib db = Map.fromList [("store", M $ createStoreModule db)]

makeData :: T.Text -> Data
makeData = fromRight unit . parseData
