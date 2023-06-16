{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Test.Database.Store.Persistent.Trie.PathSpec
    ( spec
    ) where

import           Control.Exception                   (bracket)
import           Data.ByteString                     (ByteString)
import qualified Data.ByteString                     as ByteString
import           Data.Function                       ((&))
import qualified Data.Map.Strict                     as Map
import           Data.Word                           (Word8)
import qualified Database.Store.Persistent.Trie.LMDB as LMDB
import           Database.Store.Persistent.Trie.Path (Parent (..), Path (..))
import qualified Database.Store.Persistent.Trie.Path as Path
import           System.IO.Temp                      (withSystemTempDirectory)
import           Test.Hspec

-- * Main

spec :: Spec
spec = around (withDB "persistent-trie-lmdb-path") $ do
  isCompleteSpec
  isIncompleteSpec
  resolveSpec
  getNextChildNodeIDSpec

-- * Specs

type SpecWithDB = SpecWith (Arg ((LMDB.Connection, SeedPaths) -> IO ()))

isCompleteSpec :: SpecWithDB
isCompleteSpec =
  describe "isComplete" $ do
    context "when the path is complete" $ do
          it "returns True" $ \(db, SeedPaths {..}) -> do
            path <- LMDB.readOnly db $ Path.resolve LMDB.rootNodeID $ ByteString.pack [0, 1, 2]
            fmap Path.isComplete path `shouldBe` Right True

isIncompleteSpec :: SpecWithDB
isIncompleteSpec =
  describe "isIncomplete" $ do
    context "when the path is complete" $ do
          it "returns True" $ \(db, SeedPaths {..}) -> do
            path <- LMDB.readOnly db $ Path.resolve LMDB.rootNodeID $ ByteString.pack [0, 1]
            fmap Path.isIncomplete path `shouldBe` Right True

resolveSpec :: SpecWithDB
resolveSpec =
  describe "resolve" $ do
    context "when the key path exists with data" $ do
          it "returns the correct, complete path" $ \(db, SeedPaths {..}) -> do
            path <- LMDB.readOnly db $ Path.resolve LMDB.rootNodeID $ ByteString.pack [0, 1, 2]
            path `shouldBe` Right _sp012
    context "when the key path exists without data" $ do
          it "returns the correct, incomplete path" $ \(db, SeedPaths {..}) -> do
            path <- LMDB.readOnly db $ Path.resolve LMDB.rootNodeID $ ByteString.pack [0, 1]
            path `shouldBe` Right (_sp01 ByteString.empty)
    context "when the key path does not exist" $ do
          it "returns the correct, incomplete path" $ \(db, SeedPaths {..}) -> do
            path <- LMDB.readOnly db $ Path.resolve LMDB.rootNodeID $ ByteString.pack [9, 8, 7]
            path `shouldBe` Right (_spRoot (ByteString.pack [9, 8, 7]))

getNextChildNodeIDSpec :: SpecWithDB
getNextChildNodeIDSpec =
  describe "getNextChildNodeID" $ do
    context "when the node has the next child" $
      it "returns the correct child information and remaining key" $ \(db, SeedPaths {..}) -> do
        Right rootNode <- LMDB.readOnly db $ LMDB.get LMDB.rootNodeID
        let (Incomplete _ (Parent expectedNodeID _ _)) = _sp0 ByteString.empty
        Path.getNextChildNodeID (ByteString.pack [0, 1, 2]) rootNode `shouldBe`
          Just (expectedNodeID, 0, ByteString.pack [1, 2])
    context "when the node does not have the next child" $
      it "returns Nothing" $ \(db, SeedPaths {..}) -> do
        Right rootNode <- LMDB.readOnly db $ LMDB.get LMDB.rootNodeID
        Path.getNextChildNodeID (ByteString.pack [9, 8, 7]) rootNode `shouldBe` Nothing
    context "when the supplied key is empty" $
      it "returns Nothing" $ \(db, SeedPaths {..}) -> do
        Right rootNode <- LMDB.readOnly db $ LMDB.get LMDB.rootNodeID
        Path.getNextChildNodeID ByteString.empty rootNode `shouldBe` Nothing

-- * Helpers

withDB :: String -> ((LMDB.Connection, SeedPaths) -> IO ()) -> IO ()
withDB name action =
  withSystemTempDirectory name $ \dir -> do
    bracket (connectAndSeed dir) (LMDB.disconnect . fst) action
      where
        connectAndSeed dir = do
          connection <- LMDB.connect dir
          Right seedPaths <- LMDB.readWrite connection seedMockData
          return (connection, seedPaths)

-- ** Transaction Helpers

data SeedPaths
  = SeedPaths
      { _spRoot :: ByteString -> Path
      , _sp0    :: ByteString -> Path
      , _sp01   :: ByteString -> Path
      , _sp012  :: Path
      , _sp0123 :: Path
      }

seedMockData :: LMDB.Tx LMDB.ReadWrite SeedPaths
seedMockData = do
  dataID0123 <- LMDB.create $ LMDB.Data $ ByteString.pack [0, 1, 2, 3]
  dataID012 <- LMDB.create $ LMDB.Data $ ByteString.pack [0, 1, 2]
  let node3 = LMDB.Node Map.empty (Just dataID0123)
  nodeID3 <- LMDB.create node3
  let node2 = LMDB.Node (Map.singleton 3 nodeID3) (Just dataID012)
  nodeID2 <- LMDB.create node2
  let node1 = LMDB.Node (Map.singleton 2 nodeID2) Nothing
  nodeID1 <- LMDB.create node1
  let node0 = LMDB.Node (Map.singleton 1 nodeID1) Nothing
  nodeID0 <- LMDB.create node0
  oldRootNode@LMDB.Node {..} <- LMDB.get LMDB.rootNodeID
  let newRootNode = oldRootNode { LMDB._nodeChildren = Map.insert 0 nodeID0 _nodeChildren }
  LMDB.replace LMDB.rootNodeID newRootNode
  let parentRoot = Parent LMDB.rootNodeID newRootNode Nothing
  let parent0 = Parent nodeID0 node0 $ Just parentRoot
  let parent1 = Parent nodeID1 node1 $ Just parent0
  let parent2 = Parent nodeID2 node2 $ Just parent1
  let parent3 = Parent nodeID3 node3 $ Just parent2
  return $
    SeedPaths
      { _spRoot = flip Incomplete parentRoot
      , _sp0 = flip Incomplete parent0
      , _sp01 = flip Incomplete parent1
      , _sp012 = Complete dataID012 parent2
      , _sp0123 = Complete dataID0123 parent3
      }
