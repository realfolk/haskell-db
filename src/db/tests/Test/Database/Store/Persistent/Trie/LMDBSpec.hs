{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TupleSections     #-}

module Test.Database.Store.Persistent.Trie.LMDBSpec
    ( spec
    ) where

import           Control.Exception                            (bracket)
import qualified Control.Monad.State                          as State
import qualified Data.Binary                                  as Binary
import           Data.ByteString                              (ByteString)
import qualified Data.ByteString                              as ByteString
import qualified Data.ByteString.Lazy                         as LazyByteString
import           Data.Function                                ((&))
import qualified Data.Map.Strict                              as Map
import           Data.Maybe                                   (fromMaybe)
import           Database.Lib.IDManager                       (IDManager)
import qualified Database.Lib.IDManager                       as IDManager
import qualified Database.Store.Persistent.LMDB.Base          as LMDBBase
import qualified Database.Store.Persistent.Trie.LMDB          as LMDB
import qualified Database.Store.Persistent.Trie.LMDB.Internal as LMDBInternal
import qualified Lib.Binary                                   as Binary
import           System.IO.Temp                               (withSystemTempDirectory)
import           Test.Hspec
import qualified Test.Lib.Spec.Binary                         as BinarySpec
import qualified Test.Lib.Spec.Enum                           as EnumSpec
import qualified Test.Lib.Spec.Hashable                       as HashableSpec

-- * Constants

tempDirectoryName = "persistent-trie-lmdb"

-- * Main

spec :: Spec
spec = do
  connectSpec
  getChildNodeIDSpec
  around withEmptyDB $ do
    createSpec
    putSpec
  around withSeededDB $ do
    getSpec
    deleteSpec
    replaceSpec
  BinarySpec.create "Node" mockNode mockNodeByteString
  BinarySpec.create "Data" mockData mockDataByteString
  BinarySpec.create "ID (IDManager (ID value))" mockIDManagerID mockIDManagerIDByteString
  BinarySpec.create "ID Node" mockNodeID mockNodeIDByteString
  BinarySpec.create "ID Data" mockDataID mockDataIDByteString
  EnumSpec.create "ID (IDManager (ID value))" mockIDManagerID (LMDB.IDManagerID 6) (LMDB.IDManagerID 4 :: LMDB.ID (IDManager (LMDB.ID LMDB.Node)))
  EnumSpec.create "ID Node" mockNodeID (LMDB.NodeID 6) (LMDB.NodeID 4)
  EnumSpec.create "ID Data" mockDataID (LMDB.DataID 6) (LMDB.DataID 4)
  HashableSpec.create "ID (IDManager (ID value))" mockIDManagerID mockSalt mockIDManagerIDHash
  HashableSpec.create "ID Node" mockNodeID mockSalt mockNodeIDHash
  HashableSpec.create "ID Data" mockDataID mockSalt mockDataIDHash

-- * Mock Data

mockSalt = 999

mockNodeID :: LMDB.ID LMDB.Node
mockNodeID = LMDB.NodeID 5

mockNodeIDByteString :: LazyByteString.ByteString
mockNodeIDByteString = "\SOH\NUL\NUL\NUL\NUL\NUL\NUL\NUL\ENQ"

mockNodeIDHash :: Int
mockNodeIDHash = 281207010793074217

mockDataID :: LMDB.ID LMDB.Data
mockDataID = LMDB.DataID 5

mockDataIDByteString :: LazyByteString.ByteString
mockDataIDByteString = "\STX\NUL\NUL\NUL\NUL\NUL\NUL\NUL\ENQ"

mockDataIDHash :: Int
mockDataIDHash = 281207010843407072

mockNode :: LMDB.Node
mockNode = LMDB.Node (Map.singleton 5 mockNodeID) (Just mockDataID)

mockNodeByteString :: LazyByteString.ByteString
mockNodeByteString = "\NUL\NUL\NUL\NUL\NUL\NUL\NUL\NUL\SOH\ENQ\SOH\NUL\NUL\NUL\NUL\NUL\NUL\NUL\ENQ\SOH\STX\NUL\NUL\NUL\NUL\NUL\NUL\NUL\ENQ"

mockData :: LMDB.Data
mockData = LMDB.Data $ ByteString.pack [0, 1, 2, 3]

mockDataByteString :: LazyByteString.ByteString
mockDataByteString = "\NUL\NUL\NUL\NUL\NUL\NUL\NUL\NUL\EOT\NUL\SOH\STX\ETX"

mockIDManagerID :: LMDB.ID (IDManager (LMDB.ID value))
mockIDManagerID = LMDB.IDManagerID 5

mockIDManagerIDByteString :: LazyByteString.ByteString
mockIDManagerIDByteString = "\NUL\NUL\NUL\NUL\NUL\NUL\NUL\NUL\ENQ"

mockIDManagerIDHash :: Int
mockIDManagerIDHash = 281207010809851834

-- * Specs

connectSpec :: Spec
connectSpec =
  describe "connect" $ do
    context "when the database is empty" $
      around withEmptyDB $ do
        it "creates an empty root node" $ \db -> do
          rootNode <- LMDB.readOnly db $ LMDB.get LMDB.rootNodeID
          rootNode `shouldBe` Right (LMDB.Node Map.empty Nothing)
        it "uses the fallback IDManagers" $ \db -> do
          Right (nodeIDManager, dataIDManager) <- LMDB.readOnly db getIDManagers
          nodeIDManager `shouldBe` IDManager.empty LMDBInternal.defaultNodeIDStart
          dataIDManager `shouldBe` IDManager.empty LMDBInternal.defaultDataIDStart
    context "when the database is not empty" $
      around (withSystemTempDirectory tempDirectoryName) $ do
        it "uses the existing root node" $ \dir -> do
          conn0 <- LMDB.connect dir
          Right SeedData{..} <- LMDB.readWrite conn0 seedMockData
          LMDB.disconnect conn0
          conn1 <- LMDB.connect dir
          Right rootNode <- LMDB.readOnly conn1 $ LMDB.get LMDB.rootNodeID
          LMDB.disconnect conn1
          _sdRootNode `shouldBe` rootNode
        it "uses the existing IDManagers" $ \dir -> do
          conn0 <- LMDB.connect dir
          Right _ <- LMDB.readWrite conn0 seedMockData
          LMDB.disconnect conn0
          conn1 <- LMDB.connect dir
          Right (nodeIDManager, dataIDManager) <- LMDB.readOnly conn1 getIDManagers
          LMDB.disconnect conn1
          let expectedNodeIDManager = IDManager.empty LMDBInternal.defaultNodeIDStart & IDManager.next & snd
          let expectedDataIDManager = IDManager.empty LMDBInternal.defaultDataIDStart & IDManager.next & snd
          nodeIDManager `shouldBe` expectedNodeIDManager
          dataIDManager `shouldBe` expectedDataIDManager

getChildNodeIDSpec :: Spec
getChildNodeIDSpec =
  describe "getChildNodeID" $ do
    context "when the node has children" $ do
      context "and the queried child exists" $
        it "returns the child node ID successfully" $ do
          let childNodeID = LMDB.NodeID 0
          let node = LMDB.Node (Map.singleton 0 childNodeID) Nothing
          LMDB.getChildNodeID 0 node `shouldBe` Just childNodeID
      context "and the queried child does not exist" $
        it "returns Nothing" $ do
          let childNodeID = LMDB.NodeID 0
          let node = LMDB.Node (Map.singleton 0 childNodeID) Nothing
          LMDB.getChildNodeID 1 node `shouldBe` Nothing
    context "when the node has no children" $
      it "always returns Nothing" $ do
        let node = LMDB.Node Map.empty Nothing
        LMDB.getChildNodeID minBound node `shouldBe` Nothing
        LMDB.getChildNodeID 1 node `shouldBe` Nothing
        LMDB.getChildNodeID 123 node `shouldBe` Nothing
        LMDB.getChildNodeID maxBound node `shouldBe` Nothing

createSpec :: SpecWithEmptyDB
createSpec =
  describe "create" $ do
    it "returns the correct IDs" $ \db -> do
      let node = LMDB.Node Map.empty Nothing
      let data' = LMDB.Data ByteString.empty
      Right (nodeIDManager, dataIDManager) <- LMDB.readOnly db getIDManagers
      Right (nodeID, dataID) <- (,) <$> LMDB.create node <*> LMDB.create data' & LMDB.readWrite db
      nodeID `shouldBe` fst (IDManager.next nodeIDManager)
      dataID `shouldBe` fst (IDManager.next dataIDManager)
    it "persists the new values correctly" $ \db -> do
      let node = LMDB.Node Map.empty Nothing
      let data' = LMDB.Data ByteString.empty
      Right (nodeID, dataID) <- (,) <$> LMDB.create node <*> LMDB.create data' & LMDB.readWrite db
      Right (expectedNode, expectedData) <- (,) <$> LMDB.get nodeID <*> LMDB.get dataID & LMDB.readOnly db
      expectedNode `shouldBe` node
      expectedData `shouldBe` data'
    it "updates the internal IDManagers correctly" $ \db -> do
      let node = LMDB.Node Map.empty Nothing
      let data' = LMDB.Data ByteString.empty
      Right (nodeIDManager0, dataIDManager0) <- LMDB.readOnly db getIDManagers
      Right _ <- LMDB.readWrite db $ LMDB.create node >> LMDB.create data'
      Right (nodeIDManager1, dataIDManager1) <- LMDB.readOnly db getIDManagers
      nodeIDManager1 `shouldBe` snd (IDManager.next nodeIDManager0)
      dataIDManager1 `shouldBe` snd (IDManager.next dataIDManager0)

putSpec :: SpecWithEmptyDB
putSpec =
  describe "put" $ do
    context "when a value does not already exist" $ do
      it "persists the value correctly" $ \db -> do
        let node = LMDB.Node Map.empty Nothing
        let nodeID = LMDB.NodeID maxBound
        outcome <- LMDB.readWrite db $ LMDBInternal.put nodeID node
        outcome `shouldBe` Right ()
      context "and the ID has been previously recycled" $
        it "unrecycles the ID" $ \db -> do
          let node = LMDB.Node Map.empty Nothing
          let nodeID = LMDB.NodeID maxBound
          Right (nodeIDManager0, _) <- LMDB.readOnly db getIDManagers
          LMDB.readWrite db $ do
            nodeIDManager <- LMDBInternal.getIDManager
            LMDBInternal.setIDManager $ fromMaybe nodeIDManager (IDManager.recycle nodeID nodeIDManager)
          Right _ <- LMDB.readWrite db $ LMDBInternal.put nodeID node
          Right (nodeIDManager1, _) <- LMDB.readOnly db getIDManagers
          nodeIDManager0 `shouldBe` nodeIDManager1
    context "when a value already exists" $ do
      it "throws the correct error" $ \db -> do
        let node = LMDB.Node Map.empty Nothing
        let nodeID = LMDB.NodeID maxBound
        Right _ <- LMDB.readWrite db $ LMDBInternal.put nodeID node
        outcome <- LMDB.readWrite db $ LMDBInternal.put nodeID node
        outcome `shouldBe` Left LMDB.KeyAlreadyExists

getSpec :: SpecWithSeededDB
getSpec =
  describe "get" $ do
    context "when the value exists" $ do
      context "and it can be decoded" $
        it "returns the correct value" $ \(db, SeedData {..}) -> do
          Right (node, data') <- (,) <$> LMDB.get _sdNode0ID <*> LMDB.get _sdData0ID & LMDB.readOnly db
          node `shouldBe` _sdNode0
          data' `shouldBe` _sdData0
      context "and it cannot be decoded" $
        it "throws the correct error" $ \(db@LMDBInternal.Connection {..}, SeedData {..}) -> do
          let nodeID = LMDB.NodeID maxBound
          Right _ <- LMDBBase.readWrite _connLMDBConnection $
            LMDBBase.put (Binary.encodeStrict nodeID) $ ByteString.pack [0, 1, 2, 3]
          outcome <- LMDB.readOnly db $ LMDB.get nodeID
          outcome `shouldBe` Left LMDB.InvalidValue
    context "when the value does not exist" $
      it "throws the correct error" $ \(db, SeedData {..}) -> do
        outcome <- LMDB.readOnly db $ LMDB.get $ LMDB.NodeID maxBound
        outcome `shouldBe` Left LMDB.KeyDoesNotExist

deleteSpec :: SpecWithSeededDB
deleteSpec =
  describe "delete" $ do
    context "when the value exists" $ do
      it "deletes the value successfully" $ \(db, SeedData {..}) -> do
        Right _ <- LMDB.readWrite db $ LMDB.delete _sdNode0ID >> LMDB.delete _sdData0ID
        nodeOutcome <- LMDB.readOnly db $ LMDB.get _sdNode0ID
        dataOutcome <- LMDB.readOnly db $ LMDB.get _sdData0ID
        nodeOutcome `shouldBe` Left LMDB.KeyDoesNotExist
        dataOutcome `shouldBe` Left LMDB.KeyDoesNotExist
      it "updates the IDManager correctly" $ \(db, SeedData {..}) -> do
        Right (nodeIDManager0, dataIDManager0) <- LMDB.readOnly db getIDManagers
        Right _ <- LMDB.readWrite db $ LMDB.delete _sdNode0ID >> LMDB.delete _sdData0ID
        Right (nodeIDManager1, dataIDManager1) <- LMDB.readOnly db getIDManagers
        Just nodeIDManager1 `shouldBe` IDManager.recycle _sdNode0ID nodeIDManager0
        Just dataIDManager1 `shouldBe` IDManager.recycle _sdData0ID dataIDManager0
    context "when the value does not exist" $
      it "throws the correct error" $ \(db, SeedData {..}) -> do
        outcome <- LMDB.readWrite db $ LMDB.delete $ LMDB.NodeID maxBound
        outcome `shouldBe` Left LMDB.KeyDoesNotExist

replaceSpec :: SpecWithSeededDB
replaceSpec =
  describe "replace" $ do
    context "when the value exists" $ do
      it "replaces the value successfully" $ \(db, seedData@SeedData {..}) -> do
        Right _ <- replace db seedData
        Right (node, data') <- (,) <$> LMDB.get _sdNode0ID <*> LMDB.get _sdData0ID & LMDB.readOnly db
        node `shouldBe` newNode
        data' `shouldBe` newData
      it "does not update the IDManager" $ \(db, seedData) -> do
        Right (nodeIDManager0, dataIDManager0) <- LMDB.readOnly db getIDManagers
        Right _ <- replace db seedData
        Right (nodeIDManager1, dataIDManager1) <- LMDB.readOnly db getIDManagers
        nodeIDManager0 `shouldBe` nodeIDManager1
        dataIDManager0 `shouldBe` dataIDManager1
    context "when the value does not exist" $
      it "throws the correct error" $ \(db, SeedData {..}) -> do
        outcome <- LMDB.readWrite db $ LMDB.replace (LMDB.NodeID maxBound) newNode
        outcome `shouldBe` Left LMDB.KeyDoesNotExist
    where
      newNode = LMDB.Node Map.empty Nothing
      newData = LMDB.Data $ ByteString.pack [0, 1, 2]
      replace db SeedData {..} = LMDB.readWrite db $ do
        LMDB.replace _sdNode0ID newNode
        LMDB.replace _sdData0ID newData

-- * Helpers

type SpecWithEmptyDB = SpecWith (Arg (LMDB.Connection -> IO ()))

withEmptyDB :: (LMDB.Connection -> IO a) -> IO a
withEmptyDB action =
  withSystemTempDirectory tempDirectoryName $ \dir -> do
    bracket (LMDB.connect dir) LMDB.disconnect action

type SpecWithSeededDB = SpecWith (Arg ((LMDB.Connection, SeedData) -> IO ()))

withSeededDB :: ((LMDB.Connection, SeedData) -> IO a) -> IO a
withSeededDB action = withEmptyDB $ \db -> do
  Right seedData <- LMDB.readWrite db seedMockData
  action (db, seedData)

-- ** Transaction Helpers

data SeedData
  = SeedData
      { _sdRootNode :: LMDB.Node
      , _sdNode0ID  :: LMDB.ID LMDB.Node
      , _sdNode0    :: LMDB.Node
      , _sdData0ID  :: LMDB.ID LMDB.Data
      , _sdData0    :: LMDB.Data
      }

seedMockData :: LMDB.Tx LMDB.ReadWrite SeedData
seedMockData = do
  let _sdData0 = LMDB.Data ByteString.empty
  _sdData0ID <- LMDB.create _sdData0
  let _sdNode0 = LMDB.Node Map.empty (Just _sdData0ID)
  _sdNode0ID <- LMDB.create _sdNode0
  let _sdRootNode = LMDB.Node (Map.singleton 0 _sdNode0ID) Nothing
  LMDB.replace LMDB.rootNodeID _sdRootNode
  return $ SeedData {..}

getIDManagers :: LMDB.Tx mode (IDManager (LMDB.ID LMDB.Node), IDManager (LMDB.ID LMDB.Data))
getIDManagers = do
  LMDBInternal.TxState {..} <- State.get
  return (_txNodeIDManager, _txDataIDManager)
