{-# LANGUAGE OverloadedStrings #-}

module Main
    where

import qualified Control.Concurrent.Async       as Async
import           Control.Exception              (bracket)
import           Control.Monad                  (foldM, unless)
import qualified Control.Monad.Except           as Except
import           Criterion.Main                 (Benchmark, bench, bgroup,
                                                 defaultMain, nf, nfIO)
import           Data.ByteString                (ByteString)
import           Data.Text                      (Text)
import qualified Data.Text                      as Text
import qualified Database.Lib.Tx                as Tx
import qualified Database.Store.Persistent.Trie as Trie
import qualified Pouch.Base64                     as Base64
import qualified Pouch.Crypto.Hash                as Hash
import qualified Pouch.Crypto.Hash.SHA1           as Hash
import qualified Pouch.Crypto.Random              as Random
import           System.IO.Temp                 (withSystemTempDirectory)

-- * Constants

syncInterval = 20000
numKeyBytes = 64
numValueBytes = 128
seedDBTx = 50
seedDBOps = 50

-- * Main

main = withSeededDB seedDBTx seedDBOps runBenchmarks

runBenchmarks seededDB =
  defaultMain
    [ makeBenchmark "insert" insert [10, 50] [10, 50]
    , makeBenchmark "get" (get seededDB) [10, 50] [10, 50]
    ]

makeBenchmark:: String -> (Word -> Word -> IO Bool) -> [Word] -> [Word] -> Benchmark
makeBenchmark name action numTxList numOpsList =
  bgroup name $ map txBenchmark numTxList
  where
    txBenchmark numTx =
      bgroup (show numTx <> "tx") $ map (opsBenchmark numTx) numOpsList
    opsBenchmark numTx numOps =
      bench (show numOps <> "op") $ nfIO $ action numTx numOps

-- * Benchmarks

insert :: Word -> Word -> IO Bool
insert numTx numOps =
  withSeededDB numTx numOps (const (return True))

get :: Trie.Connection -> Word -> Word -> IO Bool
get db numTx numOps = do
  makeDBAction "get" Trie.readOnly singleTxOp numTx numOps db
  return True
  where
    singleTxOp k v = do
      v' <- Trie.get k `Except.catchError` \e -> do
        printKeyValuePair "Get Failure: " k v
        Except.throwError e
      unless (v == v') $ do
        printKeyValuePair2 k v v'
        Except.throwError $ Trie.Failure "Get Failure: values don't match"

-- * Helpers

-- ** Database Connection Helpers

withEmptyDB :: (Trie.Connection -> IO a) -> IO a
withEmptyDB action =
  withSystemTempDirectory "persistent-trie" $ \dir -> do
    bracket (Trie.connect dir syncInterval) Trie.disconnect action

withSeededDB :: Word -> Word -> (Trie.Connection -> IO a) -> IO a
withSeededDB numTx numOps action =
  withEmptyDB $ \db -> seedDB numTx numOps db *> action db

seedDB :: Word -> Word -> Trie.Connection -> IO ()
seedDB = makeDBAction "seedDB" Trie.readWrite singleTxOp
  where
    singleTxOp k v = Trie.insert k v `Except.catchError` \e -> do
      printKeyValuePair "Insert Failure: " k v
      Except.throwError e

makeDBAction :: Text -> Tx.RunTransaction mode Trie.TxState Trie.Connection () -> (Trie.Key -> Trie.Value -> Trie.Tx mode ()) -> Word -> Word -> Trie.Connection -> IO ()
makeDBAction name runTx singleTxOp numTx numOps db = do
  putStrLn $ "Start " <> Text.unpack name <> ", numTx=" <> show numTx <> ", numOps=" <> show numOps
  let keyValuePairs = generateKeyValuePairs numTx numOps
  Async.mapConcurrently (execTx db) keyValuePairs
  putStrLn $ "Complete " <> Text.unpack name <> ", numTx=" <> show numTx <> ", numOps=" <> show numOps
  return ()
  where
    execTx db opKeyValuePairs = do
      outcome <- runTx db $ createTx opKeyValuePairs
      case outcome of
        Left e           -> do
          Except.liftIO $ print e
          fail $ Text.unpack name <> " Failed"
        Right _ -> return ()
    createTx opKeyValuePairs = foldM (\_ (key, value) -> singleTxOp key value) () opKeyValuePairs

-- ** Random Helpers

generateKeyValuePairs :: Word -> Word -> [[(Trie.Key, Trie.Value)]]
generateKeyValuePairs numTx numOps =
  snd $ loop numTx startingGenerator []
    where
      startingGenerator = Random.newTestGenerator (0, 0, 0, 0, 0)
      loop remainingTxs generator acc =
        case remainingTxs of
          0 -> (generator, acc)
          _ -> let (newGenerator, ops) = loopOps numOps generator []
                in loop (remainingTxs - 1) newGenerator (ops : acc)
      loopOps remainingOps generator acc =
        case remainingOps of
          0 -> (generator, acc)
          _ -> let (key, value, newGenerator) = generateKeyValuePair generator
                in loopOps (remainingOps - 1) newGenerator ((key, value) : acc)

generateKeyValuePair :: Random.Generator -> (Trie.Key, Trie.Value, Random.Generator)
generateKeyValuePair generator = (key, value, generator'')
  where
    (key, generator') = Random.generateStrict numKeyBytes generator
    (value, generator'') = Random.generateStrict numValueBytes generator'


-- ** Misc. Helpers

printKeyValuePair :: Text -> Trie.Key -> Trie.Value -> Trie.Tx mode ()
printKeyValuePair prefix k v = Except.liftIO $ print $ Trie.Failure $ prefix <> Text.pack (hash k) <> ": " <> Text.pack (hash v)

printKeyValuePair2 :: Trie.Key -> Trie.Value -> Trie.Value -> Trie.Tx mode ()
printKeyValuePair2 k v v' = Except.liftIO $ print $ Trie.Failure $ "KVFAIL: " <> Text.pack (hash k) <> ": " <> Text.pack (hash v) <> ": " <> Text.pack (hash v')

hash :: ByteString -> String
hash = show . Base64.encodeStrict . Hash.encodeStrict . Hash.hashStrict
