{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}

module Test.Integration.Server.Module
    ( createStoreModule
    ) where

import           Control.Monad.IO.Class               (liftIO)
import           Data.Bifunctor                       (second)
import qualified Database.Store.Persistent.PrefixedDB as DB
import           MDRN.Data.Encode                     (toData)
import           MDRN.Language.Expr
import qualified MDRN.Language.Expr                   as Expr
import qualified MDRN.Language.Map                    as Map

createStoreModule :: DB.Connection -> Map
createStoreModule db =
  Map.fromList $
    map
      (second F)
      [ ("exists", exists db),
        ("get", get db),
        ("put", put db),
        ("delete", delete db)
      ]

exists :: DB.Connection -> Function
exists db _ _ args =
  case args of
    [P (Text name)] -> do
      result <- liftIO $ DB.readOnly db $ DB.existsValue name
      case result of
        Left err     -> throwEvaluationError (EEDomainError $ toData err)
        Right output -> return $ P $ Bool output
    _ -> throwEvaluationError EEInvalidArguments

get :: DB.Connection -> Function
get db _ _ args =
  case args of
    [P (Text name)] -> do
      result <- liftIO $ DB.readOnly db $ DB.getValue name
      case result of
        Left err     -> throwEvaluationError (EEDomainError $ toData err)
        Right output -> return $ Expr.optimize output
    _ -> throwEvaluationError EEInvalidArguments

put :: DB.Connection -> Function
put db _ _ args =
  case args of
    [P (Text name), value] -> do
      let encodedResult = Expr.tryConvertToData value
      case encodedResult of
        Left _ -> throwEvaluationError (EENotSerializable "Unable to convert expression to data.")
        Right data' -> save name data'
    _ -> throwEvaluationError EEInvalidArguments
  where
    save name data' = do
      result <- liftIO $ DB.readWrite db $ DB.createValue name data'
      case result of
        Left err -> throwEvaluationError (EEDomainError $ toData err)
        Right _  -> return $ P Unit

delete :: DB.Connection -> Function
delete db _ _ args =
  case args of
    [P (Text index)] -> do
      result <- liftIO $ DB.readWrite db $ DB.deleteValue index
      case result of
        Left err -> throwEvaluationError (EEDomainError $ toData err)
        Right _  -> return $ P Unit
    _ -> throwEvaluationError EEInvalidArguments
