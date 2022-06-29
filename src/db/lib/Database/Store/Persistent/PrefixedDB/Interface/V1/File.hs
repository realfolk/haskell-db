{-# LANGUAGE RecordWildCards #-}

module Database.Store.Persistent.PrefixedDB.Interface.V1.File
    ( FileData
    , FileIndexers (..)
    , FileName
    , deleteFile
    , getFileHashById
    , readFile
    , writeFile
    ) where

import           Control.Monad                        (void, when)
import qualified Control.Monad.Except                 as E
import qualified Data.ByteString                      as BS
import qualified Data.ByteString.Lazy                 as LBS
import qualified Data.List                            as List
import qualified Data.Text                            as T
import           Database.Store.Persistent.PrefixedDB (Error (..), Key,
                                                       MakeIndex, ReadOnly,
                                                       ReadWrite, Tx)
import qualified Database.Store.Persistent.PrefixedDB as DB
import qualified Lib.Base64                           as B64
import qualified Lib.Crypto.Hash                      as Hash
import qualified Lib.Crypto.Hash.SHA1                 as SHA1
import qualified MDRN.Data                            as Data
import qualified MDRN.Data.Decode                     as Decode
import           Prelude                              hiding (readFile,
                                                       writeFile)

type FileName = T.Text

type FileData = BS.ByteString

data FileIndexers
  = FileIndexers
      { _fiNameIndexer    :: !MakeIndex
      , _fiHashIndexer    :: !MakeIndex
      , _fiRefListIndexer :: !MakeIndex
      }

-- KEY SPACE HELPERS

fileNameIndex :: FileIndexers -> T.Text -> T.Text
fileNameIndex FileIndexers {..} name = _fiNameIndexer [name]

fileHashIndex :: FileIndexers -> BS.ByteString -> T.Text
fileHashIndex FileIndexers {..} blob = _fiHashIndexer [sha1 blob]

fileRefListIndex :: FileIndexers -> Key -> T.Text
fileRefListIndex FileIndexers {..} key = _fiRefListIndexer [T.pack (show key)]

-- DOMAIN SPECIFIC FUNCTIONS

writeFile :: FileIndexers -> T.Text -> BS.ByteString -> Tx ReadWrite ()
writeFile fileIndexers fileName fileData = do
  -- 1. Ensure no other files already exists with this name.
  let fnIndex = fileNameIndex fileIndexers fileName
  fileNameUsed <- DB.existsKey fnIndex
  when fileNameUsed $ E.throwError KeyAlreadyExists
  -- 2. Determine the file hash.
  let fhIndex = fileHashIndex fileIndexers fileData
  -- 3. Using the file hash, check to see if the file blob is already stored.
  blobExists <- DB.existsKey fhIndex
  -- 4. If file blob does not exist, create it, using the fileHash as the lookup index. Otherwise, just retrieve the key.
  blobKey <- if blobExists
                then DB.getKey fhIndex
                else DB.createValue fhIndex (Data.byteString fileData)
  -- 5. Add a lookup index for fileName -> blobKey
  DB.putNameForKey fnIndex blobKey
  -- 6. Update the file reference list by adding fileName to list (blobKey -> refList of fileNames)
  updateReferenceList blobKey
    where
      updateReferenceList blobKey = do
        let rlIndex = fileRefListIndex fileIndexers blobKey
        refList <- DB.getValue rlIndex `E.catchError` const (return [])
        let updatedList = fileName : refList
        DB.updateValue rlIndex updatedList `E.catchError`
          const (void $ DB.createValue rlIndex updatedList)

readFile :: FileIndexers -> T.Text -> Tx ReadOnly LBS.ByteString
readFile fileIndexers fileName = do
  result <- Decode.decodeData Decode.byteString <$> DB.getValue (fileNameIndex fileIndexers fileName)
  either (const $ E.throwError KeyDoesNotExist) (return . LBS.fromStrict) result

deleteFile :: FileIndexers -> T.Text -> Tx ReadWrite ()
deleteFile fileIndexers fileName = do
  let fnIndex = fileNameIndex fileIndexers fileName
  blobKey <- DB.getKey fnIndex
  blob <- DB.getValueWithKey blobKey
  let fhIndex = fileHashIndex fileIndexers blob
  let rlIndex = fileRefListIndex fileIndexers blobKey
  refList <- DB.getValue rlIndex
  if length refList <= 1
     then do -- delete ref list since this is only reference, along with name and hash lookups
       DB.deleteValue rlIndex
       DB.deleteValue fnIndex
       DB.deleteName fhIndex
     else do -- remove ref from ref list and persist, and then delete file name lookup only
       let updatedRefList = Data.list (map Data.text (List.delete fileName refList))
       DB.updateValue rlIndex updatedRefList
       DB.deleteName fnIndex

getFileHashById :: FileIndexers -> T.Text -> Tx ReadOnly T.Text
getFileHashById fileIndexers fileId = sha1 <$> DB.getValue (fileNameIndex fileIndexers fileId)

-- MISC. HELPERS

sha1 = B64.encodeTextStrict . Hash.encodeStrict . SHA1.hashStrict
