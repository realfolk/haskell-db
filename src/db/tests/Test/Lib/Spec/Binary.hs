module Test.Lib.Spec.Binary
    ( create
    ) where

import           Data.Binary          (Binary)
import qualified Data.Binary          as Binary
import           Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Lazy as ByteString
import           Test.Hspec

create :: (Binary a, Eq a, Show a) => String -> a -> ByteString -> Spec
create typeName value byteString =
  describe (typeName <> " Binary instance") $ do
    context "when encoding to a ByteString" $
      it "encodes correctly" $
        Binary.encode value `shouldBe` byteString
    context "when decoding from a ByteString" $
      it "decodes correctly" $
        Binary.decode byteString `shouldBe` value
    context "when encoding and decoding to and from a ByteString" $
      it "returns the same value" $
        Binary.decode (Binary.encode value) `shouldBe` value
