module Test.Lib.Spec.Hashable
    ( create
    ) where

import           Data.Hashable (Hashable)
import qualified Data.Hashable as Hashable
import           Test.Hspec

create :: (Hashable a, Eq a, Show a) => String -> a -> Int -> Int -> Spec
create typeName value salt hash =
  describe (typeName <> " Hashable instance") $ do
    context "hashWithSalt" $
      it "returns the correct value" $
        Hashable.hashWithSalt salt value `shouldBe` hash
