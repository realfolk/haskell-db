module Test.Lib.Spec.Enum
    ( create
    ) where

import           Test.Hspec

create :: (Enum a, Eq a, Show a) => String -> a -> a -> a -> Spec
create typeName value succValue predValue =
  describe (typeName <> " Enum instance") $ do
    context "succ" $
      it "returns the correct value" $
        succ value `shouldBe` succValue
    context "pred" $
      it "returns the correct value" $
        pred value `shouldBe` predValue
    context "when sequencing succ then pred" $
      it "returns the same value" $
        pred (succ value) `shouldBe` value
    context "when sequencing pred then succ" $
      it "returns the same value" $
        succ (pred value) `shouldBe` value
