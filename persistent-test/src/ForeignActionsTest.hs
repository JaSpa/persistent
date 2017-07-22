{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleInstances #-}
module ForeignActionsTest where

import Init
import UnliftIO

share [mkPersist persistSettings, mkMigrate "foreignActionsMigrate"] [persistLowerCase|
  ForeignActionsParent

  ForeignActionsNoAction
    parent ForeignActionsParentId

  ForeignActionsCascade
    parent ForeignActionsParentId on_delete=CASCADE

  ForeignActionsSet
    parentNull      ForeignActionsParentId Maybe         on_delete=SET_NULL
    parentDefault   ForeignActionsParentId default='123' on_delete=SET_DEFAULT
|]

specs :: Spec
specs = describe "foreign key actions" $ do
  describe "NO ACTION" $ do
    it "disallows deletion" $ db $ do
      p <- insert ForeignActionsParent
      _ <- insert $ ForeignActionsNoAction p
      shouldThrow' $ delete p

  describe "CASCADE" $ do
    it "deletes the children" $ db $ do
      p <- insert ForeignActionsParent
      _ <- insert $ ForeignActionsCascade p
      delete p
      cnt <- count ([] :: [Filter ForeignActionsCascade])
      cnt @== 0

  describe "SET NULL / SET DEFAULT" $ do
    it "changes the children" $ db $ do
      let pDefault    = toSqlKey 123 :: ForeignActionsParentId
          pReferenced = toSqlKey 456 :: ForeignActionsParentId
      insertKey pDefault ForeignActionsParent
      insertKey pReferenced ForeignActionsParent
      childKey <- insert $ ForeignActionsSet (Just pReferenced) pReferenced
      delete pReferenced
      Just (ForeignActionsSet pNullCol pDefCol) <- get childKey
      pNullCol @== Nothing
      pDefCol  @== pDefault

shouldThrow' :: MonadUnliftIO m => m a -> m ()
shouldThrow' action = try action >>=
  \case Right _ -> liftIO $ assertFailure "expected exception not thrown"
        Left e  -> const (return ()) (e :: SomeException)
