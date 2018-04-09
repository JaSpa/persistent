{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Database.Persist.Sql.Raw where

import Database.Persist
import Database.Persist.Sql.Types
import Database.Persist.Sql.Class
import qualified Data.Map as Map
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Reader (ReaderT, ask, MonadReader)
import Data.Acquire (allocateAcquire, Acquire, mkAcquire, with)
import Data.IORef (writeIORef, readIORef, newIORef)
import Control.Exception (throwIO)
import Control.Monad (when, liftM)
import Data.Text (Text, pack)
import Control.Monad.Logger (logDebugNS, runLoggingT)
import Data.Int (Int64)
import qualified Data.Text as T
import Data.Conduit
import Control.Monad.Trans.Resource (MonadResource,release)

rawQuery :: (MonadResource m, MonadReader env m, HasPersistBackend env, BaseBackend env ~ SqlBackend)
         => Text
         -> [PersistValue]
         -> ConduitM () [PersistValue] m ()
rawQuery sql vals = do
    srcRes <- liftPersist $ rawQueryRes sql vals
    (releaseKey, src) <- allocateAcquire srcRes
    src
    release releaseKey

rawQueryRes
    :: (MonadIO m1, MonadIO m2, IsSqlBackend env)
    => Text
    -> [PersistValue]
    -> ReaderT env m1 (Acquire (ConduitM () [PersistValue] m2 ()))
rawQueryRes sql vals = do
    conn <- persistBackend `liftM` ask
    let make = do
            runLoggingT (logDebugNS (pack "SQL") $ T.append sql $ pack $ "; " ++ show vals)
                (connLogFunc conn)
            getStmtConn conn sql
    return $ do
        stmt <- mkAcquire make stmtReset
        stmtQuery stmt vals

-- | Execute a raw SQL statement
rawExecute :: (MonadIO m, BackendCompatible SqlBackend backend)
           => Text            -- ^ SQL statement, possibly with placeholders.
           -> [PersistValue]  -- ^ Values to fill the placeholders.
           -> ReaderT backend m ()
rawExecute x y = liftM (const ()) $ rawExecuteCount x y

-- | Execute a raw SQL statement and return the number of
-- rows it has modified.
rawExecuteCount :: (MonadIO m, BackendCompatible SqlBackend backend)
                => Text            -- ^ SQL statement, possibly with placeholders.
                -> [PersistValue]  -- ^ Values to fill the placeholders.
                -> ReaderT backend m Int64
rawExecuteCount sql vals = do
    conn <- projectBackend `liftM` ask
    runLoggingT (logDebugNS (pack "SQL") $ T.append sql $ pack $ "; " ++ show vals)
        (connLogFunc conn)
    stmt <- getStmt sql
    res <- liftIO $ stmtExecute stmt vals
    liftIO $ stmtReset stmt
    return res

getStmt
  :: (MonadIO m, BackendCompatible SqlBackend backend)
  => Text -> ReaderT backend m Statement
getStmt sql = do
    conn <- projectBackend `liftM` ask
    liftIO $ getStmtConn conn sql

getStmtConn :: SqlBackend -> Text -> IO Statement
getStmtConn conn sql = do
    smap <- liftIO $ readIORef $ connStmtMap conn
    case Map.lookup sql smap of
        Just stmt -> return stmt
        Nothing -> do
            stmt' <- liftIO $ connPrepare conn sql
            iactive <- liftIO $ newIORef True
            let stmt = Statement
                    { stmtFinalize = do
                        active <- readIORef iactive
                        if active
                            then do
                                stmtFinalize stmt'
                                writeIORef iactive False
                            else return ()
                    , stmtReset = do
                        active <- readIORef iactive
                        when active $ stmtReset stmt'
                    , stmtExecute = \x -> do
                        active <- readIORef iactive
                        if active
                            then stmtExecute stmt' x
                            else throwIO $ StatementAlreadyFinalized sql
                    , stmtQuery = \x -> do
                        active <- liftIO $ readIORef iactive
                        if active
                            then stmtQuery stmt' x
                            else liftIO $ throwIO $ StatementAlreadyFinalized sql
                    }
            liftIO $ writeIORef (connStmtMap conn) $ Map.insert sql stmt smap
            return stmt

-- | Execute a raw SQL statement and return its results as a
-- list.
--
-- If you're using 'Entity'@s@ (which is quite likely), then you
-- /must/ use entity selection placeholders (double question
-- mark, @??@).  These @??@ placeholders are then replaced for
-- the names of the columns that we need for your entities.
-- You'll receive an error if you don't use the placeholders.
-- Please see the 'Entity'@s@ documentation for more details.
--
-- You may put value placeholders (question marks, @?@) in your
-- SQL query.  These placeholders are then replaced by the values
-- you pass on the second parameter, already correctly escaped.
-- You may want to use 'toPersistValue' to help you constructing
-- the placeholder values.
--
-- Since you're giving a raw SQL statement, you don't get any
-- guarantees regarding safety.  If 'rawSql' is not able to parse
-- the results of your query back, then an exception is raised.
-- However, most common problems are mitigated by using the
-- entity selection placeholder @??@, and you shouldn't see any
-- error at all if you're not using 'Single'.
--
-- Some example of 'rawSql' based on this schema:
--
-- @
-- share [mkPersist sqlSettings, mkMigrate "migrateAll"] [persistLowerCase|
-- Person
--     name String
--     age Int Maybe
--     deriving Show
-- BlogPost
--     title String
--     authorId PersonId
--     deriving Show
-- |]
-- @
--
-- Examples based on the above schema:
--
-- @
-- getPerson :: MonadIO m => ReaderT SqlBackend m [Entity Person]
-- getPerson = rawSql "select ?? from person where name=?" [PersistText "john"]
--
-- getAge :: MonadIO m => ReaderT SqlBackend m [Single Int]
-- getAge = rawSql "select person.age from person where name=?" [PersistText "john"]
--
-- getAgeName :: MonadIO m => ReaderT SqlBackend m [(Single Int, Single Text)]
-- getAgeName = rawSql "select person.age, person.name from person where name=?" [PersistText "john"]
--
-- getPersonBlog :: MonadIO m => ReaderT SqlBackend m [(Entity Person, Entity BlogPost)]
-- getPersonBlog = rawSql "select ??,?? from person,blog_post where person.id = blog_post.author_id" []
-- @
--
-- Minimal working program for PostgreSQL backend based on the above concepts:
--
-- > {-# LANGUAGE EmptyDataDecls             #-}
-- > {-# LANGUAGE FlexibleContexts           #-}
-- > {-# LANGUAGE GADTs                      #-}
-- > {-# LANGUAGE GeneralizedNewtypeDeriving #-}
-- > {-# LANGUAGE MultiParamTypeClasses      #-}
-- > {-# LANGUAGE OverloadedStrings          #-}
-- > {-# LANGUAGE QuasiQuotes                #-}
-- > {-# LANGUAGE TemplateHaskell            #-}
-- > {-# LANGUAGE TypeFamilies               #-}
-- >
-- > import           Control.Monad.IO.Class  (liftIO)
-- > import           Control.Monad.Logger    (runStderrLoggingT)
-- > import           Database.Persist
-- > import           Control.Monad.Reader
-- > import           Data.Text
-- > import           Database.Persist.Sql
-- > import           Database.Persist.Postgresql
-- > import           Database.Persist.TH
-- >
-- > share [mkPersist sqlSettings, mkMigrate "migrateAll"] [persistLowerCase|
-- > Person
-- >     name String
-- >     age Int Maybe
-- >     deriving Show
-- > |]
-- >
-- > conn = "host=localhost dbname=new_db user=postgres password=postgres port=5432"
-- >
-- > getPerson :: MonadIO m => ReaderT SqlBackend m [Entity Person]
-- > getPerson = rawSql "select ?? from person where name=?" [PersistText "sibi"]
-- >
-- > liftSqlPersistMPool y x = liftIO (runSqlPersistMPool y x)
-- >
-- > main :: IO ()
-- > main = runStderrLoggingT $ withPostgresqlPool conn 10 $ liftSqlPersistMPool $ do
-- >          runMigration migrateAll
-- >          xs <- getPerson
-- >          liftIO (print xs)
-- >

rawSql :: (RawSql a, MonadIO m)
       => Text             -- ^ SQL statement, possibly with placeholders.
       -> [PersistValue]   -- ^ Values to fill the placeholders.
       -> ReaderT SqlBackend m [a]
rawSql stmt params = do
  srcRes <- liftPersist $ rawSqlSourceRes stmt params
  liftIO $ with srcRes sourceToList

rawSqlSource :: (RawSql a, MonadResource m, MonadReader env m, IsSqlBackend env)
             => Text            -- ^ SQL statement, possibly with placeholders.
             -> [PersistValue]  -- ^ Values to fill the placeholders.
             -> ConduitM () a m ()
rawSqlSource stmt params = do
  srcRes <- liftPersist $ rawSqlSourceRes stmt params
  (releaseKey, src) <- allocateAcquire srcRes
  src
  release releaseKey

rawSqlSourceRes :: forall a m1 m2 env. (RawSql a, MonadIO m1, MonadIO m2, IsSqlBackend env)
                => Text
                -> [PersistValue]
                -> ReaderT env m1 (Acquire (ConduitM () a m2 ()))
rawSqlSourceRes stmt = run
  where
    a :: a
    a = error "rawSql: dummy type annotation"

    run params = do
      conn <- persistBackend `liftM` ask
      let (colCount, colSubsts) = rawSqlCols (connEscapeName conn) a
      srcRes <- liftPersist $ rawQueryRes (stmt' colSubsts) params
      return $ fmap (.| process colCount) srcRes

    process :: Int -> ConduitM [PersistValue] a m2 ()
    process colCount = do
      mrow <- await
      case mrow of
        Nothing -> mempty
        Just row
          | colCount == length row -> process' row >> awaitForever process'
          | otherwise -> fail $ concat
             [ "rawSql: wrong number of columns (got ", show (length row)
             , " but expected ", show colCount, ": ", rawSqlColCountReason a
             , ")."
             ]
          where process' = either (fail . T.unpack) yield . rawSqlProcessRow

    stmt' colSubsts = T.concat $ makeSubsts colSubsts (T.splitOn qs stmt)
      where
        qs = "??"
        makeSubsts (s:ss) (t:tt) = t : s : makeSubsts ss tt
        makeSubsts []     []     = []
        makeSubsts []     tt     = [T.intercalate qs tt]
        makeSubsts ss     []     = error $ concat
          [ "rawSql: there are still ", show (length ss), " '??' placeholder "
          , " substitutions to be made but all there are no more placeholders."
          , " Plase read 'rawSql's documentation on how '??' placeholders work."
          ]
