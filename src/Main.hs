{-# LANGUAGE OverloadedStrings #-}

module Main where

import           Control.Monad
import           Criterion
import           Criterion.Main
import           Data.ByteString (ByteString)
import           Data.Functor.Contravariant
import           Data.List
import           Data.Monoid
import           Data.UUID
import qualified Database.PostgreSQL.Simple as S
import qualified Hasql.Connection as Hasql
import qualified Hasql.Decoders as Decoders
import qualified Hasql.Encoders as Encoders
import qualified Hasql.Query as Hasql
import qualified Hasql.Session as Hasql
import           System.Random


type Location = (UUID, Double, Double)

main :: IO ()
main = do
  Right hconn <- Hasql.acquire connStr
  _ <-
    flip Hasql.run hconn $
    Hasql.sql
      "CREATE TABLE IF NOT EXISTS hasql_location (id uuid NOT NULL, x double precision NOT NULL, y double precision NOT NULL)"
  pconn <- S.connectPostgreSQL connStr
  _ <-
    S.execute_
      pconn
      "CREATE TABLE IF NOT EXISTS simple_location (id uuid NOT NULL, x double precision NOT NULL, y double precision NOT NULL)"
  defaultMain
    [ env (genData 100) $ \locs ->
        bgroup
          "Insert 100"
          [ bench "hasql" $ whnfIO $ insertHasql locs hconn
          , bench "hasql'" $ whnfIO $ insertHasql' locs hconn
          , bench "simple" $ whnfIO $ insertSimple locs pconn
          ]
    , env (genData 1000) $ \locs ->
        bgroup
          "Insert 1000"
          [ bench "hasql" $ whnfIO $ insertHasql locs hconn
          , bench "hasql'" $ whnfIO $ insertHasql' locs hconn
          , bench "simple" $ whnfIO $ insertSimple locs pconn
          ]
    ]

insertHasql :: [Location] -> Hasql.Connection -> IO ()
insertHasql locs hconn = do
  mapM_
    (\loc ->
       void $
       flip
         Hasql.run
         hconn
         (Hasql.query
            loc
            (Hasql.statement
               "INSERT INTO hasql_location VALUES ($1, $2, $3)"
               (contramap (\(a, _, _) -> a) (Encoders.value Encoders.uuid) <>
                contramap (\(_, b, _) -> b) (Encoders.value Encoders.float8) <>
                contramap (\(_, _, c) -> c) (Encoders.value Encoders.float8))
               Decoders.unit
               True)))
    locs

insertHasql' :: [Location] -> Hasql.Connection -> IO ()
insertHasql' locs hconn = do
  void $
    flip
      Hasql.run
      hconn
      (Hasql.query
         (unzip3 locs)
         (Hasql.statement
            "insert into hasql_location (select A.val,B.val,C.val from \
               \  (select *, row_number() over () from unnest($1) as val) as A\
               \  inner join\
               \    (select *, row_number() over () from unnest($2) as val) as B\
               \    on A.row_number = B.row_number\
               \  inner join\
               \    (select *, row_number() over () from unnest($3) as val) as C\
               \    ON B.row_number = C.row_number\
               \)"
            (contramap (\(a, _, _) -> a) (Encoders.value (array' Encoders.uuid)) <>
             contramap
               (\(_, b, _) -> b)
               (Encoders.value (array' Encoders.float8)) <>
             contramap
               (\(_, _, c) -> c)
               (Encoders.value (array' Encoders.float8)))
            Decoders.unit
            True))
  where
    array' el =
      Encoders.array (Encoders.arrayDimension foldl' (Encoders.arrayValue el))

insertSimple :: [Location] -> S.Connection -> IO ()
insertSimple locs pconn = void (S.executeMany pconn query locs)
    where query = "INSERT INTO simple_location VALUES (?, ?, ?)"

connStr :: ByteString
connStr = "host=localhost user=postgres dbname=sql_driver_race"

genData :: Int -> IO [Location]
genData i = do
        gen <- newStdGen
        let locs = zip3 (randoms gen) [1..] [1..]
        return $ take i locs
