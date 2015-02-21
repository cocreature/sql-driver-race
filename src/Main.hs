{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}

module Main where

import           Control.Monad
import           Criterion
import           Criterion.Main
import           Data.ByteString            (ByteString)
import           Data.UUID
import qualified Database.PostgreSQL.Simple as S
import qualified Hasql                      as H
import qualified Hasql.Postgres             as HP
import           System.Random

type Location = (UUID, Double, Double)

main :: IO ()
main = do
        hps   <- maybe (fail "Improper session settings") return $ H.poolSettings 6 30
        hconn <- H.acquirePool (HP.StringSettings connStr) hps :: IO (H.Pool HP.Postgres)
        _ <- H.session hconn $
            H.tx Nothing $
                H.unitEx [H.stmt| CREATE TABLE IF NOT EXISTS hasql_location (
                                    id          uuid                NOT NULL,
                                    x           double precision    NOT NULL,
                                    y           double precision    NOT NULL
                                  )
                                |]

        pconn <- S.connectPostgreSQL connStr
        _     <- S.execute_ pconn "CREATE TABLE IF NOT EXISTS simple_location (id uuid NOT NULL, x double precision NOT NULL, y double precision NOT NULL)"

        defaultMain
            [ env (genData 100) $ \locs ->
                bgroup "Insert 100"
                    [ bench "hasql"  $ whnfIO $ insertHasql locs hconn
                    , bench "simple" $ whnfIO $ insertSimple locs pconn
                    ]
            , env (genData 1000) $ \locs ->
                bgroup "Insert 1000"
                    [ bench "hasql"  $ whnfIO $ insertHasql locs hconn
                    , bench "simple" $ whnfIO $ insertSimple locs pconn
                    ]
            ]

insertHasql :: [Location] -> H.Pool HP.Postgres -> IO ()
insertHasql locs hconn = do
        _ <- H.session hconn $
            H.tx Nothing $ mapM_ (H.unitEx . insLoc) locs
        H.releasePool hconn
    where insLoc (i, x, y) = [H.stmt|INSERT INTO hasql_location (id, x, y) VALUES ($i, $x, $y)|]

insertSimple :: [Location] -> S.Connection -> IO ()
insertSimple locs pconn = void (S.executeMany pconn query locs)
    where query = "INSERT INTO simple_location VALUES (?, ?, ?)"

connStr :: ByteString
connStr = "host=192.168.59.103 port=5432 user=postgres dbname=sql_driver_race"

genData :: Int -> IO [Location]
genData i = do
        gen <- newStdGen
        let locs = zip3 (randoms gen) [1..] [1..]
        return $ take i locs
