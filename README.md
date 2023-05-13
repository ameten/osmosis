You need to build image for indexer from the repository directory

```shell
docker build -f indexer/Dockerfile -t indexer:latest .
```

You can start indexer and postgres database using `docker-compose` from the repository directory
```shell
docker-compose up -d
```

You can connect to postgres database and see that indexer adds the mapping from proposer to the height of the block
which was proposed by the proposer. Password: osmosis.
```shell
psql -h localhost -p 5432 -U osmosis
```

Use this query to query the database
```
select * from proposer_to_height;
```

The API is not finished and is not functional. You can find source code in `statistics` package.
