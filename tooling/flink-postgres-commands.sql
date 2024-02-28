psql -U postgres -d postgres -h localhost -p 5432
\dt

CREATE TABLE postgresTable (
    key INT,
    symbol STRING,
    name STRING
) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:postgresql://postgres:5432/postgres',
   'table-name' = 'quote_data_symbols',
   'username' = 'postgres',
   'password' = 'postgres'
);
