
===Docker: PostgreSQL ===

sudo docker build -t sparqlmap/postgres - < postgresql.dock

sudo docker run sparqlmap/postgres -name r2rmltest_postgres -p 5432:5432
