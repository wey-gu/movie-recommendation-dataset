This is a movie Knowledge Graph Dataset ready for NebulaGraph.

The dataset comes from [OMDB](https://www.omdb.org/en/us/content/Help:DataDownload) for cast and movie genre and [MovieLens](https://grouplens.org/datasets/movielens/) for real world user-movie interaction records.

<img width="1106" alt="modeling_omdb_movielens" src="https://user-images.githubusercontent.com/1651790/201309981-eacd07c3-01ad-46ba-b4d3-ba66a21b952a.png">

## How to use this Dataset

Refer to https://www.siwei.io/recommendation-system-with-graphdb/

## Scehma of the Dataset

The data aggregates [OMDB](https://www.omdb.org/en/us/content/Help:DataDownload) and [MovieLens](https://grouplens.org/datasets/movielens/) to become a knowledge graph with schema:

- Tag:
  - user(user_id)
  - movie(name)
  - person(name, birthdate)
  - genre(name)

- Edge Type:
  - watched(rate(double))
  - with_genre
  - directed_by
  - acted_by

<img width="843" alt="schema_0" src="https://user-images.githubusercontent.com/1651790/200805712-296d97bb-7871-443d-abb6-c28900de5970.png">

## Mapping between two Tabular datasources and NebulaGraph property graph model

<img width="897" alt="schema_mapping_to_graph" src="https://user-images.githubusercontent.com/1651790/201310261-85c40a50-5baa-494c-8970-3187a870d1d9.png">

## ELT Process

- Raw Data wangling
- Data Loading into Data Warehouse(Postgress)
- Transform Data into the form ready for Property Graph Model(dbt), export as CSV
- Load the CSV files into NebulaGraph(Nebula-Importer)

![ETL_dbt_nebulagraph_importer](https://user-images.githubusercontent.com/1651790/201317930-c4d0149d-6add-4a9b-a353-84fe73004625.png)


## dbt env preparation

Install dbt and dbt-postgres plugin.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install dbt-postgres

dbt init dbt_project
cd dbt_project
```

Run a postgres container with the following command:

```bash
docker run --rm --name postgres \
    -e POSTGRES_PASSWORD=nebula \
    -e POSTGRES_USER=nebula \
    -e POSTGRES_DB=warehouse -d \
    -p 5432:5432 postgres
```

## Raw Data

### Download

Download files into dbt_project/raw_data

```bash
mkdir -p dbt_project/raw_data
cd dbt_project/raw_data
wget www.omdb.org/data/all_people.csv.bz2
wget www.omdb.org/data/all_people_aliases.csv.bz2
wget www.omdb.org/data/people_links.csv.bz2
wget www.omdb.org/data/all_casts.csv.bz2
wget www.omdb.org/data/job_names.csv.bz2
wget www.omdb.org/data/all_characters.csv.bz2
wget www.omdb.org/data/movie_categories.csv.bz2
wget www.omdb.org/data/movie_keywords.csv.bz2
wget www.omdb.org/data/category_names.csv.bz2
wget www.omdb.org/data/all_categories.csv.bz2
wget www.omdb.org/data/all_movie_aliases_iso.csv.bz2
bunzip2 *.bz2
wget https://files.grouplens.org/datasets/movielens/ml-latest-small.zip
unzip ml-latest-small.zip
rm *.zip
```

### Data Wrangling

Remove `\"` from the data and copy to seeds.

```bash
grep -v '\\"' raw_data/all_movie_aliases_iso.csv > seeds/all_movie_aliases_iso.csv
grep -v '\\"' raw_data/all_casts.csv > seeds/all_casts.csv
grep -v '\\"' raw_data/all_characters.csv > seeds/all_characters.csv
grep -v '\\"' raw_data/all_people.csv > seeds/all_people.csv
grep -v '\\"' raw_data/category_names.csv > seeds/category_names.csv
grep -v '\\"' raw_data/job_names.csv > seeds/job_names.csv
cp raw_data/movie_categories.csv seeds/movie_categories.csv
cp raw_data/movie_keywords.csv seeds/movie_keywords.csv
cp raw_data/all_categories.csv seeds/all_categories.csv
cp raw_data/ml-latest-small/ratings.csv seeds/movielens_ratings.csv
cp raw_data/ml-latest-small/movies.csv seeds/movielens_movies.csv
```

Add raw CSV files into data warehouse for further processing.

```bash
❯ cd dbt_project
❯ dbt seed
05:58:27  Running with dbt=1.3.0
05:58:27  Found 2 models, 4 tests, 0 snapshots, 0 analyses, 289 macros, 0 operations, 11 seed files, 0 sources, 0 exposures, 0 metrics
05:58:28  
05:58:28  Concurrency: 8 threads (target='dev')
05:58:28  
05:58:28  1 of 11 START seed file public.all_casts ....................................... [RUN]
...
07:10:11  1 of 11 OK loaded seed file public.all_casts ................................... [INSERT 1082228 in 4303.78s]
07:10:11  
07:10:11  Finished running 11 seeds in 1 hours 11 minutes and 43.93 seconds (4303.93s).
07:10:11  
07:10:11  Completed successfully
07:10:11  
07:10:11  Done. PASS=11 WARN=0 ERROR=0 SKIP=0 TOTAL=11
```

## Transform Data with dbt

Process data for NebulaGraph Import.

The transformation magic was done via the [model files](https://github.com/wey-gu/movie-recommendation-dataset/tree/main/dbt_project/models/movie_recommedation) in `dbt_project/models`.

```bash
dbt run -m acted_by
dbt run -m user_watched_movies
dbt run -m directed_by
dbt run -m with_genre
dbt run -m people
dbt run -m genres
dbt run -m movies
```

Export data as CSV files

```postgresql
COPY acted_by TO '/tmp/acted_by.csv'  WITH DELIMITER ',' CSV HEADER;
COPY directed_by TO '/tmp/directed_by.csv'  WITH DELIMITER ',' CSV HEADER;
COPY with_genre TO '/tmp/with_genre.csv'  WITH DELIMITER ',' CSV HEADER;
COPY people TO '/tmp/people.csv'  WITH DELIMITER ',' CSV HEADER;
COPY movies TO '/tmp/movies.csv'  WITH DELIMITER ',' CSV HEADER;
COPY genres TO '/tmp/genres.csv'  WITH DELIMITER ',' CSV HEADER;
# for user_watched_movies we dont export header
COPY user_watched_movies TO '/tmp/user_watched_movies.csv'  WITH DELIMITER ',' CSV;
```

```bash
mkdir -p to_nebulagraph
docker cp postgres:/tmp/. to_nebulagraph/
```

## Import Data into NebulaGraph

### Create Schema

Run following DDL to create schema.

```ngql
CREATE SPACE moviegraph(partition_num=10,replica_factor=1,vid_type=fixed_string(32));
# sleep for 20 seconds
USE moviegraph;
CREATE TAG person(name string, birthdate string);
CREATE TAG movie(name string);
CREATE TAG genre(name string);
CREATE TAG user(user_id string);
CREATE EDGE acted_by();
CREATE EDGE directed_by();
CREATE EDGE with_genre();
CREATE EDGE watched(rate float);
```

### Import Data

We use [Nebula Importer](https://github.com/vesoft-inc/nebula-importer) to import data into NebulaGraph.

The configuration file that tells Nebula Importer how CSV should be loaded into NebulaGraph is [here](https://github.com/wey-gu/movie-recommendation-dataset/blob/main/nebula-importer.yaml).

```bash
cd ..

docker run --rm -ti \
    --network=nebula-net \
    -v ${PWD}:/root/ \
    -v ${PWD}/dbt_project/to_nebulagraph/:/data \
    vesoft/nebula-importer:v3.2.0 \
    --config /root/nebula-importer.yaml
```

Verify data stats:

```ngql
USE moviegraph;
SHOW STATS;
```
It should look like this:

```
(root@nebula) [moviegraph]> SHOW STATS;
+---------+---------------+---------+
| Type    | Name          | Count   |
+---------+---------------+---------+
| "Tag"   | "genre"       | 14397   |
| "Tag"   | "movie"       | 20701   |
| "Tag"   | "person"      | 263907  |
| "Tag"   | "user"        | 610     |
| "Edge"  | "acted_by"    | 673763  |
| "Edge"  | "directed_by" | 101949  |
| "Edge"  | "watched"     | 31781   |
| "Edge"  | "with_genre"  | 194009  |
| "Space" | "vertices"    | 299615  |
| "Space" | "edges"       | 1001502 |
+---------+---------------+---------+
Got 10 rows (time spent 1693/15136 us)
```
