# convert csv to nebula-ng queries.
# it would combine multiple queries into a single file, and then
# could execute via ngql console.
# TODO:
# 1. change the schema label, Actor, Director, `User` should have label Person, not Person&Actor, Person&Director

import csv

schema = '''
"""
CREATE GRAPH TYPE movie_type AS {
Node Actor (:Person {id INT PRIMARY KEY, name STRING, birthDate Date}),
Node Director (:Person {id INT PRIMARY KEY, name STRING, birthDate Date}),
Node `User` (:Person {id INT PRIMARY KEY}),
Node Movie (:Movie {id INT PRIMARY KEY, name STRING}),
Node Genre (:Genre {id INT PRIMARY KEY, name STRING}),
Edge Act (Actor)-[:Act]->(Movie),
Edge Direct (Director)-[:Direct]->(Movie),
Edge Watch (`User`)-[:Watch {rate float}]->(Movie),
Edge WithGenre (Movie)-[:WithGenre]->(Genre)
}
"""
CREATE GRAPH movie TYPED movie_type

'''
prefix_node = "USE movie INSERT OR REPLACE "
prefix_edge = "USE movie "

actor_act_movie_template = (
    "table t{{user_id, movie_id}}={} "
    "use movie for r in t match (a@`Actor`) where a.id=r.user_id "
    "match (b@`Movie`) where b.id=r.movie_id insert or replace (a)-[@Act]->(b)"
)

director_direct_movie_template = (
    "table t{{user_id, movie_id}}={} "
    "use movie for r in t match (a@`Director`) where a.id=r.user_id "
    "match (b@`Movie`) where b.id=r.movie_id insert or replace (a)-[@Direct]->(b)"
)

user_watched_movies_template = (
    "table t{{user_id, rate, movie_id}}={} "
    "use movie for r in t match (a@`User`) where a.id=r.user_id "
    "match (b@`Movie`) where b.id=r.movie_id insert or replace (a)-[@Watch{{rate: r.rate}}]->(b)"
)

movie_withgenre_genre_template = (
    "table t{{movie_id, genre_id}}={} "
    "use movie for r in t match (a@`Movie`) where a.id=r.movie_id "
    "match (b@`Genre`) where b.id=r.genre_id insert or replace (a)-[@WithGenre]->(b)"
)


def to_ngql(output):
    data = []
    data.append(schema)
    data.extend(convert_node_to_ngql("sub_movie.csv", prefix_node, row_fn_movie))
    data.extend(convert_node_to_ngql("sub_actor.csv", prefix_node, row_fn_actor))
    data.extend(convert_node_to_ngql("sub_director.csv", prefix_node, row_fn_director))
    data.extend(convert_node_to_ngql("sub_user.csv", prefix_node, row_fn_user))
    data.extend(convert_node_to_ngql("sub_genre.csv", prefix_node, row_fn_genre))
    data.extend(
        convert_egde_to_ngql(
            "sub_actor_act_movie.csv",
            actor_act_movie_template,
            row_fn_act_movie,
        )
    )
    data.extend(
        convert_egde_to_ngql(
            "sub_director_direct_movie.csv",
            director_direct_movie_template,
            row_fn_direct_movie,
        )
    )
    data.extend(
        convert_egde_to_ngql(
            "sub_user_watched_movies.csv",
            user_watched_movies_template,
            row_fn_watch_movie,
        )
    )
    data.extend(
        convert_egde_to_ngql(
            "sub_movie_withgenre_genre.csv",
            movie_withgenre_genre_template,
            row_fn_withgenre,
        )
    )
    with open(output, "w") as output_ngql:
        content = "\n".join(data)
        output_ngql.write(content)


# USE movie INSERT OR IGNORE (@Actor{id:1,name:"player_1"), (@Actor{id:2,name:"player_1"),
def convert_node_to_ngql(
    input_file, prefix, row_fn, batch_size=256, ignore_header=True
):
    with open(input_file, "r") as input_csv:
        ignored = False
        csv_reader = csv.reader(input_csv)
        batch_cursor = 0
        batch = []
        queries = []
        for row in csv_reader:
            if ignore_header and not ignored:
                ignored = True
                continue
            batch_cursor += 1
            batch.append(row_fn(row))
            if batch_cursor >= batch_size:
                queries.append(prefix + ",".join(batch))
                batch_cursor = 0
                batch = []
        if batch_cursor > 0:
            queries.append(prefix + ",".join(batch))
        return queries


# table t{user_id, movie_id, rate}=
# (305, 11, 5.0),
# (304, 11, 3.5)
# use movie
# for r in t
# return r.user_id as user_id, r.movie_id as movie_id, r.rate as rate
# next
# match (a@`User`) where a.id=user_id
# match (b@`Movie`) where b.id=movie_id
# insert or replace (a)-[@Watch{rate: rate}]->(b)
def convert_egde_to_ngql(
    input_file, template, row_fn, batch_size=256, ignore_header=True
):
    with open(input_file, "r") as input_csv:
        csv_reader = csv.reader(input_csv)
        batch_cursor = 0
        batch = []
        queries = []
        ignored = False
        for row in csv_reader:
            if ignore_header and not ignored:
                ignored = True
                continue
            batch_cursor += 1
            batch.append(row_fn(row))
            if batch_cursor >= batch_size:
                q = template.format(",".join(batch))
                queries.append(q)
                batch_cursor = 0
                batch = []
        if batch_cursor > 0:
            q = template.format(",".join(batch))
            queries.append(q)
        return queries


def row_fn_movie(row):
    return f'(@Movie{{id: {row[0]}, name: "{row[1]}"}})'


def row_fn_actor(row):
    # birthDate maybe \N sometimes
    if row[2] == "\\N":
        return f'(@Actor{{id: {row[0][2:]}, name: "{row[1]}", birthDate: null}})'
    else:
        return f'(@Actor{{id: {row[0][2:]}, name: "{row[1]}", birthDate: Date("{row[2]}")}})'


def row_fn_director(row):
    if row[2] == "\\N":
        return f'(@Director{{id: {row[0][2:]}, name: "{row[1]}", birthDate: null}})'
    else:
        return f'(@Director{{id: {row[0][2:]}, name: "{row[1]}", birthDate: Date("{row[2]}")}})'


def row_fn_user(row):
    return f"(@`User`{{id: {row[0][2:]}}})"


def row_fn_genre(row):
    return f'(@Genre{{id: {row[0][2:]}, name: "{row[1]}"}})'


def row_fn_act_movie(row):
    return f'({row[0][2:]}, {row[1]})'


def row_fn_direct_movie(row):
    return f'({row[0][2:]}, {row[1]})'


def row_fn_watch_movie(row):
    return f'({row[0][2:]}, {row[1]}, {row[2]})'


def row_fn_withgenre(row):
    return f'({row[0]}, {row[1][2:]})'
