# convert csv to nebula-ng queries.
# it would combine multiple queries into a single file, and then
# could execute via ngql console.
# TODO:
# 1. change the schema label, Actor, Director, User should have label Person, not Person&Actor, Person&Director

import csv

schema = '''
"""
CREATE GRAPH TYPE movie_type AS {
(Actor LABELS Actor&Person {id INT PRIMARY KEY, name STRING, birthDate Date}),
(Director LABELS Director&Person {id INT PRIMARY KEY, name STRING, birthDate Date}),
(User LABELS Person {id INT PRIMARY KEY}),
(Movie LABELS Movie {id INT PRIMARY KEY, name STRING}),
(Gener LABELS Gener {id INT PRIMARY KEY, name STRING}),
(Actor)-[Act:Acrt]->(Movie),
(Director)-[Direct:Direct]->(Movie),
(User)-[Watch:Watch {rate float}]->(Movie),
(Movie)-[WithGener:WithGener]->(Gener)
}
"""

:sleep 10

CREATE GRAPH movie TYPED movie_type

:sleep 10
'''
prefix = "USE movie INSERT OR REPLACE "

def to_ngql(output):
    data = []
    data.append(schema)
    data.extend(convert_node_to_ngql("sub_movie.csv", prefix, row_fn_movie))
    data.extend(convert_node_to_ngql("sub_actor.csv", prefix, row_fn_actor))
    data.extend(
        convert_node_to_ngql("sub_director.csv", prefix, row_fn_director)
    )
    data.extend(convert_node_to_ngql("sub_user.csv", prefix, row_fn_user))
    data.extend(convert_node_to_ngql("sub_gener.csv",prefix, row_fn_gener))
    data.extend(
        convert_egde_to_ngql(
            "sub_actor_act_movie.csv",prefix, row_fn_act_movie
        )
    )
    data.extend(
        convert_egde_to_ngql(
            "sub_director_direct_movie.csv",
           prefix,
            row_fn_direct_movie,
        )
    )
    data.extend(
        convert_egde_to_ngql(
            "sub_user_watched_movies.csv",
            prefix,
            row_fn_watch_movie,
        )
    )
    data.extend(
        convert_egde_to_ngql(
            "sub_movie_withgenre_genre.csv",
            prefix,
            row_fn_withgener,
        )
    )
    with open(output, "w") as output_ngql:
        content = "\n".join(data)
        output_ngql.write(content)


# USE movie INSERT OR IGNORE (::Actor{id:1,name:"player_1"), (::Actor{id:2,name:"player_1"), 
def convert_node_to_ngql(input_file, prefix, row_fn, batch_size=256, ignore_header=True):
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


# USE ldbc INSERT OR REPLACE (::Act{id:1}-[{})->{id:2}]), (::Act{id:3}-[{})->{id:4}]),
def convert_egde_to_ngql(input_file, prefix, row_fn, batch_size=256, ignore_header=True):
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
                queries.append(prefix + ",".join(batch))
                batch_cursor = 0
                batch = []
        if batch_cursor > 0:
            queries.append(prefix + ",".join(batch))
        return queries


def row_fn_movie(row):
    return f'(::Movie{{id: {row[0]}, name: "{row[1]}"}})'


# remove prefix from id
def row_fn_actor(row):
    # birthDate maybe \N sometimes
    if row[2] == "\\N":
      return f'(::Actor{{id: {row[0][2:]}, name: "{row[1]}", birthDate: null}})'
    else:
      return f'(::Actor{{id: {row[0][2:]}, name: "{row[1]}", birthDate: Date("{row[2]}")}})'


def row_fn_director(row):
    if row[2] == "\\N":
      return f'(::Director{{id: {row[0][2:]}, name: "{row[1]}", birthDate: null}})'
    else:
      return f'(::Director{{id: {row[0][2:]}, name: "{row[1]}", birthDate: Date("{row[2]}")}})'


def row_fn_user(row):
    return f"(::User{{id: {row[0][2:]}}})"


def row_fn_gener(row):
    return f'(::Gener{{id: {row[0][2:]}, name: "{row[1]}"}})'


def row_fn_act_movie(row):
    return f"(::Act{{id: {row[0][2:]}}})-[{{}}]->({{id: {row[1]}}})"


def row_fn_direct_movie(row):
    return f"(::Direct{{id: {row[0][2:]}}})-[{{}}]->({{id: {row[1]}}})"


def row_fn_watch_movie(row):
    return f"(::Watch{{id: {row[0][2:]}}})-[{{rate: {row[1]}}}]->({{id: {row[2]}}})"


def row_fn_withgener(row):
    return f"(::WithGener{{id: {row[0]}}})-[{{}}]->({{id: {row[1][2:]}}})"
