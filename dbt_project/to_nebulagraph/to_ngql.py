import csv

BATCH_SIZE = 256


def convert_node_records_to_ngql(
    input_file,
    output_file,
    prefix="INSERT VERTEX `movie`(name) VALUES ",
    indexes=[0, 1],
):
    with open(input_file, "r") as input_csv, open(output_file, "w") as output_ngql:
        csv_reader = csv.reader(input_csv)

        batch_cursor = 0
        batch = []
        for row in csv_reader:
            batch_cursor += 1
            batch.append(row)
            if batch_cursor >= BATCH_SIZE:
                output_ngql.write(generate_vertex_ngql(batch, prefix, indexes))
                batch_cursor = 0
                batch = []

        # write the last batch
        if batch_cursor > 0:
            output_ngql.write(generate_vertex_ngql(batch, prefix, indexes))


def generate_vertex_ngql(batch, prefix, indexes):
    values = []
    for row in batch:
        values.append(
            f'"{row[indexes[0]]}":'
            + '("'
            + '","'.join(row[i] for i in indexes[1:])
            + '")'
        )
    return f'{prefix} {", ".join(values)};\n'


def convert_edge_records_to_ngql(
    input_file, output_file, prefix="INSERT EDGE `acted_by`() VALUES ", indexes=[0, 1]
):
    with open(input_file, "r") as input_csv, open(output_file, "w") as output_ngql:
        csv_reader = csv.reader(input_csv)

        batch_cursor = 0
        batch = []
        for row in csv_reader:
            batch_cursor += 1
            batch.append(row)
            if batch_cursor >= BATCH_SIZE:
                output_ngql.write(generate_edge_ngql(batch, prefix, indexes))
                batch_cursor = 0
                batch = []

        # write the last batch
        if batch_cursor > 0:
            output_ngql.write(generate_edge_ngql(batch, prefix, indexes))


def generate_edge_ngql(batch, prefix, indexes):
    values = []
    for row in batch:
        values.append(
            f'"{row[indexes[0]]}"->"{row[indexes[1]]}":'
            + "("
            + ",".join(row[i] for i in indexes[2:])
            + ")"
        )
    return f'{prefix} {", ".join(values)};\n'


convert_node_records_to_ngql(
    "movies.csv",
    "movies.ngql",
    prefix="INSERT VERTEX `movie`(name) VALUES ",
    indexes=[0, 1],
)
convert_node_records_to_ngql(
    "genres.csv",
    "genres.ngql",
    prefix="INSERT VERTEX `genre`(name) VALUES ",
    indexes=[0, 1],
)
convert_node_records_to_ngql(
    "people.csv",
    "people.ngql",
    prefix="INSERT VERTEX `person`(name, birthdate) VALUES ",
    indexes=[0, 1, 2],
)

convert_node_records_to_ngql(
    "user_watched_movies.csv",
    "user.ngql",
    prefix="INSERT VERTEX `user`() VALUES ",
    indexes=[0],
)

convert_edge_records_to_ngql(
    "acted_by.csv",
    "acted_by.ngql",
    prefix="INSERT EDGE `acted_by`() VALUES ",
    indexes=[0, 1],
)
convert_edge_records_to_ngql(
    "directed_by.csv",
    "directed_by.ngql",
    prefix="INSERT EDGE `directed_by`() VALUES ",
    indexes=[0, 1],
)
convert_edge_records_to_ngql(
    "with_genre.csv",
    "with_genre.ngql",
    prefix="INSERT EDGE `with_genre`() VALUES ",
    indexes=[0, 1],
)
convert_edge_records_to_ngql(
    "user_watched_movies.csv",
    "user_watched_movies.ngql",
    prefix="INSERT EDGE `watched`(rate) VALUES ",
    indexes=[0, 3, 1],
)
