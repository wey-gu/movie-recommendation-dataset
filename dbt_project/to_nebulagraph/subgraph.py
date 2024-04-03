import pandas as pd
import to_ngql_ng


def get_data_frame(csv, columns, with_header=True):
    if with_header:
        return pd.read_csv(csv, names=columns, header=0)
    else:
        return pd.read_csv(csv, names=columns, header=0)


def get_nonan_df(df, columns):
    attr = None
    for column in columns:
        if attr is None:
            attr = getattr(df, column).notna()
        else:
            attr = attr & getattr(df, column).notna()

    new_df = df[attr]
    return new_df[columns].drop_duplicates()


def sub_graph(max=300):
    movie_df = get_data_frame("movies.csv", ["movie_id", "name"])
    user_df = get_data_frame("user.csv", ["user_id"])
    people_df = get_data_frame("people.csv", ["people_id", "name", "birth_date"])
    actor_act_movie_df = get_data_frame("acted_by.csv", ["movie_id", "people_id"])
    director_direct_movie_df = get_data_frame("directed_by.csv", ["movie_id", "people_id"])
    genre_df = get_data_frame("genres.csv", ["genre_id", "name"])
    movie_withgenre_genre_df = get_data_frame("with_genre.csv", ["movie_id", "genre_id"])
    user_watch_movie_df = get_data_frame(
        "user_watched_movies.csv", ["user_id", "rate", "name", "movie_id", "genres"]
    )

    sub_movie_df = movie_df.sample(n=max, ignore_index=True)
    sub_movie_df.to_csv("sub_movie.csv", index=False)
    # people
    sub_actor_act_movie_df = pd.merge(sub_movie_df, actor_act_movie_df, on=["movie_id"], how="left")
    _sub_actor_act_movie_df = get_nonan_df(sub_actor_act_movie_df, ["people_id", "movie_id"])
    _sub_actor_act_movie_df.to_csv("sub_actor_act_movie.csv", index=False)
    sub_director_direct_movie_df = pd.merge(
        sub_movie_df, director_direct_movie_df, on=["movie_id"], how="left"
    )
    _sub_director_direct_movie_df = get_nonan_df(
        sub_director_direct_movie_df, ["people_id", "movie_id"]
    )
    _sub_director_direct_movie_df.to_csv("sub_director_direct_movie.csv", index=False)

    sub_actor_df = pd.merge(people_df, _sub_actor_act_movie_df, on=["people_id"], how="right")
    _sub_actor_df = get_nonan_df(sub_actor_df, ["people_id", "name", "birth_date"])
    _sub_actor_df.to_csv("sub_actor.csv", index=False)

    sub_director_df = pd.merge(
        people_df, _sub_director_direct_movie_df, on=["people_id"], how="right"
    )
    _sub_director_df = get_nonan_df(sub_director_df, ["people_id", "name", "birth_date"])
    _sub_director_df.to_csv("sub_director.csv", index=False)

    # user
    sub_user_df = pd.merge(sub_movie_df, user_watch_movie_df, on=["movie_id"], how="left")
    sub_user_df = pd.merge(sub_user_df, user_df, on=["user_id"], how="left")
    sub_user_df = sub_user_df[sub_user_df["user_id"].notna()]
    _sub_user_df = get_nonan_df(sub_user_df, ["user_id"])
    _sub_user_df.to_csv("sub_user.csv", index=False)
    _sub_user_watch_df = get_nonan_df(sub_user_df, ["user_id", "rate", "movie_id"])
    _sub_user_watch_df.to_csv("sub_user_watched_movies.csv", index=False)

    # gener
    sub_movie_withgenre_genre_df = pd.merge(
        sub_movie_df, movie_withgenre_genre_df, on=["movie_id"], how="left"
    )
    sub_gener_df = pd.merge(sub_movie_withgenre_genre_df, genre_df, on=["genre_id"], how="left")
    _sub_gener_df = get_nonan_df(sub_gener_df, ["genre_id", "name_y"])
    _sub_gener_df.to_csv("sub_gener.csv", index=False)
    _sub_movie_withgenre_genre_df = get_nonan_df(
        sub_movie_withgenre_genre_df, ["movie_id", "genre_id"]
    )
    _sub_movie_withgenre_genre_df.to_csv("sub_movie_withgenre_genre.csv", index=False)


def main():
    sub_graph()
    to_ngql_ng.to_ngql("sub_movie.ngql")


main()
