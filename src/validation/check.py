"""
Fonctions de validation de données
"""

import logging
import pandas as pd
import duckdb


def check_name_formatting(
    connection: duckdb.DuckDBPyConnection,
<<<<<<< HEAD
=======
    df: pd.DataFrame
>>>>>>> 1d531e751395352829dc81f69fe441249970b417
):

    query = (
        "SELECT COUNT(*) AS n_bad "
<<<<<<< HEAD
        "FROM titanic "
=======
        "FROM df "
>>>>>>> 1d531e751395352829dc81f69fe441249970b417
        "WHERE list_count(string_split(Name, ',')) <> 2"
    )

    bad = connection.sql(query).fetchone()[0]

    if bad == 0:
        logging.info("Test OK: colonne 'Name' se découpe toujours en 2 parties avec ','")
    else:
        logging.warn(
            f"Problème dans la colonne Name: {bad} ne se décomposent pas en 2 parties."
        )


def check_missing_values(
    connection: duckdb.DuckDBPyConnection,
<<<<<<< HEAD
    variable: str = "Survived",
):

    query = f"SELECT COUNT(*) AS n_missing FROM titanic WHERE {variable} IS NULL"
=======
    df: pd.DataFrame,
    variable: str = "Survived",
):

    query = f"SELECT COUNT(*) AS n_missing FROM df WHERE {variable} IS NULL"
>>>>>>> 1d531e751395352829dc81f69fe441249970b417

    n_missing = connection.sql(query).fetchone()[0]

    message_ok = f"Pas de valeur manquante pour la variable {variable}"
    message_warn = f"{n_missing} valeurs manquantes pour la variable {variable}"
    logging.info(message_ok) if n_missing == 0 else logging.warn(message_warn)


def check_data_leakage(
    train_dataset: pd.DataFrame,
    test_dataset: pd.DataFrame,
    variable: str,
):

    if set(train_dataset[variable].dropna().unique()) - set(
        test_dataset[variable].dropna().unique()
    ):
        logging.error(
            f"Problème de data leakage pour la variable {variable}"
        )
    else:
        logging.info(
            f"Pas de problème de data leakage pour la variable {variable}"
        )
