"""
Fonctions de validation de données
"""

import logging
import pandas as pd
import duckdb


def check_name_formatting(
    connection: duckdb.DuckDBPyConnection,
):
    """Vérifie que le formatage de la colonne Name est correct.

    Chaque nom doit comporter exactement deux parties séparées par une virgule.
    """

    query = (
        "SELECT COUNT(*) AS n_bad "
        "FROM titanic "
        "WHERE list_count(string_split(Name, ',')) <> 2"
    )

    bad = connection.sql(query).fetchone()[0]

    if bad == 0:
        logging.info("Test OK: colonne 'Name' se découpe toujours en 2 parties avec ','")
    else:
        logging.warning(
            "Problème dans la colonne Name: %s ne se décomposent pas en 2 parties.", bad
        )


def check_missing_values(
    connection: duckdb.DuckDBPyConnection,
    variable: str = "Survived",
):
    """Vérifie l'absence de valeurs manquantes pour une variable donnée."""

    query = f"SELECT COUNT(*) AS n_missing FROM titanic WHERE {variable} IS NULL"

    n_missing = connection.sql(query).fetchone()[0]

    message_ok = f"Pas de valeur manquante pour la variable {variable}"
    message_warn = f"{n_missing} valeurs manquantes pour la variable {variable}"
    logging.info(message_ok) if n_missing == 0 else logging.warn(message_warn)


def check_data_leakage(
    train_dataset: pd.DataFrame,
    test_dataset: pd.DataFrame,
    variable: str,
):
    """Vérifie l'absence de data leakage entre le dataset d'entraînement et de test."""

    if set(train_dataset[variable].dropna().unique()) - set(
        test_dataset[variable].dropna().unique()
    ):
        logging.error(
            "Problème de data leakage pour la variable %s", variable
        )
    else:
        logging.info(
            "Pas de problème de data leakage pour la variable %s", variable
        )
