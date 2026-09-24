from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    dag_id="daily_batch_report",
    schedule="0 3 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
)
def daily_batch_report():

    @task
    def extract():
        hook = PostgresHook(postgres_conn_id="postgres_default")
        df = hook.get_pandas_df(
            """
            SELECT commande_id,
                   produit,
                   prix_unitaire,
                   quantite,
                   montant_total,
                   ville,
                   "timestamp"
            FROM silver.commandes_live
            WHERE "timestamp"::date = CURRENT_DATE - INTERVAL '1 day';
            """
        )
        print(str(len(df)) + " commandes extraites pour le rapport.")
        return df

    @task
    def clean_and_agregate(df):
        rapport = df.groupby("produit").agg(
            nombre_commandes=("commande_id", "count"),
            chiffre_affaires=("montant_total", "sum")
        ).reset_index()

        rapport["date_rapport"] = (datetime.now() - timedelta(days=1)).date()

        return rapport

    @task
    def load_report(rapport):
        hook = PostgresHook(postgres_conn_id="postgres_default")
        engine = hook.get_sqlalchemy_engine()
        rapport.to_sql(
            "rapport_journalier",
            engine,
            schema="gold",
            if_exists="append",
            index=False,
        )
        print(str(len(rapport)) + " lignes chargées dans gold.rapport_journalier.")

    donnees_brutes = extract()
    donnees_agregees = clean_and_agregate(donnees_brutes)
    load_report(donnees_agregees)


daily_batch_report()