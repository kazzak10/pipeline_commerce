CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;


CREATE TABLE silver.commandes_live (
    commande_id     VARCHAR(64) PRIMARY KEY,
    produit         VARCHAR(128) NOT NULL,
    prix_unitaire   NUMERIC(10, 2) NOT NULL,
    quantite        INTEGER NOT NULL,
    montant_total   NUMERIC(10, 2) NOT NULL,
    ville           VARCHAR(64),
    "timestamp"     TIMESTAMPTZ NOT NULL
);

CREATE TABLE gold.rapport_journalier (
    id                  SERIAL PRIMARY KEY,
    produit             VARCHAR(128) NOT NULL,
    nombre_commandes    INTEGER NOT NULL,
    chiffre_affaires    NUMERIC(12, 2) NOT NULL,
    date_rapport        DATE NOT NULL,
    UNIQUE (produit, date_rapport)
);
