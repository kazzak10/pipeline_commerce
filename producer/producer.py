import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

#on commence par créer le script pour génerer des commande aleatoire pour alimenter le programme

PRODUITS = [
    {"nom": "Casque audio", "prix": 79.99},
    {"nom": "Clavier mécanique", "prix": 129.90},
    {"nom": "Souris sans fil", "prix": 34.50},
    {"nom": "Écran 27 pouces", "prix": 249.00},
]
VILLE = [
  "LILLE",
  "PARIS",
  "LYON",
  "MARSEILLE",
  "DIJON"
  ]

def generer_commande():
  produit = random.choice(PRODUITS)

  ville = random.choice(VILLE)
 
  prix = produit["prix"]
 
  quantite = random.randint(1, 3)
 
  montant_total = prix*quantite
 
  commande_id = "CMD-" + str(int(time.time()*1000)) + "-" + str(random.randint(100,999))

  commande_final = {
    "commande_id":commande_id,
    "produit":produit["nom"],
    "prix_unitaire":prix,
    "quantite":quantite,
    "montant_total":montant_total,
    "ville":ville,
    "timestamp" : datetime.now(timezone.utc).isoformat()
  }
  return commande_final


producer = KafkaProducer(
      bootstrap_servers="kafka:9092",
      value_serializer=lambda v: json.dumps(v).encode("utf-8")
  )

while True :
  commande = generer_commande()
  producer.send("commandes", value=commande)
  print("commande envoyée"+commande["commande_id"]+ "-" +str(commande["montant_total"])+"€")
  time.sleep(4)