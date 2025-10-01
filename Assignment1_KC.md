
# GROUPE 3 : Kirsten CHANG

### USE CASE : Suivre les ventes de figurines KSCM

En bref, notre projet simule la création et le stockage des données de commandes pour otre entreprise de figurines, KSCM.

*   **Le code Python (Générateur de données)** : Il agit comme un système de point de vente qui enregistre chaque nouvelle commande. À chaque exécution, il génère des données de vente aléatoires mais réalistes pour les figurines KSCM.

*   **Le code Snowflake (Entrepôt de données)** : Il prépare la base de données où toutes ces informations de commandes seront stockées et analysées. C'est l'entrepôt central pour toutes les données de vente de votre entreprise.

### Les données générées : Que représentent-elles ?

Le script Python crée des enregistrements de commandes de clients. Chaque commande contient les informations suivantes :

*   **`ITEM` (L'article)** : Le nom de la figurine achetée. La liste `inventory` inclut toutes les variations possibles des figurines pour chaque fondateur (Kirsten, Sasha, Mattéo, et Corentin) dans différents styles (Policier, Vampire, etc.) et finitions (Gold, Silver, Regular).
    *   **Mention spéciale** : La figurine **"Statue Corentin Nu Gold"** est un article exclusif dans cette liste, étant donné que c'est le seul personnage qui a une édition "Nu".
*   **`PURCHASE_TIME` (Date d'achat)** : La date et l'heure exactes de la transaction.
*   **Informations sur le client** : Nom, adresse, téléphone, et email de l'acheteur.
*   **Identifiants uniques (`TXID`, `RFID`)** : Des numéros uniques pour suivre chaque transaction et chaque produit spécifique.

### Le choix de la table Snowflake : Pourquoi cette structure ?

La table `CLIENT_SUPPORT_ORDERS` dans Snowflake est conçue pour stocker efficacement ces informations de commande :

*   **Colonnes claires** : Chaque information (nom de l'article, date d'achat, nom du client, etc.) a sa propre colonne, ce qui facilite l'interrogation et l'analyse.
*   **Flexibilité avec `VARIANT`** : Les colonnes `ADDRESS` et `EMERGENCY_CONTACT` utilisent le type de données `VARIANT`, ce qui permet de stocker des informations structurées (JSON) comme une adresse complète, sans avoir à créer de multiples colonnes.
*   **Optimisation pour l'analyse** : Cette structure permet de répondre facilement à des questions commerciales clés, telles que :
    *   Quelle est la figurine la plus vendue ?
    *   Combien de figurines "Statue Corentin Nu Gold" ont été vendues ?
    *   Quels sont nos clients les plus fidèles ?
    *   Dans quelle région vendons-nous le plus ?