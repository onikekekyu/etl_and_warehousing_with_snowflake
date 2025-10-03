-- =============================================================================
-- SCRIPT DE SUPPRESSION (À EXÉCUTER EN TANT QU'ACCOUNTADMIN)
-- =============================================================================
USE ROLE ACCOUNTADMIN;

-- 1. Supprime la base de données et tout ce qu'elle contient (schémas, tables, stages, pipes)
-- C'est l'étape la plus importante pour nettoyer les données.
DROP DATABASE IF EXISTS FIGURINE_DB;

-- 2. Supprime l'entrepôt virtuel
DROP WAREHOUSE IF EXISTS FIGURINE_WH;

-- 3. Supprime l'utilisateur
DROP USER IF EXISTS FIGURINE_USER;

-- 4. Supprime le rôle. On le fait en dernier car les autres objets lui étaient liés.
DROP ROLE IF EXISTS FIGURINE_ROLE;