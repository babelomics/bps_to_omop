--postgresql CDM DDL Specification for OMOP Common Data Model 5.4

--wrapper for starting the database, ie creating tables, and updating
-- vocabulary content.

-- initialize all tables
\i 01_OMOPCDM_ddl.sql
-- Load the vocab
\i 02_OMOPCDM_VocabularyLoad.sql
