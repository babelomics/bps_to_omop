--postgresql CDM DDL Specification for OMOP Common Data Model 5.4

--wrapper for finishing the database after it is populated.
-- ie creating primary_keys, constraints, indices and clusters.

-- create primary keys
\i 11_OMOPCDM_primary_keys.sql
-- Create constraints, aka relations between keys
\i 12_OMOPCDM_constraints.sql
-- create indices and clustrs
\i 13_OMOPCDM_indices_clusters.sql
