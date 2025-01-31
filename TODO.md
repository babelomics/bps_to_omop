
# TO DO

- In most scripts there's a gather up stage. This could be refactored.
  - It will need a list of necessary columns for the rest of the script to work, which will be retrieved from each file or created with nans as needed.
- Corregir C5 (De qué, past-isi? D:)
- group_dates()
	- Indicar filas iniciales y finales
- source_to_omop mapping
  - Usar metodo de hgsoc, limpiar los anteriores.
  - Generalizar para cualquier *_source_value y*_source_concept_id
  - Definir bien pasos
  1- Crear *_source_value y vocabulary_id
  2- Buscar*_source_concept_id (gen.map_source_value())
  3- Buscar *_concept_id (gen.map_source_concept_id())
- Añadir PERSON y OBSERVATION_PERIOD al esquema gather, clean, format.
- Purgar funciones en OBSERVATION_PERIOD
- Relanzar C3
- Modificar computación type_concept
  - Assign id to groups
    - use the already calculated index
  - groupby that id
  - compute mode on groupby
- AÑADIR TESTS!!
  - Test generales
    - All clinical event_start_dates must be populted
    - Start dates must be <= death_date
    - end dates must be <= death_date +60 days
    - if end_date, all event_start_dates must be <= event_end_dates
    - all clinical event_start_dates must be >= birthdate
