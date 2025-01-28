
# TO DO

- Corregir C5 (De qué, past-isi? D:)
- Plantear aislamiento del código de bps_to_omop del proyecto covid.
  - Crear repo para covid, igual que en hgsoc
  - Usar dvc y pipelines como en hgsoc
  - Mover docs allí
  - reducir al mínimo el contenido de bps_to_omop, para que sea lo más genérico posible.
- group_dates()
	- Eliminar salto en group_dates (\n en línea 268)
	- Indicar filas iniciales y finales
- config_files
  - Unificar comportamiento files_list
    - Only files_to_use, que no se listen archivos que no se van a usar.
    - Cada proceso tiene su config_file ahora. Eliminar las definiciones concretas
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

# DONE!

- ~~Fix visit_occurrence single-day visit overlap~~
  - Las visitas de varios días no pueden superponerse, pero si son de un sólo dia, sí.
  - Quizá se pueda solucionar haciendo el remove_overlap sólo para visitas con duración>1día.
    - Se sacan dos dataframes: visitas < 1 día, visitas > 1 día.
    - Se aplica remove_overlap sólo para visitas > 1 día.
    - Se concatenan ambos dataframes y se ordena.
- ~~AÑADIR TESTS!!~~
  - ~~Test provider_id, que en caso de que haya filas con misma fecha y con y sin provider_id, se mantenga el provider.~~
    - Se hace aumentando ncol hasta que incluye provider_id
  - ~~Test que al quitar el overlap, si hay dos citas diferentes **en un mismo día** con dos provider_id diferentes, queden ambos.~~
- group_dates()
  - ~~Eliminar salto en group_dates (\n en línea 268)~~
  - Test para intialize_extraction, read_yaml y update_yaml
- initialize_extraction()
  - ~~Change initialize not to require save dir~~
    - Que use la misma sintaxis que read_yaml y update_yaml
- ~~Mover C*.ipynb a docs~~
- initialize_extraction()
  -se ha eliminado la funcion por completo
  
