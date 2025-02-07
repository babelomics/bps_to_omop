
# TO DO

- In most scripts there's a gather up stage. This could be refactored.
  - It will need a list of necessary columns for the rest of the script to work, which will be retrieved from each file or created with nans as needed.
- Corregir C5 (De qué, past-isi? D:)
- group_dates()
	- Indicar filas iniciales y finales
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
- Refactor examples so they are test_friendly
  - La idea sería pasar cada etapa del proceso a una función.
  - Habría una función principal (main()) que lanzaría todo
  - Se añade un __main__ al final para que se lance todo si se lanza el script, pero que a la vez sea importable.
  - Con esto, se podrían hacer tests para cada etapa del proceso.
