
# TO DO

- In most scripts there's a gather up stage. This could be refactored.
  - It will need a list of necessary columns for the rest of the script to work, which will be retrieved from each file or created with nans as needed.
- group_dates()
	- Indicar filas iniciales y finales
- Modificar computación type_concept
  - Assign id to groups
    - use the already calculated index
  - groupby that id
  - compute mode on groupby
- AÑADIR TESTS!!
  - Test generales
    - All clinical event_start_dates must be populated
    - Start dates must be <= death_date
    - end dates must be <= death_date +60 days
    - if end_date, all event_start_dates must be <= event_end_dates
    - all clinical event_start_dates must be >= birthdate
- Refactor examples so they are test_friendly
  - La idea sería pasar cada etapa del proceso a una función.
  - Habría una función principal (main()) que lanzaría todo
  - Se añade un __main__ al final para que se lance todo si se lanza el script, pero que a la vez sea importable.
  - Con esto, se podrían hacer tests para cada etapa del proceso.


# Big changes

- Seems like serializing had a good effect on measurement processing, could we do it always like that?

- Use polars

- Refactor params so they follow a file first approach
  - You define a file and every other param needed is grouped under the file
    - If add a file now, you have to go look for the next section to put its name again and set their params
    - It would be better to put the name of the file once and every needed operation under it just once.
  - Normalize parameters names while you are at it

- Refactor functions of each table in a class
  - There should be a parent class that has common operations: 
    - Common checks
    - Format table to schema
    - etc.
  - Each table will have its own process.
    - Sequential, each table could have a "state" variable that tells you next step.
    - Could store warnings about data.
    - Could store the parameters for future reference.
    - 
