# Bps_to_omop tool

Una   herramienta   desarrollada   para   el  [Plan   Complementario   de
Biotecnología   aplicada   a   la   Salud](https://planescomplementariossalud.es/).

Dentro   de   la   Línea   1:   Desarrollo   de   herramientas   para   diagnóstico, pronóstico y terapias avanzadas o dirigidas en medicina personalizada.

El objetivo de la **línea de actuación 1** consiste en la armonización de las bases de bio-datos (desde historias médicas, a datos biométricos, datos
ómicos, etc.) para poder utilizar todo el poder del Big Data y de la IA no solo dentro de la CA de Andalucía sino en colaboración con otras CCAA que estén trabajando o vayan a trabajar en el proyecto BAS. 

**La línea de actuación LA1.1.** Desarrollo de una pasarela que transforme
los datos clínicos (diagnósticos, tratamientos, analíticas, uso del sistema de salud,   etc.)   en   modelo   OMOP   para   su   interoperabilidad   en   proyectos federados con otras CCAA. 

Como   consecuencia   se   desarrolla  bps_to_omop  con   las   siguientes
características: 

- **Servicios que se ofertan:**   Una   aplicación   operativa   que   permite
transformar   los   datos   clínicos   del   sistema   andaluz   (BPS)   a   formato OMOP,   un   estándar   internacional.   Están   cubiertos   datos sociodemográficos,   visitas   clínicas,   diagnósticos,   síntomas,   pruebas analíticas y periodo de observación.
- **A quien van dirigidos:** Investigadores que necesiten usar datos clínicos
estructurados y homologables, tanto para estudios retrospectivos dentro
de Andalucía como en redes nacionales/internacionales. 
- **Qué supondrá a nivel de cambio de paradigma a escala regional:**
La disponibilidad de datos clínicos en formato OMOP (un estándar internacional reconocido) permite a Andalucía integrarse directamente en   redes federadas de investigación como [OmicSpace](https://omicspace.iislafe.es/en/home/) o [IMPaCT de datos](https://impact-data.bsc.es/en/), evitando costosas fases de preparación de datos. Esto coloca a la región en una posición competitiva para participar en proyectos internacionales como [DARWIN](https://www.darwin-eu.org/). 
-**Aplicaciones:** Participación en el proyecto [OmicSpace](https://omicspace.iislafe.es/en/home/), en el que participamos como un nodo federado, la parte de conversión a OMOP la permite bps_to_omop. En el proyecto IMPaCT de ciencia de datos, que co-lideramos junto con el BSC, la homogeneización de los datos está en la base de su federación. También abre la posibilidad a Andalucía a participar en iniciativas internacionales a gran escala, como el proyecto [DARWIN](https://www.darwin-eu.org/.

## ⚙️ Configuración e instalación

**Nota:** Este proyecto se encuentra actualmente en **desarrollo activo**. Los usuarios deben anticipar **actualizaciones y cambios frecuentes**.

-----

El repositorio incluye un archivo ***pyproject.toml*** y ***pixi.lock*** que gestionan el entorno. Es necesario tener la herramienta **pixi** instalada (se puede instalar ejecutando `curl -fsSL https://pixi.sh/install.sh | bash`).

Puedes encontrar más información sobre `pixi` [aquí](https://pixi.sh/latest/).

### Pasos de configuración

1.  **Configurar el entorno**.
    Para configurar el entorno, primero ejecutamos el *makefile*:

    ```bash
    make
    ```

2.  **Activar el entorno**.
    Para **activar** el entorno en la *shell*:

    ```bash
    pixi shell
    ```

      * Esto es equivalente a ejecutar `conda activate .venv`.
      * Para **salir** del entorno, simplemente escribe `exit`.

3.  **Ejecutar comandos sin entrar al entorno**
    Para ejecutar un comando sin necesidad de activar el entorno:

    ```bash
    pixi run <insert-your-commands-here>
    ```

-----
