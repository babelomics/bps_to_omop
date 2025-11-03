# Repository Setup

This repository provides a suite of tools designed to **transform data stored in Parquet files into the OMOP Common Data Model (CDM) v5.4 standard**.

**Note:** This project is currently under active and heavy development. Users should anticipate frequent updates and changes.

The repository comes with a *pyproject.toml* and *pixi.lock* file that manage the environment. Make sure you have pixi installed (just run `curl -fsSL https://pixi.sh/install.sh | bash`). See [pixi](https://pixi.sh/latest/).

## Configuring and installing

To configure the environment, first run the makefile typing:

```bash
make
```

To activate the environment in the shell use:

```bash
pixi shell
```

This is equivalent to run `conda activate .venv`. To exit the environment just type `exit`.

To run a comand without entering the environment you can do `pixi run <insert-your-commands-here>`.