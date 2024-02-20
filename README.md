# Demo - Potential Kuzu REL GROUP Data Ingest Bugs

Demonstrating various potential bugs with Kuzu REL Table Groups.

## Installation

Please use Python [`poetry`](https://python-poetry.org/) to run and install related content.
The Poetry environment for this project includes dependencies which help demonstrate the content below.

```bash
# after installing poetry, create the environment
poetry install
```

## Poe the Poet

Use [Poe the Poet](https://poethepoet.natn.io/index.html) to define and run tasks defined within `pyproject.toml` under the section `[tool.poe.tasks*]`.

For example, use the following to run the `run_demo` task:

```bash
# run data_prep task using poethepoet defined within `pyproject.toml`
poetry run poe run_demo
```

## Data Acknowledgement

Data found within this repo is a modified version of [RTX-KG2](https://github.com/RTXteam/RTX-KG2) data which was published at the [NCATS Biomedical Data Translator repository](https://github.com/ncats/translator-lfs-artifacts). Special thanks goes to RTX researchers, developers, and those related to support for the data.
