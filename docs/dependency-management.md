# Dependency Management

Dependencies for this project are intended to be managed using poetry.

## Installing project dependencies
* project requirements: 
  * using poetry: `poetry install`
  * using pip: `pip install -r requirements.txt`
* development requirements: 
  * using poetry: `poetry install --with dev`
  * using pip: `pip install -r dev-requirements.txt`

## Adding new dependencies
#### Adding the dependency to `pyproject.toml`
* add project dependency
  * `poetry add <package-name>`
  * `poetry export --output requirements.txt`
* add dev dependency
  * `poetry add --dev <package-name>`
  * `poetry export --output dev-requirements.txt --only dev`
* add optional dependency
  * `poetry add <package-name> --optional`
