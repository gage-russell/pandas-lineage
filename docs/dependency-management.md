# Dependency Management

Dependencies for this project are intended to be managed using poetry.

## Installing project dependencies
* project requirements: 
  * using poetry: `poetry install --without test,docs`
  * using pip: `pip install -r requirements.txt`
* development requirements: 
  * using poetry: `poetry install --with test`
  * using pip: `pip install -r dev-requirements.txt`
* documentation requirements: 
  * using poetry: `poetry install --with docs`
  * using pip: `pip install -r docs-requirements.txt`

## Adding new dependencies
#### Adding the dependency to `pyproject.toml`
* add project dependency
  * `poetry add <package-name>`
  * `poetry export --output requirements.txt --without test,docs`
* add test dependency
  * `poetry add <package-name> --group test`
  * `poetry export --output dev-requirements.txt --with test`
* add docs dependency
  * `poetry add <package-name> --group docs`
  * `poetry export --output docs-requirements.txt --with docs`
* add optional dependency
  * `poetry add <package-name> --group optional`
  * `poetry export --output optional-requirements.txt --with optional`
