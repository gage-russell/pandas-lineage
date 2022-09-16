# pandas-lineage
BEWARE: This project is in very early stages (as of 2022-09-12)

pandas-lineage is intended to extend the functionality of I/O and standard transform operations on a pandas dataframe to emit OpenLineage RunEvents. I am starting just with read/write operations emiting RunEvents with schema facets.

## Badges:
![python-package](https://github.com/gage-russell/pandas-lineage/actions/workflows/python-package.yml/badge.svg)

## Installation
`pip install pandas-lineage`

## Development Documentation
* [dependency-management](docs/dependency-management.md)
* [publishing](docs/publishing.md)
* [testing](docs/testing.md)

## Examples:
* [marquez-examples](examples/marquez-example/)
  * contains getting started code and a script for running Marquez locally in Docker
* [mock-api-example](examples/mock-api-example)
  * contains getting started code and a simple Flask API for sending lineage events to which will just always return a 200 status code

## References:
* :green_heart: [Marquez](https://github.com/MarquezProject/marquez) :green_heart:
* :green_heart: [OpenLineage](https://github.com/OpenLineage/OpenLineage) :green_heart:
* :green_heart: [Pandas](https://github.com/pandas-dev/pandas) :green_heart:

## Contributing:
[Issues](https://github.com/gage-russell/pandas-lineage/issues)

I have not created any sort of contribution guide yet, but I don't want that to stop anyone!
If you are interested in contributing, fork this repository and open a PR. As this becomes more feature-rich/useful, we will establish a contributors workflow. For now, please just use the pre-commit hooks.

## Notes:
* The pandas-lineage directory structure (for now) will mirror the directory structure of pandas for the components that it is extending.
