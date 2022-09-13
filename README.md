# pandas-lineage

BEWARE: This project is in very early stages (as of 2022-09-12)

pandas-lineage is intended to extend the functionality of I/O and standard transform operations on a pandas dataframe to emit OpenLineage RunEvents. I am starting just with read/write operations emiting RunEvents with schema facets.

## Examples:
* [marquez-examples](examples/marquez-example/)
  * contains getting started code and a script for running Marquez locally in Docker
* [mock-api-example](examples/mock-api-example)
  * contains getting started code and a simple Flask API for sending lineage events to which will just always return a 200 status code

## Notes:
* The pandas-lineage directory structure (for now) will mirror the directory structure of pandas for the components that it is extending.
