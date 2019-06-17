## Agile ETL Prototyping

dataspace standardizes ETL operations for structured data. There are two base objects that facilitate ETL&mdash;`Workspace` objects and `Pipe` objects. Workspaces implement `to_storage` and `from_storage` operations for IO. Pipes implement `transfer` operations between source and destination workspaces, where the data flows through the `memory` attributes of workspaces as a pandas `DataFrame`. These objects are *agile* and powerful for prototyping data pipelines.

## Supported Endpoints

There are several workspaces currently implemented in dataspace. Two workspaces interface with mongodb systems and are implemented at the collection level. Another workspace interfaces with retrieval APIs for open materials databases.

## Examples

A simple example demonstrating ETL operations with mongodb is given in mongodb_example.ipynb. For more complex examples of how dataspace can be integrated into database building and machine learning, visit some of my other repositories ([matcom](https://github.com/dyllamt/matcom) & [bonding_models](https://github.com/dyllamt/bonding_models)).
