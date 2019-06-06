## Agile ETL Prototyping

dataspace standardizes ETL operations for structured data. There are two objects that facilitate ETL&mdash;`Workspace` objects and `Pipe` objects. Workspaces implement `to_storage` and `from_storage` operations for IO. Pipes implement `transfer` operations between source and destination workspaces, where the data flows through the `memory` attributes of workspaces as a pandas `DataFrame`. These objects are *agile* and powerful for prototyping data pipelines.

## Features

There are several workspaces currently implemented in dataspace. Two workspaces interface with mongodb systems and are implemented at the collection level. Another workspace interfaces with retrieval APIs for open materials databases.

## Examples

A simple example demonstrating ETL operations with mongodb is given in mongodb_example.ipynb. For more complex examples of how dataspace can be integrated into database building and machine learning, visit some of my other repositories.
