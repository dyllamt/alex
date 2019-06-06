# Construct ETL Operations for Structured Data

dataspace standardizes ETL operations for structured data. There are two objects that facilitate ETL&mdash;`Workspace`s and `Pipe`s. Workspaces implement `to_storage` and `from_storage` operations for IO with storage formats. Pipes implement `transfer` operations between sources and destinations, where the data flows through `memory` attributes of the workspace objects as pandas `DataFrame`s. These objects are *agile* and powerful for prototyping data pipelines.

There are several workspaces currently implemented in dataspace. Two workspaces interface with mongodb systems and are implemented at the collection level. A separate workspace interfaces with a set of retrieval APIs for open materials databases.

A simple example, demonstrating ETL operations with mongodb is given in mongodb_example.ipynb. For more complex examples of how dataspace can be integrated into database building and model construction, visit some of my other repositories.
