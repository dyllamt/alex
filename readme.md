# Construct ETL Operations for Structured Data

dataspace standardizes ETL operations by flowing data through pandas `DataFrame` objects. There are two objects that facilitate ETL---`Workspace` objects and `Pipe` objects. Workspaces implement to-storage and from-storage operations, where data at run-time is stored (in memory) in a pandas data-frame. Pipes implement transfer operations and connect various data sources and destinations. These objects are extremely powerful for prototyping data pipelines.

There are several workspaces currently implemented in dataspace. Two workspaces interface with mongodb systems and are implemented at the collection level. A separate workspace interfaces with a set of retrieval APIs for open materials databases.

You can find a simple example with mongodb in mongodb_example.ipynb. For more examples of how I have integrated dataspace into database building and model construction, visit some of my other repositories.
