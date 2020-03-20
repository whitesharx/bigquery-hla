# bigquery-hla

An easy to use Google BigQuery database connector

## Features:

### Query
    var dataModels = await new BigQueryContext("projectId", "dataset", "Creds/creds.json").Query<MyDataModel>("SELECT * FROM `my-dataset.my-table` WHERE name = @Name", new { Name = "Alex" })

### Insert
    await new BigQueryContext("projectId", "dataset", "Creds/creds.json").GetTable<MyDataModel>("my-table").InsertMany(dataModels);

### Merge
    await new BigQueryContext("projectId", "dataset", "Creds/creds.json").GetTable<MyDataModel>("my-table").Upsert<MyDataModel>(dataModels, new Expression<Func<T, object>>[]{x => x.Id}, new Expression<Func<T, object>>[]{x => x.Name, x.Age})

## Attributes for data model members:

[BigQueryPartition] - defines partition and it's type

[BigQueryIgnore] - ignores member

## Changelog:
0.0.7 - Improved concurrency
0.0.6 - Added BigContext.Query( ) method; improved mapping