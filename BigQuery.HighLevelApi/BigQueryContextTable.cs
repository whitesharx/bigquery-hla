// Copyright (C) 2019 White Sharx (https://whitesharx.com) - All Rights Reserved.
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Humanizer;
using WhiteSharx.BigQuery.HighLevelApi.Attributes;

namespace WhiteSharx.BigQuery.HighLevelApi {
  public class BigQueryContextTable<T> where T:class {

    private readonly SemaphoreSlim tableAccessLocker = new SemaphoreSlim(1, 1);

    private const int InsertBatchSize = 4000;
    private readonly string tableName;
    private readonly string projectId;
    private readonly string datasetId;
    private readonly string credsPath;
    private readonly BigQueryContextMapper mapper = new BigQueryContextMapper();
    private readonly BigQueryContextClientResolver bigQueryContextClientResolver = new BigQueryContextClientResolver();
    private readonly Batcher batcher = new Batcher();

    public BigQueryContextTable(string projectId, string datasetId, string tableName, string credsPath) {
      this.tableName = tableName;
      this.projectId = projectId;
      this.datasetId = datasetId;
      this.credsPath = credsPath;
    }

    public async Task EnsureExists() {
      await GetTable();
    }

    public async Task InsertMany(IReadOnlyCollection<T> dataModels) {

      var table = await GetTable();
      var rows = mapper.MapToInsertRows(dataModels);
      await batcher.Do(rows, async x => await table.InsertRowsAsync(x), InsertBatchSize);
    }

    public async Task Upsert(
      IReadOnlyCollection<T> dataModels,
      Expression<Func<T, object>>[] compareFields,
      Expression<Func<T, object>>[] updateFields = null) {

      string tempTableName = $"{tableName}_merge_{Guid.NewGuid().ToString().Replace("-", string.Empty)}";
      var tempTable = new BigQueryContextTable<T>(projectId, datasetId, tempTableName, credsPath);

      await tempTable.InsertMany(dataModels);

      try {
        await Merge(tempTableName, compareFields, updateFields);
      } finally {
        await tempTable.DeleteTable();
      }
    }

    private async Task Merge(
      string tempTableName,
      Expression<Func<T, object>>[] compareFields,
      Expression<Func<T, object>>[] updateFields = null) {

      var client = await bigQueryContextClientResolver.GetClient(projectId, credsPath);
      await GetTable();

      var sb = new StringBuilder();
      sb.Append($"MERGE `{datasetId}.{tableName}` t {Environment.NewLine}");
      sb.Append($"USING `{datasetId}.{tempTableName}` s {Environment.NewLine}");

      sb.Append("ON ");

      var compareMembers = GetFieldNames(compareFields);
      var merges = compareMembers.Select(x => $"t.{x} = s.{x}");
      var mergesStr = string.Join(" AND ", merges);
      sb.Append(mergesStr);
      sb.Append(Environment.NewLine);

      var updateMembers = updateFields != null ? GetFieldNames(updateFields) : GetFieldsNamesThatExcludesCompareMembers(compareMembers);

      if (updateMembers.Any()) {
        sb.Append($"WHEN MATCHED THEN {Environment.NewLine}");
        sb.Append($"UPDATE SET {Environment.NewLine}");

        var updates = updateMembers.Select(x => $"{x} = s.{x}").ToArray();
        var updatesStr = string.Join(", ", updates);
        sb.Append($"{updatesStr}{Environment.NewLine}");
      }

      sb.Append($"WHEN NOT MATCHED THEN {Environment.NewLine}");

      var allMembers = typeof(T).GetProperties().Where(x => x.GetCustomAttribute<BigQueryIgnoreAttribute>() == null).Select(x => SnakeCaseConverter.ConvertToSnakeCase(x.Name)).ToArray();
      var allMembersStr = $"({string.Join(", ", allMembers)})";
      sb.Append($"INSERT {allMembersStr}");
      sb.Append($"VALUES {allMembersStr}");

      string sql = sb.ToString();
      await client.ExecuteQueryAsync(sql, null);
    }

    private string[] GetFieldsNamesThatExcludesCompareMembers(string[] compareMembers) {
      var validFields = typeof(T).GetProperties().Where(x => x.GetCustomAttribute<BigQueryIgnoreAttribute>() == null)
        .Select(x => SnakeCaseConverter.ConvertToSnakeCase(x.Name));

      var updateFields = validFields.Except(compareMembers).ToArray();

      return updateFields;
    }

    public async Task DeleteTable() {
      var table = await GetTable();
      await table.DeleteAsync();
    }

    private async Task<BigQueryTable> GetTable() {

      var client = await bigQueryContextClientResolver.GetClient(projectId, credsPath);
      var dataset = client.GetDataset(projectId, datasetId);

      var schema = BigQueryContextTableSchemaBuilder.BuildSchema<T>();

      BigQueryTable table;


      await tableAccessLocker.WaitAsync();

      try {

        var partitionProperty = typeof(T).GetProperties()
          .SingleOrDefault(x => x.GetCustomAttribute<BigQueryPartitionAttribute>() != null);
        var options = new CreateTableOptions();

        if (partitionProperty != null) {
          options.TimePartitioning = new TimePartitioning {
            Field = SnakeCaseConverter.ConvertToSnakeCase(typeof(T).GetProperties()
              .Single(x => x.GetCustomAttribute<BigQueryPartitionAttribute>() != null).Name)
          };
        }

        table = await dataset.GetOrCreateTableAsync(tableName, schema, null, options);
      } finally {
        tableAccessLocker.Release();
      }

      return table;
    }

    private string[] GetFieldNames(Expression<Func<T, object>>[] mergeFields) {
      return mergeFields.Select(x => {

        string originalName = string.Empty;

        if (x.Body is UnaryExpression unaryExpression) {
          originalName = ((MemberExpression) unaryExpression.Operand).Member.Name;
        }

        if (x.Body is MemberExpression memberExpression) {
          originalName = memberExpression.Member.Name;
        }

        var cased = SnakeCaseConverter.ConvertToSnakeCase(originalName);

        return cased;


      }).Where(x => x != null).ToArray();
    }
  }
}
