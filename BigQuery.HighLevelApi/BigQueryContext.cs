// Copyright (C) 2019 White Sharx (https://whitesharx.com) - All Rights Reserved.
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Google.Cloud.BigQuery.V2;
using WhiteSharx.BigQuery.HighLevelApi.AuditLogs;

namespace WhiteSharx.BigQuery.HighLevelApi {
  public class BigQueryContext{
    private readonly string projectId;
    private readonly string datasetName;
    private readonly string credsPath;
    private readonly int? maxParallelism;
    private readonly Func<AuditLogConfig> auditLogConfig;

    private readonly BigQueryContextMapper mapper = new BigQueryContextMapper();

    private static SemaphoreSlim semaphore = null;
    private static object semaphoreCreationLocker = new object();

    public BigQueryContext(string projectId, string datasetName, string credsPath, int? maxParallelism = null, Func<AuditLogConfig> auditLogConfig = null) {
      this.projectId = projectId;
      this.datasetName = datasetName;
      this.credsPath = credsPath;
      this.maxParallelism = maxParallelism;
      this.auditLogConfig = auditLogConfig;

      if (maxParallelism.HasValue) {
        lock (semaphoreCreationLocker) {
          if (semaphore == null) {
            semaphore = new SemaphoreSlim(maxParallelism.Value, maxParallelism.Value);
          }
        }
      }
    }

    public async Task<IReadOnlyCollection<T>> Query<T>(string sql, object parameters = null, int? parallelPageSize = null) {
      var client = await new BigQueryContextClientResolver().GetClient(projectId, credsPath);

      var nativeParameters = mapper.MapToNativeParameters(parameters);

      BigQueryJob job = null;
      BigQueryResults results = null;
      IReadOnlyCollection<BigQueryRow> rows = null;

      await BigQueryParallelRestrictor.RestrictParallelInvocation(
        semaphore,
        async () => {
          job = await client.CreateQueryJobAsync(sql, nativeParameters);
          await job.PollUntilCompletedAsync();
          results = await job.GetQueryResultsAsync();

          if (parallelPageSize == null) {
            rows = results.ToList();
          } else {
            rows = await GetRowsInParallel(client, results, parallelPageSize.Value);
          }
        });

      long? bytesBilled = job.Statistics.Query.TotalBytesBilled;

      await new AuditLogSender(this, auditLogConfig).SendIfPossible(results.JobReference?.JobId, bytesBilled);

      var models = mapper.MapFromResults<T>(rows, results.Schema);

      return models;
    }

    private async Task<IReadOnlyCollection<BigQueryRow>> GetRowsInParallel(BigQueryClient client, BigQueryResults results, int pageSize) {
      var resultsDataset = await client.GetDatasetAsync(results.TableReference.DatasetId);
      var resultsTable = await resultsDataset.GetTableAsync(results.TableReference.TableId);

      var tasks = new List<Task>();
      var rows = new ConcurrentBag<BigQueryRow>();

      int batches = (int) results.TotalRows.Value / pageSize + 1;

      for (int i = 0; i < batches; i++) {
        int localI = i;
        var task = Task.Run(async () => {
          var pagedTableDataLists = resultsTable.ListRowsAsync(new ListRowsOptions {
            PageSize = pageSize,
            StartIndex = (ulong)(pageSize * localI)
          });
          var fetchedRows = await pagedTableDataLists.Take(pageSize).ToListAsync();

          foreach (var fetchedRow in fetchedRows) {
            rows.Add(fetchedRow);
          }
        });

        tasks.Add(task);
      }

      await Task.WhenAll(tasks);

      return rows;
    }

    public BigQueryContextTable<T> GetTable<T>(string name) where T: class{
      return new BigQueryContextTable<T>(projectId, datasetName, name, credsPath, semaphore);
    }

    public async Task<bool> TableExists(string tableName) {

      bool exists = false;

      await BigQueryParallelRestrictor.RestrictParallelInvocation(
        semaphore,
        async () => {
          var client = await new BigQueryContextClientResolver().GetClient(projectId, credsPath);
          var tables = client.ListTables(this.datasetName);
          exists = tables.Any(x => x.Reference.TableId.ToLower() == tableName.ToLower());
        });


      return exists;
    }

    public static async Task<IReadOnlyCollection<string>> GetDatasetNames(string projectId, string credsPath) {

      IReadOnlyCollection<string> names = null;

      await BigQueryParallelRestrictor.RestrictParallelInvocation(
        semaphore,
        async () => {
          var client = await new BigQueryContextClientResolver().GetClient(projectId, credsPath);
          var datasets = await client.ListDatasetsAsync(projectId).ReadPageAsync(100);
          names = datasets.Select(x => x.Reference.DatasetId).ToArray();
        });

      

      return names;
    }
  }
}
