// Copyright (C) 2019 White Sharx (https://whitesharx.com) - All Rights Reserved.
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Google.Cloud.BigQuery.V2;

namespace WhiteSharx.BigQuery.HighLevelApi {
  public class BigQueryContext{
    private readonly string projectId;
    private readonly string datasetName;
    private readonly string credsPath;

    private readonly BigQueryContextMapper mapper = new BigQueryContextMapper();

    public BigQueryContext(string projectId, string datasetName, string credsPath) {
      this.projectId = projectId;
      this.datasetName = datasetName;
      this.credsPath = credsPath;
    }

    public async Task<IReadOnlyCollection<T>> Query<T>(string sql, object parameters = null) {
      var client = await new BigQueryContextClientResolver().GetClient(projectId, credsPath);

      var nativeParameters = mapper.MapToNativeParameters(parameters);
      var results = await client.ExecuteQueryAsync(sql, nativeParameters);
      var models = mapper.MapFromResults<T>(results);

      return models;
    }

    public BigQueryContextTable<T> GetTable<T>(string name) where T: class{
      return new BigQueryContextTable<T>(projectId, datasetName, name, credsPath);
    }

    public static async Task<IReadOnlyCollection<string>> GetDatasetNames(string projectId, string credsPath) {
      var client = await new BigQueryContextClientResolver().GetClient(projectId, credsPath);
      var datasets = await client.ListDatasetsAsync(projectId).ReadPageAsync(100);
      var names = datasets.Select(x => x.Reference.DatasetId).ToArray();

      return names;
    }
  }
}
