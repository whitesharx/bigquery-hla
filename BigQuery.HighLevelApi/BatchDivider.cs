using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;

namespace WhiteSharx.BigQuery.HighLevelApi {
  public class BatchDivider {

    /// <summary>
    /// Original size limit is 10485760, this value was achieved for this object size calculation way
    /// </summary>
    private const long PayloadSizeLimit = 7000000;
    private const int RowsCountLimit = 10000;

    private readonly ObjectSizeCalculator objectSizeCalculator = new ObjectSizeCalculator();

    public IEnumerable<IReadOnlyCollection<BigQueryInsertRow>> GetBatches(IEnumerable<BigQueryInsertRow> rows) {

      long currentPayload = 0;
      var batch = new List<BigQueryInsertRow>();

      foreach (var row in rows) {
        if (currentPayload >= PayloadSizeLimit || batch.Count == RowsCountLimit) {
          yield return batch;
          batch = new List<BigQueryInsertRow>();
          currentPayload = 0;
        }

        batch.Add(row);
        currentPayload += objectSizeCalculator.GetObjectSize(row);
      }

      if (batch.Any()) {
        yield return batch;
      }
    }
  }
}
