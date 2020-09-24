// Copyright (C) 2019 White Sharx (https://whitesharx.com) - All Rights Reserved.
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Google.Cloud.BigQuery.V2;

namespace WhiteSharx.BigQuery.HighLevelApi {
  public class BatchExecutor {
    public async Task Do(IReadOnlyCollection<BigQueryInsertRow> records, Func<IReadOnlyCollection<BigQueryInsertRow>, Task> batchAction, int batchSize) {

      int currentBatch = 0;

      while (currentBatch * batchSize < records.Count) {
        var batchRecords = records.Skip(currentBatch * batchSize).Take(batchSize).ToArray();

        await batchAction(batchRecords);
        currentBatch++;
      }
    }
  }
}
