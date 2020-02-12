// Copyright (C) 2019 White Sharx (https://whitesharx.com) - All Rights Reserved.
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace WhiteSharx.BigQuery.HighLevelApi {
  public class Batcher {
    public async Task Do<T>(IReadOnlyCollection<T> records, Func<IReadOnlyCollection<T>, Task> batchAction, int batchSize) {

      int currentBatch = 0;

      while (currentBatch * batchSize < records.Count) {
        var batchRecords = records.Skip(currentBatch * batchSize).Take(batchSize).ToArray();
        await batchAction(batchRecords);
        currentBatch++;
      }
    }
  }
}
