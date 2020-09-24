using System;
using System.Collections.Generic;
using Google.Cloud.BigQuery.V2;

namespace WhiteSharx.BigQuery.HighLevelApi {
  public class ObjectSizeCalculator {
    public long GetObjectSize(BigQueryInsertRow row) {
      long rowSize = InternalGetObjectSize(row);

      foreach (var field in row) {
        long fieldSize = InternalGetObjectSize(field);
        rowSize += fieldSize;
      }

      return rowSize;
    }

    private int InternalGetObjectSize(object o) {
      unsafe {
        RuntimeTypeHandle th = o.GetType().TypeHandle;
        int size = *(*(int**) &th + 1);

        return size;
      }
    }
  }
}
