// Copyright (C) 2019 White Sharx (https://whitesharx.com) - All Rights Reserved.
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.

using System;
using Google.Cloud.BigQuery.V2;

namespace WhiteSharx.BigQuery.HighLevelApi.Attributes {
  public class BigQueryPartitionAttribute : Attribute {
    public BigQueryDbType Type { get; }

    public BigQueryPartitionAttribute(BigQueryDbType type = BigQueryDbType.Date) {

      if (type != BigQueryDbType.Date && type != BigQueryDbType.Timestamp) {
        throw new InvalidOperationException($"Type can be only {BigQueryDbType.Date} or {BigQueryDbType.Timestamp}");
      }

      Type = type;
    }
  }
}
