// Copyright (C) 2019 White Sharx (https://whitesharx.com) - All Rights Reserved.
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.

using Google.Cloud.BigQuery.V2;

namespace WhiteSharx.BigQuery.HighLevelApi.Data {
  public class StringDataInfo : IDataInfo {
    public BigQueryDbType DbType => BigQueryDbType.String;
    public object MapToRowValue(object source) => source?.ToString();
  }
}
