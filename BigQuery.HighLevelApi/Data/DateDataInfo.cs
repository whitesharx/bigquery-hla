// Copyright (C) 2019 White Sharx (https://whitesharx.com) - All Rights Reserved.
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.

using System;
using Google.Cloud.BigQuery.V2;

namespace WhiteSharx.BigQuery.HighLevelApi.Data {
  public class DateDataInfo : IDataInfo {
    public BigQueryDbType DbType => BigQueryDbType.Date;

    public object MapToRowValue(object source) => ((DateTime) source).ToString("yyyy-MM-dd");
  }
}
