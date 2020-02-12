// Copyright (C) 2019 White Sharx (https://whitesharx.com) - All Rights Reserved.
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.

using System.Globalization;
using Google.Cloud.BigQuery.V2;

namespace WhiteSharx.BigQuery.HighLevelApi.Data {
  public class Float64DataInfo : IDataInfo {
    public BigQueryDbType DbType => BigQueryDbType.Float64;
    public object MapToRowValue(object source) => ((decimal) source).ToString(CultureInfo.InvariantCulture);
  }
}
