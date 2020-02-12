// Copyright (C) 2019 White Sharx (https://whitesharx.com) - All Rights Reserved.
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.

using System;
using System.Reflection;
using WhiteSharx.BigQuery.HighLevelApi.Attributes;

namespace WhiteSharx.BigQuery.HighLevelApi.Data {
  public class DataInfoFactory {
    public IDataInfo Get(PropertyInfo property) {
      if (property.PropertyType == typeof(DateTime)) {

        if (property.GetCustomAttribute<BigQueryPartitionAttribute>() != null) {
          return new DateDataInfo();
        }

        return new DateTimeDataInfo();
      }

      if (property.PropertyType == typeof(int) || property.PropertyType == typeof(long)) {
        return new Int64DataInfo();
      }

      if (property.PropertyType == typeof(decimal) || property.PropertyType == typeof(double) || property.PropertyType == typeof(float)) {
        return new Float64DataInfo();
      }

      return new StringDataInfo();
    }
  }
}
