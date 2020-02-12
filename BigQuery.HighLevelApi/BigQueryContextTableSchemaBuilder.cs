// Copyright (C) 2019 White Sharx (https://whitesharx.com) - All Rights Reserved.
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.

using System.Reflection;
using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using WhiteSharx.BigQuery.HighLevelApi.Attributes;
using WhiteSharx.BigQuery.HighLevelApi.Data;

namespace WhiteSharx.BigQuery.HighLevelApi {
  internal static class BigQueryContextTableSchemaBuilder {
    public static TableSchema BuildSchema<T>() {
      var publicProps = typeof(T).GetProperties();

      var builder = new TableSchemaBuilder();

      foreach (var property in publicProps) {

        if (property.GetCustomAttribute<BigQueryIgnoreAttribute>() != null) {
          continue;
        }

        builder.Add(SnakeCaseConverter.ConvertToSnakeCase(property.Name), new DataInfoFactory().Get(property).DbType);
      }

      return builder.Build();
    }
  }
}
