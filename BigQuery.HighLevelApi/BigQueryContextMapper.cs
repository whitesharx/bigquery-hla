// Copyright (C) 2019 White Sharx (https://whitesharx.com) - All Rights Reserved.
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.

using System.Collections.Generic;
using System.Reflection;
using Google.Cloud.BigQuery.V2;
using WhiteSharx.BigQuery.HighLevelApi.Attributes;
using WhiteSharx.BigQuery.HighLevelApi.Data;

namespace WhiteSharx.BigQuery.HighLevelApi {
  public class BigQueryContextMapper {

    private readonly DataInfoFactory dataInfoFactory = new DataInfoFactory();

    public IReadOnlyCollection<BigQueryInsertRow> MapToInsertRows<T>(IReadOnlyCollection<T> dataModels) {
      var publicProps = typeof(T).GetProperties();

      var rows = new List<BigQueryInsertRow>();

      foreach (var dataModel in dataModels) {

        var row = new BigQueryInsertRow();

        foreach (var property in publicProps) {
          if (property.GetCustomAttribute<BigQueryIgnoreAttribute>() != null) {
            continue;
          }

          row.Add(SnakeCaseConverter.ConvertToSnakeCase(property.Name), dataInfoFactory.Get(property).MapToRowValue(property.GetValue(dataModel)));
        }

        rows.Add(row);
      }

      return rows;
    }
  }
}
