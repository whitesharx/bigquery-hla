// Copyright (C) 2019 White Sharx (https://whitesharx.com) - All Rights Reserved.
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Google.Cloud.BigQuery.V2;
using WhiteSharx.BigQuery.HighLevelApi.Attributes;
using WhiteSharx.BigQuery.HighLevelApi.Data;

namespace WhiteSharx.BigQuery.HighLevelApi {
  public class BigQueryContextMapper {

    private readonly DataInfoFactory dataInfoFactory = new DataInfoFactory();

    public IEnumerable<BigQueryInsertRow> MapToInsertRows<T>(IReadOnlyCollection<T> dataModels) {
      var publicProps = typeof(T).GetProperties();

      foreach (var dataModel in dataModels) {

        var row = new BigQueryInsertRow();

        foreach (var property in publicProps) {
          if (property.GetCustomAttribute<BigQueryIgnoreAttribute>() != null) {
            continue;
          }

          string fieldName = SnakeCaseConverter.ConvertToSnakeCase(property.Name);
          var fieldValue = dataInfoFactory.Get(property).MapToRowValue(property.GetValue(dataModel));

          row.Add(fieldName, fieldValue);
        }

        yield return row;
      }
    }

    public IReadOnlyCollection<BigQueryParameter> MapToNativeParameters(object parameters) {
      var nativeParameters = new List<BigQueryParameter>();

      if (parameters == null) {
        return nativeParameters;
      }

      var props = parameters.GetType().GetProperties();

      foreach (var prop in props) {
        var dataInfo = dataInfoFactory.Get(prop);
        var nativeParameter = new BigQueryParameter {
          Name = prop.Name,
          Type = dataInfo.DbType,
          Value = dataInfo.MapToRowValue(prop.GetValue(parameters))
        };
        nativeParameters.Add(nativeParameter);
      }

      return nativeParameters;
    }

    public IReadOnlyCollection<T> MapFromResults<T>(BigQueryResults results) {

      var models = new List<T>();

      foreach (var row in results) {

        var instance = Activator.CreateInstance<T>();

        foreach (var field in results.Schema.Fields) {
          var property = typeof(T).GetProperties().SingleOrDefault(x => x.Name == field.Name);

          if (property == null) {
            throw new InvalidOperationException($"Unable to find a model property for the response field '{field.Name}'");
          }

          property.SetValue(instance, row[field.Name]);
        }

        models.Add(instance);
      }

      return models;
    }
  }
}
