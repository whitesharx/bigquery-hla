// Copyright (C) 2019 White Sharx (https://whitesharx.com) - All Rights Reserved.
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.

using Humanizer;

namespace WhiteSharx.BigQuery.HighLevelApi {
  internal static class SnakeCaseConverter {
    public static string ConvertToSnakeCase(string input) {
      return input.Underscore();
    } 
  }
}
