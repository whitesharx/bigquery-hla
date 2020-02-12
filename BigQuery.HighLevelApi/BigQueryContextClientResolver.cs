// Copyright (C) 2019 White Sharx (https://whitesharx.com) - All Rights Reserved.
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.

using System.IO;
using System.Threading.Tasks;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.V2;

namespace WhiteSharx.BigQuery.HighLevelApi {
  public class BigQueryContextClientResolver {
    public async Task<BigQueryClient> GetClient(string projectId, string credsPath) {

      GoogleCredential creds;
      using (var stream = new FileStream(credsPath, FileMode.Open, FileAccess.Read)) {
        creds = GoogleCredential.FromStream(stream);
      }

      var client = await BigQueryClient.CreateAsync(projectId, creds);

      return client;
    }
  }
}
