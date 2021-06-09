using System;
using Google.Cloud.BigQuery.V2;
using WhiteSharx.BigQuery.HighLevelApi.Attributes;

namespace WhiteSharx.BigQuery.HighLevelApi.AuditLogs {
  internal class AuditLogDataModel {
    [BigQueryPartition(BigQueryDbType.Timestamp)]
    public DateTime Date { get; set; }
    public string JobId { get; set; }
    public string Source { get; set; }
    public string Initiator { get; set; }
    public string Comment { get; set; }
    public long BytesBilled { get; set; }
  }
}
