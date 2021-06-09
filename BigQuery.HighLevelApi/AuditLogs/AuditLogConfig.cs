namespace WhiteSharx.BigQuery.HighLevelApi.AuditLogs {
  public class AuditLogConfig {
    public string Initiator { get; set; }
    public string Source { get; set; }
    public string Comment { get; set; }
    public string TableName { get; set; }
  }
}
