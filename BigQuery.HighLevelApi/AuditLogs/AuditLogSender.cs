using System;
using System.Threading.Tasks;

namespace WhiteSharx.BigQuery.HighLevelApi.AuditLogs {
  internal class AuditLogSender {
    private readonly BigQueryContext context;
    private readonly Func<AuditLogConfig> auditLogConfigResolver;

    public AuditLogSender(BigQueryContext context, Func<AuditLogConfig> auditLogConfigResolver) {
      this.context = context;
      this.auditLogConfigResolver = auditLogConfigResolver;
    }

    public async Task SendIfPossible(string jobId, long? bytesBilled) {

      if (auditLogConfigResolver?.Invoke() == null || jobId == null) {
        return;
      }

      var auditLogConfig = auditLogConfigResolver.Invoke();

      try {
        var table = context.GetTable<AuditLogDataModel>(auditLogConfig.TableName);
        var dataModel = new AuditLogDataModel {
          Date = DateTime.UtcNow,
          Initiator = auditLogConfig.Initiator,
          Comment = auditLogConfig.Comment,
          Source = auditLogConfig.Source,
          JobId = jobId,
          BytesBilled = bytesBilled ?? 0
        };
        await table.InsertMany(new[] { dataModel });
      } catch (Exception e) {
      }
    }
  }
}
