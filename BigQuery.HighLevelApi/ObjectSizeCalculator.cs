using System.Text;
using Google.Cloud.BigQuery.V2;

namespace WhiteSharx.BigQuery.HighLevelApi {
  public class ObjectSizeCalculator {
    public long GetObjectSize(BigQueryInsertRow row) {

      long rowSize = 0;

      foreach (var field in row) {
        string str = field?.ToString();

        if (str == null) {
          str = string.Empty;
        }

        var fieldSize = Encoding.UTF8.GetByteCount(str);

        rowSize += fieldSize;
      }

      return rowSize;
    }
  }
}
