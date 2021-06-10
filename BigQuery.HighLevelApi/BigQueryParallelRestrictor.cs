using System;
using System.Threading;
using System.Threading.Tasks;

namespace WhiteSharx.BigQuery.HighLevelApi {
  internal static class BigQueryParallelRestrictor {
    internal static async Task RestrictParallelInvocation(SemaphoreSlim semaphore, Func<Task> action) {

      if (semaphore == null) {
        await action.Invoke();

        return;
      }

      try {
        await semaphore.WaitAsync();
        await action.Invoke();
        semaphore.Release();
      } catch (Exception e) {
        semaphore.Release();
      }
    }
  }
}
