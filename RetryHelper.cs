using Serilog;

namespace ParquetExporter.Services;

public static class RetryHelper
{
    public static async Task ExecuteWithRetryAsync(
        Func<Task> action,
        int maxRetries,
        TimeSpan baseDelay,
        ILogger logger,
        string operationName)
    {
        var attempt = 0;
        Exception? lastException = null;

        while (attempt <= maxRetries)
        {
            try
            {
                await action();
                return;
            }
            catch (Exception ex)
            {
                lastException = ex;
                attempt++;

                if (attempt > maxRetries)
                {
                    logger.Error(ex,
                        "Operation {OperationName} failed after {Attempts} attempts",
                        operationName, attempt);
                    throw;
                }

                var delay = TimeSpan.FromMilliseconds(
                    baseDelay.TotalMilliseconds * Math.Pow(2, attempt - 1));

                logger.Warning(ex,
                    "Operation {OperationName} failed (attempt {Attempt}/{Max}). Retrying in {Delay}...",
                    operationName, attempt, maxRetries, delay);

                await Task.Delay(delay);
            }
        }

        throw lastException ?? new Exception($"Operation {operationName} failed with unknown error.");
    }
}
