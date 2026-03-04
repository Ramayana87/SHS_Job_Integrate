using Hangfire;
using Microsoft.Extensions.Options;
using SHS_Job_Integrate.Models;
using SHS_Job_Integrate.Services.Archive;

namespace SHS_Job_Integrate.Jobs;

[Queue("default")]
[AutomaticRetry(Attempts = 0)]
public class ArchiveCleanupJob
{
    private readonly IArchiveService _archiveService;
    private readonly GcLcSettings _gcLcSettings;
    private readonly ILogger<ArchiveCleanupJob> _logger;

    public ArchiveCleanupJob(
        IArchiveService archiveService,
        IOptions<GcLcSettings> gcLcSettings,
        ILogger<ArchiveCleanupJob> logger)
    {
        _archiveService = archiveService;
        _gcLcSettings = gcLcSettings.Value;
        _logger = logger;
    }

    public async Task ExecuteAsync(CancellationToken ct = default)
    {
        _logger.LogInformation("========== Archive Cleanup Job Started ==========");

        try
        {
            // Cleanup NIR/Excel remote archive
            var deletedCount = await _archiveService.CleanupOldFilesAsync(ct);
            _logger.LogInformation("NIR/Excel archive cleanup completed: {Count} files deleted", deletedCount);

            // Cleanup GC-LC local archive
            var gcLcDeleted = await CleanupGcLcArchiveAsync(ct);
            _logger.LogInformation("GC-LC archive cleanup completed: {Count} files deleted", gcLcDeleted);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Archive cleanup failed");
            throw;
        }

        _logger.LogInformation("========== Archive Cleanup Job Completed ==========");
    }

    private async Task<int> CleanupGcLcArchiveAsync(CancellationToken ct)
    {
        if (_gcLcSettings.RetentionDays <= 0)
        {
            _logger.LogDebug("GC-LC archive retention is disabled (RetentionDays <= 0)");
            return 0;
        }

        var cutoffDate = DateTime.UtcNow.AddDays(-_gcLcSettings.RetentionDays);
        var totalDeleted = 0;

        totalDeleted += await CleanupLocalFolderAsync(_gcLcSettings.ProcessedPath, cutoffDate, ct);
        totalDeleted += await CleanupLocalFolderAsync(_gcLcSettings.ErrorPath, cutoffDate, ct);

        return totalDeleted;
    }

    private Task<int> CleanupLocalFolderAsync(string folderPath, DateTime cutoffDate, CancellationToken ct)
    {
        return Task.Run(() =>
        {
            var deleted = 0;

            if (!Directory.Exists(folderPath))
                return 0;

            try
            {
                foreach (var file in Directory.GetFiles(folderPath, "*.*", SearchOption.AllDirectories))
                {
                    ct.ThrowIfCancellationRequested();
                    try
                    {
                        var lastWrite = File.GetLastWriteTimeUtc(file);
                        if (lastWrite < cutoffDate)
                        {
                            File.Delete(file);
                            deleted++;
                            _logger.LogDebug("Deleted old GC-LC archive file: {File}", file);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Failed to delete old GC-LC file: {File}", file);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to cleanup GC-LC folder: {Folder}", folderPath);
            }

            return deleted;
        }, ct);
    }
}