using Hangfire;
using Microsoft.Extensions.Options;
using SHS_Job_Integrate.Models;
using SHS_Job_Integrate.Services.Archive;
using SHS_Job_Integrate.Services.FileTransfer;

namespace SHS_Job_Integrate.Jobs;

[Queue("default")]
[AutomaticRetry(Attempts = 0)]
public class ArchiveCleanupJob
{
    private readonly IArchiveService _archiveService;
    private readonly IFileTransferFactory _fileTransferFactory;
    private readonly GcLcSettings _gcLcSettings;
    private readonly ILogger<ArchiveCleanupJob> _logger;

    public ArchiveCleanupJob(
        IArchiveService archiveService,
        IFileTransferFactory fileTransferFactory,
        IOptions<GcLcSettings> gcLcSettings,
        ILogger<ArchiveCleanupJob> logger)
    {
        _archiveService = archiveService;
        _fileTransferFactory = fileTransferFactory;
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

            // Cleanup GC-LC remote archive
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
        var fileTransfer = _fileTransferFactory.GetService();
        var totalDeleted = 0;

        totalDeleted += await CleanupRemoteFolderAsync(fileTransfer, _gcLcSettings.ProcessedPath, cutoffDate, ct);
        totalDeleted += await CleanupRemoteFolderAsync(fileTransfer, _gcLcSettings.ErrorPath, cutoffDate, ct);

        return totalDeleted;
    }

    private async Task<int> CleanupRemoteFolderAsync(IFileTransferService fileTransfer, string folderPath, DateTime cutoffDate, CancellationToken ct)
    {
        var deleted = 0;
        try
        {
            var files = await fileTransfer.ListFilesWithInfoAsync(folderPath, "*.*", ct);
            foreach (var file in files.Where(f => f.LastModified < cutoffDate))
            {
                try
                {
                    await fileTransfer.DeleteFileAsync(file.FullPath, ct);
                    deleted++;
                    _logger.LogDebug("Deleted old GC-LC archive file: {File}", file.FullPath);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to delete old GC-LC file: {File}", file.FullPath);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to cleanup GC-LC folder: {Folder}", folderPath);
        }
        return deleted;
    }
}