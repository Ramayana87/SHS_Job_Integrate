using Hangfire;
using SHS_Job_Integrate.Services.Archive;

namespace SHS_Job_Integrate.Jobs;

[Queue("default")]
[AutomaticRetry(Attempts = 0)]
public class ArchiveCleanupJob
{
    private readonly IArchiveService _archiveService;
    private readonly ILogger<ArchiveCleanupJob> _logger;

    public ArchiveCleanupJob(
        IArchiveService archiveService,
        ILogger<ArchiveCleanupJob> logger)
    {
        _archiveService = archiveService;
        _logger = logger;
    }

    public async Task ExecuteAsync(CancellationToken ct = default)
    {
        _logger.LogInformation("========== Archive Cleanup Job Started ==========");

        try
        {
            var deletedCount = await _archiveService.CleanupOldFilesAsync(ct);
            _logger.LogInformation("Cleanup completed:  {Count} files deleted", deletedCount);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Archive cleanup failed");
            throw;
        }

        _logger.LogInformation("========== Archive Cleanup Job Completed ==========");
    }
}