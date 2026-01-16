using Microsoft.Extensions.Options;
using SHS_Job_Integrate.Models;
using SHS_Job_Integrate.Services.FileTransfer;

namespace SHS_Job_Integrate.Services.Archive;

public class ArchiveService : IArchiveService
{
    private readonly IFileTransferFactory _fileTransferFactory;
    private readonly FileTransferConfig _config;
    private readonly ArchiveSettings _archiveSettings;
    private readonly ILogger<ArchiveService> _logger;

    public ArchiveService(
        IFileTransferFactory fileTransferFactory,
        IOptions<FileTransferConfig> config,
        ILogger<ArchiveService> logger)
    {
        _fileTransferFactory = fileTransferFactory;
        _config = config.Value;
        _archiveSettings = config.Value.Archive;
        _logger = logger;
    }

    public async Task<string> MoveToProcessedAsync(string remoteFilePath, CancellationToken ct = default)
    {
        var fileName = Path.GetFileName(remoteFilePath);
        var targetPath = BuildArchivePath(_archiveSettings.ProcessedPath, fileName);

        try
        {
            var fileTransfer = _fileTransferFactory.GetService();

            // Tạo thư mục nếu chưa có
            await EnsureDirectoryExistsAsync(fileTransfer, Path.GetDirectoryName(targetPath)!, ct);

            // Kiểm tra file trùng tên
            targetPath = await GetUniqueFilePathAsync(fileTransfer, targetPath, ct);

            // Di chuyển file
            await fileTransfer.MoveFileAsync(remoteFilePath, targetPath, ct);

            _logger.LogInformation("✓ Moved to processed: {Source} -> {Target}", remoteFilePath, targetPath);
            return targetPath;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to move file to processed:  {File}", remoteFilePath);
            throw;
        }
    }

    public async Task<string> MoveToErrorAsync(string remoteFilePath, string? errorMessage = null, CancellationToken ct = default)
    {
        var fileName = Path.GetFileName(remoteFilePath);
        var targetPath = BuildArchivePath(_archiveSettings.ErrorPath, fileName);

        try
        {
            var fileTransfer = _fileTransferFactory.GetService();

            // Tạo thư mục nếu chưa có
            await EnsureDirectoryExistsAsync(fileTransfer, Path.GetDirectoryName(targetPath)!, ct);

            // Kiểm tra file trùng tên
            targetPath = await GetUniqueFilePathAsync(fileTransfer, targetPath, ct);

            // Di chuyển file
            await fileTransfer.MoveFileAsync(remoteFilePath, targetPath, ct);

            // Tạo file log lỗi kèm theo
            if (!string.IsNullOrEmpty(errorMessage))
            {
                await CreateErrorLogFileAsync(fileTransfer, targetPath, errorMessage, ct);
            }

            _logger.LogWarning("⚠ Moved to error: {Source} -> {Target}, Reason: {Error}",
                remoteFilePath, targetPath, errorMessage ?? "Unknown");
            return targetPath;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to move file to error folder: {File}", remoteFilePath);
            throw;
        }
    }

    public async Task<string> BackupFileAsync(string remoteFilePath, CancellationToken ct = default)
    {
        if (!_archiveSettings.BackupBeforeProcess)
        {
            return string.Empty;
        }

        var fileName = Path.GetFileName(remoteFilePath);
        var targetPath = BuildArchivePath(_archiveSettings.BackupPath, fileName);

        try
        {
            var fileTransfer = _fileTransferFactory.GetService();

            // Tạo thư mục nếu chưa có
            await EnsureDirectoryExistsAsync(fileTransfer, Path.GetDirectoryName(targetPath)!, ct);

            // Kiểm tra file trùng tên
            targetPath = await GetUniqueFilePathAsync(fileTransfer, targetPath, ct);

            // Copy file (không phải move)
            await fileTransfer.CopyFileAsync(remoteFilePath, targetPath, ct);

            _logger.LogDebug("Backed up: {Source} -> {Target}", remoteFilePath, targetPath);
            return targetPath;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to backup file (continuing anyway): {File}", remoteFilePath);
            return string.Empty;
        }
    }

    public async Task<int> CleanupOldFilesAsync(CancellationToken ct = default)
    {
        if (_archiveSettings.RetentionDays <= 0)
        {
            _logger.LogDebug("Archive retention is disabled (RetentionDays = 0)");
            return 0;
        }

        var cutoffDate = DateTime.UtcNow.AddDays(-_archiveSettings.RetentionDays);
        var totalDeleted = 0;

        try
        {
            var fileTransfer = _fileTransferFactory.GetService();

            // Cleanup processed folder
            totalDeleted += await CleanupFolderAsync(fileTransfer, _archiveSettings.ProcessedPath, cutoffDate, ct);

            // Cleanup error folder
            totalDeleted += await CleanupFolderAsync(fileTransfer, _archiveSettings.ErrorPath, cutoffDate, ct);

            // Cleanup backup folder
            totalDeleted += await CleanupFolderAsync(fileTransfer, _archiveSettings.BackupPath, cutoffDate, ct);

            _logger.LogInformation("Archive cleanup completed:  {Count} files deleted (older than {Days} days)",
                totalDeleted, _archiveSettings.RetentionDays);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Archive cleanup failed");
        }

        return totalDeleted;
    }

    public void CleanupLocalFile(string localFilePath)
    {
        if (_archiveSettings.KeepLocalCopy)
        {
            _logger.LogDebug("Keeping local copy:  {File}", localFilePath);
            return;
        }

        try
        {
            if (File.Exists(localFilePath))
            {
                File.Delete(localFilePath);
                _logger.LogDebug("Deleted local file: {File}", localFilePath);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to delete local file: {File}", localFilePath);
        }
    }

    #region Private Helper Methods

    /// <summary>
    /// Build đường dẫn archive với date folder nếu được cấu hình
    /// </summary>
    private string BuildArchivePath(string basePath, string fileName)
    {
        // Normalize base path
        var path = NormalizePath(basePath);

        // Thêm date folder nếu được cấu hình
        if (_archiveSettings.UseDateFolder)
        {
            var now = DateTime.Now;
            path = Path.Combine(path, now.ToString("yyyy"), now.ToString("MM"), now.ToString("dd"));
        }

        return Path.Combine(path, fileName);
    }

    /// <summary>
    /// Normalize path - remove leading slashes, convert to consistent format
    /// </summary>
    private string NormalizePath(string path)
    {
        if (string.IsNullOrEmpty(path))
            return string.Empty;

        // Remove leading slashes
        path = path.TrimStart('/', '\\');

        // Convert to forward slashes for consistency
        path = path.Replace('\\', '/');

        return path;
    }

    /// <summary>
    /// Đảm bảo thư mục tồn tại
    /// </summary>
    private async Task EnsureDirectoryExistsAsync(IFileTransferService fileTransfer, string directoryPath, CancellationToken ct)
    {
        try
        {
            await fileTransfer.CreateDirectoryAsync(directoryPath, ct);
        }
        catch
        {
            // Directory might already exist, ignore error
        }
    }

    /// <summary>
    /// Lấy đường dẫn file unique nếu file đã tồn tại
    /// </summary>
    private async Task<string> GetUniqueFilePathAsync(IFileTransferService fileTransfer, string filePath, CancellationToken ct)
    {
        if (_archiveSettings.OverwriteExisting)
        {
            return filePath;
        }

        // Kiểm tra file có tồn tại không
        if (!await fileTransfer.FileExistsAsync(filePath, ct))
        {
            return filePath;
        }

        // Thêm timestamp vào tên file
        var directory = Path.GetDirectoryName(filePath) ?? "";
        var nameWithoutExt = Path.GetFileNameWithoutExtension(filePath);
        var extension = Path.GetExtension(filePath);
        var timestamp = DateTime.Now.ToString("yyyyMMddHHmmss");

        return Path.Combine(directory, $"{nameWithoutExt}_{timestamp}{extension}");
    }

    /// <summary>
    /// Tạo file log lỗi kèm theo file error
    /// </summary>
    private async Task CreateErrorLogFileAsync(IFileTransferService fileTransfer, string errorFilePath, string errorMessage, CancellationToken ct)
    {
        try
        {
            var logFilePath = errorFilePath + ".error.txt";
            var logContent = $"""
                Error Log
                ==========
                File: {Path.GetFileName(errorFilePath)}
                Time: {DateTime.Now:yyyy-MM-dd HH:mm:ss}
                
                Error Message: 
                {errorMessage}
                """;

            await fileTransfer.WriteTextFileAsync(logFilePath, logContent, ct);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to create error log file");
        }
    }

    /// <summary>
    /// Cleanup files trong folder theo cutoff date
    /// </summary>
    private async Task<int> CleanupFolderAsync(IFileTransferService fileTransfer, string folderPath, DateTime cutoffDate, CancellationToken ct)
    {
        var deleted = 0;

        try
        {
            var normalizedPath = NormalizePath(folderPath);
            var files = await fileTransfer.ListFilesWithInfoAsync(normalizedPath, "*.*", ct);

            foreach (var file in files.Where(f => f.LastModified < cutoffDate))
            {
                try
                {
                    await fileTransfer.DeleteFileAsync(file.FullPath, ct);
                    deleted++;
                    _logger.LogDebug("Deleted old archive file: {File}", file.FullPath);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to delete old file: {File}", file.FullPath);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to cleanup folder:  {Folder}", folderPath);
        }

        return deleted;
    }

    #endregion
}