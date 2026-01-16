namespace SHS_Job_Integrate.Services.Archive;

public interface IArchiveService
{
    /// <summary>
    /// Di chuyển file đến thư mục processed
    /// </summary>
    Task<string> MoveToProcessedAsync(string remoteFilePath, CancellationToken ct = default);

    /// <summary>
    /// Di chuyển file đến thư mục error
    /// </summary>
    Task<string> MoveToErrorAsync(string remoteFilePath, string? errorMessage = null, CancellationToken ct = default);

    /// <summary>
    /// Backup file trước khi xử lý
    /// </summary>
    Task<string> BackupFileAsync(string remoteFilePath, CancellationToken ct = default);

    /// <summary>
    /// Cleanup file cũ theo retention policy
    /// </summary>
    Task<int> CleanupOldFilesAsync(CancellationToken ct = default);

    /// <summary>
    /// Xóa file local tạm
    /// </summary>
    void CleanupLocalFile(string localFilePath);
}