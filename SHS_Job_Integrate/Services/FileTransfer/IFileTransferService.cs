namespace SHS_Job_Integrate.Services.FileTransfer;

public interface IFileTransferService
{
    bool TestConnection();

    Task<List<string>> ListFilesAsync(string remotePath, string pattern, CancellationToken ct = default);

    /// <summary>
    /// List files với thông tin chi tiết (LastModified, Size)
    /// </summary>
    Task<List<RemoteFileInfo>> ListFilesWithInfoAsync(string remotePath, string pattern, CancellationToken ct = default);

    Task<string> DownloadFileAsync(string remotePath, string localPath, CancellationToken ct = default);

    Task MoveFileAsync(string sourcePath, string destPath, CancellationToken ct = default);

    /// <summary>
    /// Copy file (không xóa source)
    /// </summary>
    Task CopyFileAsync(string sourcePath, string destPath, CancellationToken ct = default);

    /// <summary>
    /// Xóa file
    /// </summary>
    Task DeleteFileAsync(string remotePath, CancellationToken ct = default);

    /// <summary>
    /// Kiểm tra file có tồn tại không
    /// </summary>
    Task<bool> FileExistsAsync(string remotePath, CancellationToken ct = default);

    /// <summary>
    /// Tạo thư mục (recursive)
    /// </summary>
    Task CreateDirectoryAsync(string remotePath, CancellationToken ct = default);

    /// <summary>
    /// Ghi text file
    /// </summary>
    Task WriteTextFileAsync(string remotePath, string content, CancellationToken ct = default);
}

public interface IFileTransferFactory
{
    IFileTransferService GetService();
    IFileTransferService GetService(string mode);
}

/// <summary>
/// Thông tin file remote
/// </summary>
public class RemoteFileInfo
{
    public string FileName { get; set; } = "";
    public string FullPath { get; set; } = "";
    public long Size { get; set; }
    public DateTime LastModified { get; set; }
    public bool IsDirectory { get; set; }
}