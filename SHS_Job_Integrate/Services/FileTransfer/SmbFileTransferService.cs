using Microsoft.Extensions.Options;
using SHS_Job_Integrate.Models;
using SMBLibrary;
using SMBLibrary.Client;
using System.Net;
using FileAttributes = SMBLibrary.FileAttributes;

namespace SHS_Job_Integrate.Services.FileTransfer;

public class SmbFileTransferService : IFileTransferService
{
    private readonly SmbConfig _config;
    private readonly ILogger<SmbFileTransferService> _logger;
    private readonly string _host;
    private readonly string _shareName;

    public SmbFileTransferService(
        IOptions<FileTransferConfig> config,
        ILogger<SmbFileTransferService> logger)
    {
        _config = config.Value.Smb;
        _logger = logger;

        // Parse host và share từ config
        var hostParts = _config.Host.TrimStart('\\').Split('\\', StringSplitOptions.RemoveEmptyEntries);
        _host = hostParts.Length > 0 ? hostParts[0] : _config.Host;
        _shareName = hostParts.Length > 1 ? hostParts[1] : "share";
    }

    private (SMB2Client client, ISMBFileStore fileStore) Connect()
    {
        var client = new SMB2Client();

        if (!client.Connect(IPAddress.Parse(_host), SMBTransportType.DirectTCPTransport))
        {
            throw new Exception($"Cannot connect to SMB server: {_host}");
        }

        var status = client.Login(_config.Domain, _config.Username, _config.Password);
        if (status != NTStatus.STATUS_SUCCESS)
        {
            throw new Exception($"SMB login failed: {status}");
        }

        var fileStore = client.TreeConnect(_shareName, out status);
        if (status != NTStatus.STATUS_SUCCESS)
        {
            throw new Exception($"Cannot access share {_shareName}: {status}");
        }

        return (client, fileStore);
    }

    public bool TestConnection()
    {
        try
        {
            var (client, _) = Connect();
            client.Disconnect();
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "SMB connection test failed");
            return false;
        }
    }

    public Task<List<string>> ListFilesAsync(string remotePath, string pattern, CancellationToken ct = default)
    {
        var files = new List<string>();
        var (client, fileStore) = Connect();

        try
        {
            var normalizedPath = NormalizePath(remotePath);

            var status = fileStore.CreateFile(
                out var handle,
                out _,
                normalizedPath,
                AccessMask.GENERIC_READ,
                FileAttributes.Directory,
                ShareAccess.Read | ShareAccess.Write,
                CreateDisposition.FILE_OPEN,
                CreateOptions.FILE_DIRECTORY_FILE,
                null);

            if (status == NTStatus.STATUS_SUCCESS)
            {
                fileStore.QueryDirectory(out var entries, handle, "*", FileInformationClass.FileDirectoryInformation);
                fileStore.CloseFile(handle);

                foreach (var entry in entries.Cast<FileDirectoryInformation>())
                {
                    if (entry.FileName == "." || entry.FileName == "..") continue;
                    if (entry.FileAttributes.HasFlag(FileAttributes.Directory)) continue;

                    if (MatchPattern(entry.FileName, pattern))
                    {
                        var fullPath = Path.Combine(normalizedPath, entry.FileName).Replace('\\', '/');
                        files.Add(fullPath);
                    }
                }
            }
        }
        finally
        {
            client.Disconnect();
        }

        _logger.LogInformation("SMB:  Found {Count} files matching {Pattern} in {Path}", files.Count, pattern, remotePath);
        return Task.FromResult(files);
    }

    public Task<List<RemoteFileInfo>> ListFilesWithInfoAsync(string remotePath, string pattern, CancellationToken ct = default)
    {
        var files = new List<RemoteFileInfo>();
        var (client, fileStore) = Connect();

        try
        {
            ListFilesRecursive(fileStore, NormalizePath(remotePath), pattern, files);
        }
        finally
        {
            client.Disconnect();
        }

        return Task.FromResult(files);
    }

    private void ListFilesRecursive(ISMBFileStore fileStore, string path, string pattern, List<RemoteFileInfo> files, HashSet<string>? visited = null)
    {
        visited ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        // Normalize path (replace backslashes, trim)
        var normalizedPath = (path ?? string.Empty).Replace('\\', '/').Trim();

        if (!visited.Add(normalizedPath))
        {
            _logger.LogDebug("SMB: Skipping already visited path {Path}", normalizedPath);
            return;
        }

        try
        {
            var status = fileStore.CreateFile(
                out var handle,
                out _,
                normalizedPath,
                AccessMask.GENERIC_READ,
                FileAttributes.Directory,
                ShareAccess.Read | ShareAccess.Write,
                CreateDisposition.FILE_OPEN,
                CreateOptions.FILE_DIRECTORY_FILE,
                null);

            if (status != NTStatus.STATUS_SUCCESS) return;

            fileStore.QueryDirectory(out var entries, handle, "*", FileInformationClass.FileDirectoryInformation);
            fileStore.CloseFile(handle);

            foreach (var entry in entries.Cast<FileDirectoryInformation>())
            {
                var entryName = entry.FileName?.Trim();
                if (string.IsNullOrEmpty(entryName)) continue;

                if (entryName == "." || entryName == "..") continue;

                var fullPath = Path.Combine(normalizedPath, entryName).Replace('\\', '/');

                if (entry.FileAttributes.HasFlag(FileAttributes.Directory))
                {
                    // Recurse into directory
                    ListFilesRecursive(fileStore, fullPath, pattern, files, visited);
                }
                else if (MatchPattern(entry.FileName, pattern))
                {
                    files.Add(new RemoteFileInfo
                    {
                        FileName = entry.FileName,
                        FullPath = fullPath,
                        Size = entry.EndOfFile,
                        LastModified = entry.LastWriteTime,
                        IsDirectory = false
                    });
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to list directory:  {Path}", path);
        }
    }

    public Task<string> DownloadFileAsync(string remotePath, string localPath, CancellationToken ct = default)
    {
        var localFilePath = Path.Combine(localPath, Path.GetFileName(remotePath));
        Directory.CreateDirectory(localPath);

        var (client, fileStore) = Connect();

        try
        {
            var normalizedPath = NormalizePath(remotePath);

            var status = fileStore.CreateFile(
                out var handle,
                out _,
                normalizedPath,
                AccessMask.GENERIC_READ,
                FileAttributes.Normal,
                ShareAccess.Read,
                CreateDisposition.FILE_OPEN,
                CreateOptions.FILE_NON_DIRECTORY_FILE,
                null);

            if (status != NTStatus.STATUS_SUCCESS)
            {
                throw new Exception($"Cannot open file {remotePath}: {status}");
            }

            using var fs = File.Create(localFilePath);
            long offset = 0;

            while (true)
            {
                status = fileStore.ReadFile(out var data, handle, offset, 65536);
                if (status != NTStatus.STATUS_SUCCESS || data.Length == 0) break;
                fs.Write(data, 0, data.Length);
                offset += data.Length;
            }

            fileStore.CloseFile(handle);
        }
        finally
        {
            client.Disconnect();
        }

        _logger.LogInformation("SMB: Downloaded {Remote} to {Local}", remotePath, localFilePath);
        return Task.FromResult(localFilePath);
    }

    public Task MoveFileAsync(string sourcePath, string destPath, CancellationToken ct = default)
    {
        var (client, fileStore) = Connect();

        try
        {
            var normalizedSource = NormalizePath(sourcePath);
            var normalizedDest = NormalizePath(destPath);

            // Tạo thư mục đích
            var destDir = Path.GetDirectoryName(normalizedDest);
            if (!string.IsNullOrEmpty(destDir))
            {
                CreateDirectoryRecursive(fileStore, destDir);
            }

            // SMB rename/move
            var status = fileStore.CreateFile(
                out var handle,
                out _,
                normalizedSource,
                AccessMask.GENERIC_READ | AccessMask.DELETE,
                FileAttributes.Normal,
                ShareAccess.Read | ShareAccess.Write | ShareAccess.Delete,
                CreateDisposition.FILE_OPEN,
                CreateOptions.FILE_NON_DIRECTORY_FILE,
                null);

            if (status != NTStatus.STATUS_SUCCESS)
            {
                throw new Exception($"Cannot open source file {sourcePath}:  {status}");
            }

            // Set rename info
            var renameInfo = new FileRenameInformationType2
            {
                ReplaceIfExists = true,
                FileName = normalizedDest
            };

            status = fileStore.SetFileInformation(handle, renameInfo);
            fileStore.CloseFile(handle);

            if (status != NTStatus.STATUS_SUCCESS)
            {
                throw new Exception($"Cannot rename file {sourcePath} to {destPath}:  {status}");
            }

            _logger.LogDebug("SMB:  Moved {Source} to {Dest}", sourcePath, destPath);
        }
        finally
        {
            client.Disconnect();
        }

        return Task.CompletedTask;
    }

    public async Task CopyFileAsync(string sourcePath, string destPath, CancellationToken ct = default)
    {
        var (client, fileStore) = Connect();

        try
        {
            var normalizedSource = NormalizePath(sourcePath);
            var normalizedDest = NormalizePath(destPath);

            // Tạo thư mục đích
            var destDir = Path.GetDirectoryName(normalizedDest);
            if (!string.IsNullOrEmpty(destDir))
            {
                CreateDirectoryRecursive(fileStore, destDir);
            }

            // Đọc file source
            var status = fileStore.CreateFile(
                out var readHandle,
                out _,
                normalizedSource,
                AccessMask.GENERIC_READ,
                FileAttributes.Normal,
                ShareAccess.Read,
                CreateDisposition.FILE_OPEN,
                CreateOptions.FILE_NON_DIRECTORY_FILE,
                null);

            if (status != NTStatus.STATUS_SUCCESS)
            {
                throw new Exception($"Cannot open source file {sourcePath}: {status}");
            }

            // Tạo file đích
            status = fileStore.CreateFile(
                out var writeHandle,
                out _,
                normalizedDest,
                AccessMask.GENERIC_WRITE,
                FileAttributes.Normal,
                ShareAccess.None,
                CreateDisposition.FILE_OVERWRITE_IF,
                CreateOptions.FILE_NON_DIRECTORY_FILE,
                null);

            if (status != NTStatus.STATUS_SUCCESS)
            {
                fileStore.CloseFile(readHandle);
                throw new Exception($"Cannot create dest file {destPath}:  {status}");
            }

            // Copy data
            long offset = 0;
            while (true)
            {
                status = fileStore.ReadFile(out var data, readHandle, offset, 65536);
                if (status != NTStatus.STATUS_SUCCESS || data.Length == 0) break;

                fileStore.WriteFile(out _, writeHandle, offset, data);
                offset += data.Length;
            }

            fileStore.CloseFile(readHandle);
            fileStore.CloseFile(writeHandle);

            _logger.LogDebug("SMB:  Copied {Source} to {Dest}", sourcePath, destPath);
        }
        finally
        {
            client.Disconnect();
        }
    }

    public Task DeleteFileAsync(string remotePath, CancellationToken ct = default)
    {
        var (client, fileStore) = Connect();

        try
        {
            var normalizedPath = NormalizePath(remotePath);

            var status = fileStore.CreateFile(
                out var handle,
                out _,
                normalizedPath,
                AccessMask.DELETE,
                FileAttributes.Normal,
                ShareAccess.Delete,
                CreateDisposition.FILE_OPEN,
                CreateOptions.FILE_NON_DIRECTORY_FILE | CreateOptions.FILE_DELETE_ON_CLOSE,
                null);

            if (status == NTStatus.STATUS_SUCCESS)
            {
                fileStore.CloseFile(handle);
            }

            _logger.LogDebug("SMB:  Deleted {Path}", remotePath);
        }
        finally
        {
            client.Disconnect();
        }

        return Task.CompletedTask;
    }

    public Task<bool> FileExistsAsync(string remotePath, CancellationToken ct = default)
    {
        var (client, fileStore) = Connect();

        try
        {
            var normalizedPath = NormalizePath(remotePath);

            var status = fileStore.CreateFile(
                out var handle,
                out _,
                normalizedPath,
                AccessMask.GENERIC_READ,
                FileAttributes.Normal,
                ShareAccess.Read,
                CreateDisposition.FILE_OPEN,
                CreateOptions.FILE_NON_DIRECTORY_FILE,
                null);

            if (status == NTStatus.STATUS_SUCCESS)
            {
                fileStore.CloseFile(handle);
                return Task.FromResult(true);
            }

            return Task.FromResult(false);
        }
        finally
        {
            client.Disconnect();
        }
    }

    public Task CreateDirectoryAsync(string remotePath, CancellationToken ct = default)
    {
        var (client, fileStore) = Connect();

        try
        {
            CreateDirectoryRecursive(fileStore, NormalizePath(remotePath));
        }
        finally
        {
            client.Disconnect();
        }

        return Task.CompletedTask;
    }

    public Task WriteTextFileAsync(string remotePath, string content, CancellationToken ct = default)
    {
        var (client, fileStore) = Connect();

        try
        {
            var normalizedPath = NormalizePath(remotePath);

            // Tạo thư mục nếu cần
            var dir = Path.GetDirectoryName(normalizedPath);
            if (!string.IsNullOrEmpty(dir))
            {
                CreateDirectoryRecursive(fileStore, dir);
            }

            var status = fileStore.CreateFile(
                out var handle,
                out _,
                normalizedPath,
                AccessMask.GENERIC_WRITE,
                FileAttributes.Normal,
                ShareAccess.None,
                CreateDisposition.FILE_OVERWRITE_IF,
                CreateOptions.FILE_NON_DIRECTORY_FILE,
                null);

            if (status != NTStatus.STATUS_SUCCESS)
            {
                throw new Exception($"Cannot create file {remotePath}:  {status}");
            }

            var data = System.Text.Encoding.UTF8.GetBytes(content);
            fileStore.WriteFile(out _, handle, 0, data);
            fileStore.CloseFile(handle);

            _logger.LogDebug("SMB: Written text file {Path}", remotePath);
        }
        finally
        {
            client.Disconnect();
        }

        return Task.CompletedTask;
    }

    #region Private Helpers

    private string NormalizePath(string path)
    {
        if (string.IsNullOrEmpty(path))
            return "";

        // Remove leading slashes và convert forward slash to backslash
        path = path.TrimStart('/', '\\').Replace('/', '\\');

        return path;
    }

    private void CreateDirectoryRecursive(ISMBFileStore fileStore, string path)
    {
        var normalizedPath = NormalizePath(path);
        var parts = normalizedPath.Split('\\', StringSplitOptions.RemoveEmptyEntries);
        var current = "";

        foreach (var part in parts)
        {
            current = string.IsNullOrEmpty(current) ? part : current + "\\" + part;

            var status = fileStore.CreateFile(
                out var handle,
                out _,
                current,
                AccessMask.GENERIC_READ,
                FileAttributes.Directory,
                ShareAccess.Read | ShareAccess.Write,
                CreateDisposition.FILE_OPEN_IF,
                CreateOptions.FILE_DIRECTORY_FILE,
                null);

            if (status == NTStatus.STATUS_SUCCESS)
            {
                fileStore.CloseFile(handle);
            }
        }
    }

    private static bool MatchPattern(string fileName, string pattern)
    {
        if (string.IsNullOrEmpty(pattern) || pattern == "*.*" || pattern == "*")
            return true;

        var patterns = pattern.Split(new[] { ';', '|' }, StringSplitOptions.RemoveEmptyEntries);

        foreach (var p in patterns)
        {
            var trimmedPattern = p.Trim();

            if (trimmedPattern.StartsWith("*."))
            {
                var extension = trimmedPattern.Substring(1);
                if (fileName.EndsWith(extension, StringComparison.OrdinalIgnoreCase))
                    return true;
            }
        }

        return false;
    }

    #endregion
}