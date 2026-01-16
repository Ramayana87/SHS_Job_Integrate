using Microsoft.Extensions.Options;
using Renci.SshNet;
using SHS_Job_Integrate.Models;

namespace SHS_Job_Integrate.Services.FileTransfer;

public class SftpFileTransferService : IFileTransferService
{
    private readonly SftpConfig _config;
    private readonly ILogger<SftpFileTransferService> _logger;

    public SftpFileTransferService(
        IOptions<FileTransferConfig> config,
        ILogger<SftpFileTransferService> logger)
    {
        _config = config.Value.Sftp;
        _logger = logger;
    }

    private SftpClient CreateClient()
    {
        return new SftpClient(_config.Host, _config.Port, _config.Username, _config.Password);
    }

    public bool TestConnection()
    {
        try
        {
            using var client = CreateClient();
            client.Connect();
            var result = client.IsConnected;
            client.Disconnect();
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "SFTP connection test failed");
            return false;
        }
    }

    public Task<List<string>> ListFilesAsync(string remotePath, string pattern, CancellationToken ct = default)
    {
        var files = new List<string>();

        using var client = CreateClient();
        client.Connect();

        try
        {
            var normalizedPath = NormalizePath(remotePath);
            var items = client.ListDirectory(normalizedPath);

            foreach (var item in items)
            {
                if (item.IsDirectory) continue;
                if (item.Name == "." || item.Name == "..") continue;

                if (MatchPattern(item.Name, pattern))
                {
                    files.Add(Path.Combine(normalizedPath, item.Name).Replace('\\', '/'));
                }
            }
        }
        finally
        {
            client.Disconnect();
        }

        return Task.FromResult(files);
    }

    public Task<List<RemoteFileInfo>> ListFilesWithInfoAsync(string remotePath, string pattern, CancellationToken ct = default)
    {
        var files = new List<RemoteFileInfo>();

        using var client = CreateClient();
        client.Connect();

        try
        {
            var normalizedPath = NormalizePath(remotePath);
            ListFilesRecursive(client, normalizedPath, pattern, files);
        }
        finally
        {
            client.Disconnect();
        }

        return Task.FromResult(files);
    }

    private void ListFilesRecursive(SftpClient client, string path, string pattern, List<RemoteFileInfo> files)
    {
        try
        {
            var items = client.ListDirectory(path);

            foreach (var item in items)
            {
                if (item.Name == "." || item.Name == ".. ") continue;

                var fullPath = Path.Combine(path, item.Name).Replace('\\', '/');

                if (item.IsDirectory)
                {
                    ListFilesRecursive(client, fullPath, pattern, files);
                }
                else if (MatchPattern(item.Name, pattern))
                {
                    files.Add(new RemoteFileInfo
                    {
                        FileName = item.Name,
                        FullPath = fullPath,
                        Size = item.Length,
                        LastModified = item.LastWriteTime,
                        IsDirectory = false
                    });
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to list directory: {Path}", path);
        }
    }

    public Task<string> DownloadFileAsync(string remotePath, string localPath, CancellationToken ct = default)
    {
        var localFilePath = Path.Combine(localPath, Path.GetFileName(remotePath));
        Directory.CreateDirectory(localPath);

        using var client = CreateClient();
        client.Connect();

        try
        {
            using var fs = File.Create(localFilePath);
            client.DownloadFile(NormalizePath(remotePath), fs);
        }
        finally
        {
            client.Disconnect();
        }

        _logger.LogInformation("SFTP: Downloaded {Remote} to {Local}", remotePath, localFilePath);
        return Task.FromResult(localFilePath);
    }

    public Task MoveFileAsync(string sourcePath, string destPath, CancellationToken ct = default)
    {
        using var client = CreateClient();
        client.Connect();

        try
        {
            var normalizedSource = NormalizePath(sourcePath);
            var normalizedDest = NormalizePath(destPath);

            // Tạo thư mục đích nếu chưa có
            var destDir = Path.GetDirectoryName(normalizedDest)?.Replace('\\', '/');
            if (!string.IsNullOrEmpty(destDir))
            {
                CreateDirectoryRecursive(client, destDir);
            }

            // Rename/Move file
            client.RenameFile(normalizedSource, normalizedDest);
            _logger.LogDebug("SFTP: Moved {Source} to {Dest}", sourcePath, destPath);
        }
        finally
        {
            client.Disconnect();
        }

        return Task.CompletedTask;
    }

    public Task CopyFileAsync(string sourcePath, string destPath, CancellationToken ct = default)
    {
        using var client = CreateClient();
        client.Connect();

        try
        {
            var normalizedSource = NormalizePath(sourcePath);
            var normalizedDest = NormalizePath(destPath);

            // Tạo thư mục đích
            var destDir = Path.GetDirectoryName(normalizedDest)?.Replace('\\', '/');
            if (!string.IsNullOrEmpty(destDir))
            {
                CreateDirectoryRecursive(client, destDir);
            }

            // Download và Upload lại (SFTP không có native copy)
            using var ms = new MemoryStream();
            client.DownloadFile(normalizedSource, ms);
            ms.Position = 0;
            client.UploadFile(ms, normalizedDest);

            _logger.LogDebug("SFTP: Copied {Source} to {Dest}", sourcePath, destPath);
        }
        finally
        {
            client.Disconnect();
        }

        return Task.CompletedTask;
    }

    public Task DeleteFileAsync(string remotePath, CancellationToken ct = default)
    {
        using var client = CreateClient();
        client.Connect();

        try
        {
            client.DeleteFile(NormalizePath(remotePath));
            _logger.LogDebug("SFTP: Deleted {Path}", remotePath);
        }
        finally
        {
            client.Disconnect();
        }

        return Task.CompletedTask;
    }

    public Task<bool> FileExistsAsync(string remotePath, CancellationToken ct = default)
    {
        using var client = CreateClient();
        client.Connect();

        try
        {
            return Task.FromResult(client.Exists(NormalizePath(remotePath)));
        }
        finally
        {
            client.Disconnect();
        }
    }

    public Task CreateDirectoryAsync(string remotePath, CancellationToken ct = default)
    {
        using var client = CreateClient();
        client.Connect();

        try
        {
            CreateDirectoryRecursive(client, NormalizePath(remotePath));
        }
        finally
        {
            client.Disconnect();
        }

        return Task.CompletedTask;
    }

    public Task WriteTextFileAsync(string remotePath, string content, CancellationToken ct = default)
    {
        using var client = CreateClient();
        client.Connect();

        try
        {
            var normalizedPath = NormalizePath(remotePath);

            // Tạo thư mục nếu cần
            var dir = Path.GetDirectoryName(normalizedPath)?.Replace('\\', '/');
            if (!string.IsNullOrEmpty(dir))
            {
                CreateDirectoryRecursive(client, dir);
            }

            client.WriteAllText(normalizedPath, content);
            _logger.LogDebug("SFTP: Written text file {Path}", remotePath);
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
            return "/";

        // Ensure starts with /
        path = path.Replace('\\', '/');
        if (!path.StartsWith("/"))
            path = "/" + path;

        return path;
    }

    private void CreateDirectoryRecursive(SftpClient client, string path)
    {
        var parts = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
        var current = "";

        foreach (var part in parts)
        {
            current += "/" + part;
            if (!client.Exists(current))
            {
                client.CreateDirectory(current);
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

            if (trimmedPattern.StartsWith("*. "))
            {
                var extension = trimmedPattern.Substring(1);
                if (fileName.EndsWith(extension, StringComparison.OrdinalIgnoreCase))
                    return true;
            }
            else
            {
                var regexPattern = "^" + System.Text.RegularExpressions.Regex.Escape(trimmedPattern)
                    .Replace("\\*", ".*")
                    .Replace("\\?", ". ") + "$";

                if (System.Text.RegularExpressions.Regex.IsMatch(fileName, regexPattern,
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase))
                    return true;
            }
        }

        return false;
    }

    #endregion
}