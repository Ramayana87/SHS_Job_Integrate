using Microsoft.Extensions.Options;
using SHS_Job_Integrate.Models;

namespace SHS_Job_Integrate.Services.FileTransfer;

public class FileTransferFactory : IFileTransferFactory
{
    private readonly FileTransferConfig _config;
    private readonly SftpFileTransferService _sftpService;
    private readonly SmbFileTransferService _smbService;

    public FileTransferFactory(
        IOptions<FileTransferConfig> config,
        SftpFileTransferService sftpService,
        SmbFileTransferService smbService)
    {
        _config = config.Value;
        _sftpService = sftpService;
        _smbService = smbService;
    }

    public IFileTransferService GetService()
    {
        return GetService(_config.Mode);
    }

    public IFileTransferService GetService(string mode)
    {
        return mode.ToUpperInvariant() switch
        {
            "SFTP" => _sftpService,
            "SMB" => _smbService,
            _ => throw new ArgumentException($"Unsupported file transfer mode: {mode}")
        };
    }
}