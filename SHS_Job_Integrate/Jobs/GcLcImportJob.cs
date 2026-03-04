using System.Data;
using System.Diagnostics;
using Hangfire;
using Microsoft.Extensions.Options;
using SHS_Job_Integrate.Models;
using SHS_Job_Integrate.Services.Database;
using SHS_Job_Integrate.Services.FileTransfer;
using SHS_Job_Integrate.Services.GcLc;

namespace SHS_Job_Integrate.Jobs;

[Queue("gclc-import")]
[AutomaticRetry(Attempts = 0)]
public class GcLcImportJob
{
    private readonly IFileTransferFactory _fileTransferFactory;
    private readonly IGcLcFileParser _parser;
    private readonly IHanaDbService _hanaDb;
    private readonly GcLcSettings _settings;
    private readonly GcLcJobSettings _jobSettings;
    private readonly FileTransferConfig _transferConfig;
    private readonly ILogger<GcLcImportJob> _logger;

    public GcLcImportJob(
        IFileTransferFactory fileTransferFactory,
        IGcLcFileParser parser,
        IHanaDbService hanaDb,
        IOptions<GcLcSettings> settings,
        IOptions<GcLcJobSettings> jobSettings,
        IOptions<FileTransferConfig> transferConfig,
        ILogger<GcLcImportJob> logger)
    {
        _fileTransferFactory = fileTransferFactory;
        _parser = parser;
        _hanaDb = hanaDb;
        _settings = settings.Value;
        _jobSettings = jobSettings.Value;
        _transferConfig = transferConfig.Value;
        _logger = logger;
    }

    public async Task ExecuteAsync(CancellationToken ct = default)
    {
        var sw = Stopwatch.StartNew();
        var processedFiles = 0;
        var errorFiles = 0;
        var processedBlocks = 0;

        _logger.LogInformation("========== GC-LC Import Job Started ==========");
        _logger.LogInformation("Mode: {Mode}, Remote folder: {Folder}", _transferConfig.Mode, _settings.RemotePath);

        try
        {
            var fileTransfer = _fileTransferFactory.GetService();

            if (!fileTransfer.TestConnection())
            {
                throw new Exception($"Cannot connect to {_transferConfig.Mode} server");
            }

            var files = await fileTransfer.ListFilesAsync(_settings.RemotePath, _settings.FilePattern, ct);

            if (files.Count == 0)
            {
                _logger.LogInformation("No GC-LC files found matching pattern {Pattern}", _settings.FilePattern);
                return;
            }

            _logger.LogInformation("Found {Count} GC-LC file(s) to process", files.Count);
            Directory.CreateDirectory(_transferConfig.LocalTempPath);

            foreach (var remoteFile in files)
            {
                ct.ThrowIfCancellationRequested();
                var fileName = Path.GetFileName(remoteFile);
                var fileSw = Stopwatch.StartNew();
                string? localFile = null;

                try
                {
                    _logger.LogInformation("Processing GC-LC file: {File}", fileName);

                    // 1. Download file to local temp
                    localFile = await fileTransfer.DownloadFileAsync(remoteFile, _transferConfig.LocalTempPath, ct);

                    // 2. Parse file into blocks
                    var blocks = _parser.ParseFile(localFile).ToList();
                    _logger.LogInformation("Found {Count} data block(s) in {File}", blocks.Count, fileName);

                    if (blocks.Count == 0)
                    {
                        _logger.LogWarning("No data blocks found in {File}, skipping", fileName);
                        await MoveToProcessedAsync(fileTransfer, remoteFile, fileName, ct);
                        processedFiles++;
                        continue;
                    }

                    // 3. Process each block
                    var blockErrors = 0;
                    foreach (var block in blocks)
                    {
                        ct.ThrowIfCancellationRequested();
                        try
                        {
                            _logger.LogInformation(
                                "  Block: SampleName={SampleName}, SampleID={SampleID}, Date={Date}, Rows={Rows}",
                                block.SampleName, block.SampleId, block.PrintedDate.ToString("yyyy-MM-dd"), block.Data.Rows.Count);

                            if (block.Data.Rows.Count == 0)
                            {
                                _logger.LogWarning("  Block for {SampleName} has no data rows, skipping", block.SampleName);
                                continue;
                            }

                            // 4. Call stored procedure with temp table
                            var rowsAffected = await _hanaDb.ExecuteScalarWithTempTableAsync(
                                data: block.Data,
                                tempTableName: _jobSettings.TempTableName,
                                queryOrProcedure: _jobSettings.ProcedureName,
                                commandType: CommandType.StoredProcedure,
                                ct: ct,
                                new DbParameter("PrintedDate", block.PrintedDate.ToString("yyyy-MM-dd")),
                                new DbParameter("SampleName", block.SampleName),
                                new DbParameter("SampleID", block.SampleId)
                            );

                            _logger.LogInformation("  ✓ Block {SampleName} processed, {Rows} rows affected",
                                block.SampleName, rowsAffected);
                            processedBlocks++;
                        }
                        catch (Exception blockEx)
                        {
                            blockErrors++;
                            _logger.LogError(blockEx, "  ✗ Error processing block {SampleName} in {File}",
                                block.SampleName, fileName);
                        }
                    }

                    // 5. Move remote file based on outcome
                    if (blockErrors == 0)
                    {
                        await MoveToProcessedAsync(fileTransfer, remoteFile, fileName, ct);
                        processedFiles++;
                    }
                    else
                    {
                        await MoveToErrorAsync(fileTransfer, remoteFile, fileName, $"{blockErrors} block(s) failed", ct);
                        errorFiles++;
                    }

                    fileSw.Stop();
                    _logger.LogInformation("✓ Completed file {File} in {Duration:F2}s", fileName, fileSw.Elapsed.TotalSeconds);
                }
                catch (Exception ex)
                {
                    errorFiles++;
                    _logger.LogError(ex, "✗ Error processing GC-LC file {File}", fileName);
                    try
                    {
                        await MoveToErrorAsync(fileTransfer, remoteFile, fileName, ex.Message, ct);
                    }
                    catch (Exception moveEx)
                    {
                        _logger.LogError(moveEx, "Failed to move file to error folder: {File}", fileName);
                    }
                }
                finally
                {
                    // Delete local temp file
                    if (!string.IsNullOrEmpty(localFile) && File.Exists(localFile))
                    {
                        try { File.Delete(localFile); } catch { /* ignore */ }
                    }
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "GC-LC Job execution failed");
            throw;
        }
        finally
        {
            sw.Stop();
            _logger.LogInformation("========== GC-LC Job Completed in {Duration:F2}s ==========", sw.Elapsed.TotalSeconds);
            _logger.LogInformation("Summary: Files Processed={Processed}, Files Errors={Errors}, Blocks Processed={Blocks}",
                processedFiles, errorFiles, processedBlocks);
        }
    }

    private async Task MoveToProcessedAsync(IFileTransferService fileTransfer, string remoteFilePath, string fileName, CancellationToken ct)
    {
        var dest = BuildArchivePath(_settings.ProcessedPath, fileName);
        await EnsureRemoteDirectoryAsync(fileTransfer, GetDirectory(dest), ct);
        dest = await GetUniqueRemotePathAsync(fileTransfer, dest, ct);
        await fileTransfer.MoveFileAsync(remoteFilePath, dest, ct);
        _logger.LogInformation("✓ Moved to processed: {Dest}", dest);
    }

    private async Task MoveToErrorAsync(IFileTransferService fileTransfer, string remoteFilePath, string fileName, string reason, CancellationToken ct)
    {
        var dest = BuildArchivePath(_settings.ErrorPath, fileName);
        await EnsureRemoteDirectoryAsync(fileTransfer, GetDirectory(dest), ct);
        dest = await GetUniqueRemotePathAsync(fileTransfer, dest, ct);
        await fileTransfer.MoveFileAsync(remoteFilePath, dest, ct);
        _logger.LogWarning("⚠ Moved to error: {Dest} (reason: {Reason})", dest, reason);
    }

    private string BuildArchivePath(string basePath, string fileName)
    {
        var path = basePath.TrimStart('/', '\\').Replace('\\', '/');
        if (_settings.UseDateFolder)
        {
            var now = DateTime.Now;
            path = $"{path}/{now:yyyy}/{now:MM}/{now:dd}";
        }
        return $"{path}/{fileName}";
    }

    private static string GetDirectory(string path) =>
        path.Contains('/') ? path[..path.LastIndexOf('/')] : "";

    private static async Task EnsureRemoteDirectoryAsync(IFileTransferService fileTransfer, string directoryPath, CancellationToken ct)
    {
        if (!string.IsNullOrEmpty(directoryPath))
        {
            try { await fileTransfer.CreateDirectoryAsync(directoryPath, ct); } catch { /* already exists */ }
        }
    }

    private static async Task<string> GetUniqueRemotePathAsync(IFileTransferService fileTransfer, string filePath, CancellationToken ct)
    {
        if (!await fileTransfer.FileExistsAsync(filePath, ct))
            return filePath;

        var dir = GetDirectory(filePath);
        var nameWithoutExt = Path.GetFileNameWithoutExtension(filePath);
        var extension = Path.GetExtension(filePath);
        var timestamp = DateTime.Now.ToString("yyyyMMddHHmmss");
        return string.IsNullOrEmpty(dir)
            ? $"{nameWithoutExt}_{timestamp}{extension}"
            : $"{dir}/{nameWithoutExt}_{timestamp}{extension}";
    }
}
