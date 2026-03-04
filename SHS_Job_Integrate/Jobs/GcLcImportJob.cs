using System.Data;
using System.Diagnostics;
using Hangfire;
using Microsoft.Extensions.Options;
using SHS_Job_Integrate.Models;
using SHS_Job_Integrate.Services.Database;
using SHS_Job_Integrate.Services.GcLc;

namespace SHS_Job_Integrate.Jobs;

[Queue("gclc-import")]
[AutomaticRetry(Attempts = 0)]
public class GcLcImportJob
{
    private readonly IGcLcFileParser _parser;
    private readonly IHanaDbService _hanaDb;
    private readonly GcLcSettings _settings;
    private readonly GcLcJobSettings _jobSettings;
    private readonly ILogger<GcLcImportJob> _logger;

    public GcLcImportJob(
        IGcLcFileParser parser,
        IHanaDbService hanaDb,
        IOptions<GcLcSettings> settings,
        IOptions<GcLcJobSettings> jobSettings,
        ILogger<GcLcImportJob> logger)
    {
        _parser = parser;
        _hanaDb = hanaDb;
        _settings = settings.Value;
        _jobSettings = jobSettings.Value;
        _logger = logger;
    }

    public async Task ExecuteAsync(CancellationToken ct = default)
    {
        var sw = Stopwatch.StartNew();
        var processedFiles = 0;
        var errorFiles = 0;
        var processedBlocks = 0;

        _logger.LogInformation("========== GC-LC Import Job Started ==========");
        _logger.LogInformation("Scanning folder: {Folder}", _settings.FolderPath);

        try
        {
            if (!Directory.Exists(_settings.FolderPath))
            {
                _logger.LogWarning("GC-LC folder not found: {Folder}", _settings.FolderPath);
                return;
            }

            var patterns = _settings.FilePattern.Split(';', StringSplitOptions.RemoveEmptyEntries);
            var files = patterns
                .SelectMany(p => Directory.GetFiles(_settings.FolderPath, p.Trim(), SearchOption.TopDirectoryOnly))
                .Distinct()
                .OrderBy(f => f)
                .ToList();

            if (files.Count == 0)
            {
                _logger.LogInformation("No GC-LC files found matching pattern {Pattern}", _settings.FilePattern);
                return;
            }

            _logger.LogInformation("Found {Count} GC-LC file(s) to process", files.Count);

            foreach (var filePath in files)
            {
                ct.ThrowIfCancellationRequested();
                var fileName = Path.GetFileName(filePath);
                var fileSw = Stopwatch.StartNew();

                try
                {
                    _logger.LogInformation("Processing GC-LC file: {File}", fileName);

                    // 1. Parse file into blocks
                    var blocks = _parser.ParseFile(filePath).ToList();
                    _logger.LogInformation("Found {Count} data block(s) in {File}", blocks.Count, fileName);

                    if (blocks.Count == 0)
                    {
                        _logger.LogWarning("No data blocks found in {File}, skipping", fileName);
                        MoveToProcessed(filePath, fileName);
                        processedFiles++;
                        continue;
                    }

                    // 2. Process each block
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

                            // 3. Call stored procedure with temp table
                            var rowsAffected = await _hanaDb.ExecuteScalarWithTempTableAsync(
                                data: block.Data,
                                tempTableName: _jobSettings.TempTableName,
                                queryOrProcedure: _jobSettings.ProcedureName,
                                commandType: CommandType.StoredProcedure,
                                ct: ct,
                                // Extra parameters: PrintedDate, SampleName, SampleID
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

                    // 4. Move file based on outcome
                    if (blockErrors == 0)
                    {
                        MoveToProcessed(filePath, fileName);
                        processedFiles++;
                    }
                    else
                    {
                        MoveToError(filePath, fileName, $"{blockErrors} block(s) failed");
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
                        MoveToError(filePath, fileName, ex.Message);
                    }
                    catch (Exception moveEx)
                    {
                        _logger.LogError(moveEx, "Failed to move file to error folder: {File}", fileName);
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

    private void MoveToProcessed(string filePath, string fileName)
    {
        var destFolder = _settings.ProcessedPath;
        if (_settings.UseDateFolder)
            destFolder = Path.Combine(destFolder, DateTime.Today.ToString("yyyy-MM-dd"));

        Directory.CreateDirectory(destFolder);
        var dest = Path.Combine(destFolder, fileName);
        // Handle duplicate file names
        if (File.Exists(dest))
            dest = Path.Combine(destFolder, $"{Path.GetFileNameWithoutExtension(fileName)}_{DateTime.Now:HHmmssfff}{Path.GetExtension(fileName)}");

        File.Move(filePath, dest);
        _logger.LogInformation("Moved to processed: {Dest}", dest);
    }

    private void MoveToError(string filePath, string fileName, string reason)
    {
        var destFolder = _settings.ErrorPath;
        if (_settings.UseDateFolder)
            destFolder = Path.Combine(destFolder, DateTime.Today.ToString("yyyy-MM-dd"));

        Directory.CreateDirectory(destFolder);
        var dest = Path.Combine(destFolder, fileName);
        if (File.Exists(dest))
            dest = Path.Combine(destFolder, $"{Path.GetFileNameWithoutExtension(fileName)}_{DateTime.Now:HHmmssfff}{Path.GetExtension(fileName)}");

        File.Move(filePath, dest);
        _logger.LogWarning("Moved to error: {Dest} (reason: {Reason})", dest, reason);
    }
}
