using System.Data;
using System.Diagnostics;
using Hangfire;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using SHS_Job_Integrate.Models;
using SHS_Job_Integrate.Services.Archive;
using SHS_Job_Integrate.Services.Database;
using SHS_Job_Integrate.Services.Excel;
using SHS_Job_Integrate.Services.FileTransfer;
using SHS_Job_Integrate.Services.Nir;

namespace SHS_Job_Integrate.Jobs;

[Queue("excel-import")]
[AutomaticRetry(Attempts = 0)]
public class ExcelImportJob
{
    private readonly IFileTransferFactory _fileTransferFactory;
    private readonly IExcelReaderService _excelReader;
    private readonly IHanaDbService _hanaDb;
    private readonly INirDataTransformer _dataTransformer;
    private readonly IArchiveService _archiveService;
    private readonly FileTransferConfig _config;
    private readonly ILogger<ExcelImportJob> _logger;
    private readonly IConfiguration _configuration;

    public ExcelImportJob(
        IFileTransferFactory fileTransferFactory,
        IExcelReaderService excelReader,
        IHanaDbService hanaDb,
        INirDataTransformer dataTransformer,
        IArchiveService archiveService,
        IOptions<FileTransferConfig> config,
        ILogger<ExcelImportJob> logger,
        IConfiguration configuration)
    {
        _fileTransferFactory = fileTransferFactory;
        _excelReader = excelReader;
        _hanaDb = hanaDb;
        _dataTransformer = dataTransformer;
        _archiveService = archiveService;
        _config = config.Value;
        _logger = logger;
        _configuration = configuration;
    }

    public async Task ExecuteAsync(CancellationToken ct = default)
    {
        var sw = Stopwatch.StartNew();
        var processedCount = 0;
        var errorCount = 0;

        _logger.LogInformation("========== Excel Import Job Started ==========");
        _logger.LogInformation("Mode: {Mode}", _config.Mode);

        try
        {
            var fileTransfer = _fileTransferFactory.GetService();

            if (!fileTransfer.TestConnection())
            {
                throw new Exception($"Cannot connect to {_config.Mode} server");
            }

            var remotePath = _config.Mode.ToUpperInvariant() == "SFTP"
                ? _config.Sftp.RemotePath
                : _config.Smb.RemotePath;

            var files = await fileTransfer.ListFilesAsync(remotePath, _config.FilePattern, ct);

            if (files.Count == 0)
            {
                _logger.LogInformation("No files found matching pattern {Pattern}", _config.FilePattern);
                return;
            }

            _logger.LogInformation("Found {Count} file(s) to process", files.Count);
            Directory.CreateDirectory(_config.LocalTempPath);

            var procedureName = _configuration.GetValue<string>("JobSettings: ProcedureName") ?? "SHS_Job_ImportNirData";

            // File NIR:  Dòng 1 = "Product:  QUE VIETNAM", Dòng 2 = Header
            var headerRow = _configuration.GetValue<int>("NirSettings: HeaderRow", 2);
            var dataStartRow = _configuration.GetValue<int?>("NirSettings: DataStartRow", null);

            foreach (var remoteFile in files)
            {
                ct.ThrowIfCancellationRequested();

                var fileName = Path.GetFileName(remoteFile);
                var fileSw = Stopwatch.StartNew();
                string? localFile = null;
                string? backupPath = null;

                try
                {
                    _logger.LogInformation("Processing: {File}", fileName);

                    // 1. Backup file trước khi xử lý
                    backupPath = await _archiveService.BackupFileAsync(remoteFile, ct);

                    // 2. Download file
                    localFile = await fileTransfer.DownloadFileAsync(
                        remoteFile,
                        _config.LocalTempPath,
                        ct);

                    // 3. Read Excel to DataTable
                    var excelData = await _excelReader.ReadFileAsync(
                        localFile,
                        sheetName: null,
                        headerRow: headerRow,      // Dòng 2 là header
                        dataStartRow: dataStartRow, // null = tự động (headerRow + 1 = dòng 3)
                        ct: ct);
                    _logger.LogInformation("Read {Rows} rows, {Cols} columns",
                        excelData.Rows.Count, excelData.Columns.Count);

                    if (excelData.Rows.Count > 0)
                    {
                        // log ra datatable
                        _logger.LogInformation("DataTable Preview: {@DataTablePreview}", JsonConvert.SerializeObject(excelData));

                        // 4. Transform data:  Ngang -> Dọc (ID, DateG, CharCode, Result)
                        var resultTable = _dataTransformer.TransformToResultTable(excelData);
                        _logger.LogInformation("Transformed to {Rows} result rows", resultTable.Rows.Count);

                        // tesst
                        //DataTable data = await _hanaDb.ExecuteWithTempTableAsync(resultTable, "tempTable", "SELECT * FROM \"#tempTable\"", CommandType.Text);
                        //DataTable data = await _hanaDb.ExecuteWithTempTableAsync(data: resultTable, tempTableName: "TEMP_NIR_DATA", queryOrProcedure: procedureName, CommandType.StoredProcedure);
                        // _logger.LogInformation("Test query data: {@Data}", JsonConvert.SerializeObject(data));

                        // 5. Gọi stored procedure với temp table
                        var rowsAffected = await _hanaDb.ExecuteScalarWithTempTableAsync(data: resultTable, tempTableName: "TEMP_NIR_DATA", queryOrProcedure: procedureName, CommandType.StoredProcedure);
                        _logger.LogInformation("Stored procedure executed, {Rows} rows affected", rowsAffected);
                    }

                    // 6. Move file đến processed folder tạm đóng để debug
                    //await _archiveService.MoveToProcessedAsync(remoteFile, ct);

                    // 7. Cleanup local file
                    _archiveService.CleanupLocalFile(localFile);

                    processedCount++;
                    fileSw.Stop();
                    _logger.LogInformation("✓ Completed {File} in {Duration:F2}s", fileName, fileSw.Elapsed.TotalSeconds);
                }
                catch (Exception ex)
                {
                    errorCount++;
                    _logger.LogError(ex, "✗ Error processing {File}", fileName);

                    // Move file đến error folder
                    try
                    {
                        await _archiveService.MoveToErrorAsync(remoteFile, ex.Message, ct);
                    }
                    catch (Exception moveEx)
                    {
                        _logger.LogError(moveEx, "Failed to move file to error folder: {File}", fileName);
                    }

                    // Cleanup local file nếu có
                    if (!string.IsNullOrEmpty(localFile))
                    {
                        _archiveService.CleanupLocalFile(localFile);
                    }
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Job execution failed");
            throw;
        }
        finally
        {
            sw.Stop();
            _logger.LogInformation("========== Job Completed in {Duration:F2}s ==========", sw.Elapsed.TotalSeconds);
            _logger.LogInformation("Summary: Processed={Processed}, Errors={Errors}", processedCount, errorCount);
        }
    }
}