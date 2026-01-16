using System.Data;
using SHS_Job_Integrate.Services.Excel.Readers;

namespace SHS_Job_Integrate.Services.Excel;

public class ExcelReaderService : IExcelReaderService
{
    private readonly ILogger<ExcelReaderService> _logger;
    private readonly List<IFileReader> _readers;

    public ExcelReaderService(ILogger<ExcelReaderService> logger)
    {
        _logger = logger;

        _readers = new List<IFileReader>
        {
            new XlsxReader(),
            new XlsReader(),
            new CsvReader()
        };
    }

    public IEnumerable<string> SupportedExtensions =>
        _readers.SelectMany(r => r.SupportedExtensions).Distinct();

    public bool IsSupported(string filePath)
    {
        return _readers.Any(r => r.CanRead(filePath));
    }

    public async Task<DataTable> ReadFileAsync(
        string filePath,
        string? sheetName = null,
        int headerRow = 1,
        int? dataStartRow = null,
        CancellationToken ct = default)
    {
        if (!File.Exists(filePath))
        {
            throw new FileNotFoundException($"File not found: {filePath}");
        }

        var reader = _readers.FirstOrDefault(r => r.CanRead(filePath));

        if (reader == null)
        {
            var ext = Path.GetExtension(filePath);
            throw new NotSupportedException(
                $"File format '{ext}' is not supported. Supported formats: {string.Join(", ", SupportedExtensions)}");
        }

        _logger.LogInformation("Reading file {File} using {Reader} (Header row: {HeaderRow}, Data start:  {DataStart})",
            Path.GetFileName(filePath), reader.GetType().Name, headerRow, dataStartRow ?? (headerRow + 1));

        var sw = System.Diagnostics.Stopwatch.StartNew();
        var dt = await reader.ReadAsync(filePath, sheetName, headerRow, dataStartRow, ct);
        sw.Stop();

        _logger.LogInformation("Read {Rows} rows, {Cols} columns from {File} in {Duration: F2}ms",
            dt.Rows.Count,
            dt.Columns.Count - 3, // Trừ 3 metadata columns
            Path.GetFileName(filePath),
            sw.ElapsedMilliseconds);

        // Log column names để debug
        var columnNames = dt.Columns.Cast<DataColumn>()
            .Where(c => !c.ColumnName.StartsWith("_"))
            .Select(c => c.ColumnName);
        _logger.LogDebug("Columns: {Columns}", string.Join(", ", columnNames));

        return dt;
    }

    public async Task<List<string>> GetSheetNamesAsync(string filePath, CancellationToken ct = default)
    {
        var reader = _readers.FirstOrDefault(r => r.CanRead(filePath));

        if (reader == null)
        {
            return new List<string>();
        }

        return await reader.GetSheetNamesAsync(filePath, ct);
    }
}