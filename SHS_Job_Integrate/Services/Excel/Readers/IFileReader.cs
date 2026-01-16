using System.Data;

namespace SHS_Job_Integrate.Services.Excel.Readers;

public interface IFileReader
{
    string[] SupportedExtensions { get; }
    bool CanRead(string filePath);

    /// <summary>
    /// Đọc file với chỉ định dòng header và data
    /// </summary>
    /// <param name="filePath">Đường dẫn file</param>
    /// <param name="sheetName">Tên sheet</param>
    /// <param name="headerRow">Dòng header (1-based)</param>
    /// <param name="dataStartRow">Dòng bắt đầu data (1-based)</param>
    /// <param name="ct">CancellationToken</param>
    Task<DataTable> ReadAsync(
        string filePath,
        string? sheetName = null,
        int headerRow = 1,
        int? dataStartRow = null,
        CancellationToken ct = default);

    Task<List<string>> GetSheetNamesAsync(string filePath, CancellationToken ct = default);
}