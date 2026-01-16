using System.Data;

namespace SHS_Job_Integrate.Services.Excel;

public interface IExcelReaderService
{
    /// <summary>
    /// Đọc file Excel/CSV và trả về DataTable
    /// </summary>
    /// <param name="filePath">Đường dẫn file</param>
    /// <param name="sheetName">Tên sheet (null = sheet đầu tiên)</param>
    /// <param name="headerRow">Dòng chứa header (1-based, mặc định = 1)</param>
    /// <param name="dataStartRow">Dòng bắt đầu data (1-based, mặc định = headerRow + 1)</param>
    /// <param name="ct">CancellationToken</param>
    Task<DataTable> ReadFileAsync(
        string filePath,
        string? sheetName = null,
        int headerRow = 1,
        int? dataStartRow = null,
        CancellationToken ct = default);

    /// <summary>
    /// Lấy danh sách sheet names
    /// </summary>
    Task<List<string>> GetSheetNamesAsync(string filePath, CancellationToken ct = default);

    /// <summary>
    /// Kiểm tra file có được hỗ trợ không
    /// </summary>
    bool IsSupported(string filePath);

    /// <summary>
    /// Lấy danh sách extensions được hỗ trợ
    /// </summary>
    IEnumerable<string> SupportedExtensions { get; }
}