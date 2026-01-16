using System.Data;
using OfficeOpenXml;

namespace SHS_Job_Integrate.Services.Excel.Readers;

/// <summary>
/// Reader cho file . xlsx (Excel 2007+)
/// </summary>
public class XlsxReader : IFileReader
{
    public string[] SupportedExtensions => new[] { ".xlsx", ". xlsm" };

    static XlsxReader()
    {
        ExcelPackage.License.SetNonCommercialOrganization("SHS_Job_Integrate");
    }

    public bool CanRead(string filePath)
    {
        var ext = Path.GetExtension(filePath).ToLowerInvariant();
        return SupportedExtensions.Contains(ext);
    }

    public Task<DataTable> ReadAsync(
        string filePath,
        string? sheetName = null,
        int headerRow = 1,
        int? dataStartRow = null,
        CancellationToken ct = default)
    {
        var dt = new DataTable();

        using var package = new ExcelPackage(new FileInfo(filePath));
        var worksheet = string.IsNullOrEmpty(sheetName)
            ? package.Workbook.Worksheets.First()
            : package.Workbook.Worksheets[sheetName];

        if (worksheet == null)
        {
            throw new Exception($"Sheet '{sheetName}' not found in {filePath}");
        }

        dt.TableName = SanitizeName(worksheet.Name);

        var dimension = worksheet.Dimension;
        if (dimension == null)
        {
            return Task.FromResult(dt);
        }

        // ✅ Sử dụng headerRow và dataStartRow được truyền vào
        var actualHeaderRow = headerRow;
        var actualDataStartRow = dataStartRow ?? (headerRow + 1);
        var endRow = dimension.End.Row;
        var startCol = dimension.Start.Column;
        var endCol = dimension.End.Column;

        // Validate
        if (actualHeaderRow > endRow)
        {
            throw new Exception($"Header row {actualHeaderRow} is beyond the data range (max: {endRow})");
        }

        // Create columns from header row
        for (var col = startCol; col <= endCol; col++)
        {
            var headerText = worksheet.Cells[actualHeaderRow, col].Text?.Trim();
            var columnName = GetUniqueColumnName(dt, headerText, col);
            var columnType = DetectColumnType(worksheet, actualDataStartRow, endRow, col);
            dt.Columns.Add(columnName, columnType);
        }

        AddMetadataColumns(dt);
        var sourceFileName = Path.GetFileName(filePath);
        var importedAt = DateTime.Now;

        // Read data rows
        for (var row = actualDataStartRow; row <= endRow; row++)
        {
            ct.ThrowIfCancellationRequested();

            var dataRow = dt.NewRow();
            var hasData = false;

            for (var col = startCol; col <= endCol; col++)
            {
                var cell = worksheet.Cells[row, col];
                var colIndex = col - startCol;
                var value = GetCellValue(cell, dt.Columns[colIndex].DataType);
                dataRow[colIndex] = value ?? DBNull.Value;

                if (value != null && !string.IsNullOrWhiteSpace(value.ToString()))
                {
                    hasData = true;
                }
            }

            if (hasData)
            {
                dataRow["_RowNumber"] = row;
                dataRow["_SourceFile"] = sourceFileName;
                dataRow["_ImportedAt"] = importedAt;
                dt.Rows.Add(dataRow);
            }
        }

        return Task.FromResult(dt);
    }

    public Task<List<string>> GetSheetNamesAsync(string filePath, CancellationToken ct = default)
    {
        using var package = new ExcelPackage(new FileInfo(filePath));
        var names = package.Workbook.Worksheets.Select(w => w.Name).ToList();
        return Task.FromResult(names);
    }

    private Type DetectColumnType(ExcelWorksheet worksheet, int startRow, int endRow, int col)
    {
        var sampleSize = Math.Min(100, endRow - startRow + 1);
        int stringCount = 0, numCount = 0, dateCount = 0;

        for (var row = startRow; row < startRow + sampleSize && row <= endRow; row++)
        {
            var value = worksheet.Cells[row, col].Value;
            if (value == null) continue;

            var valueType = value.GetType();
            if (valueType == typeof(DateTime)) dateCount++;
            else if (valueType == typeof(double) || valueType == typeof(int) || valueType == typeof(decimal)) numCount++;
            else stringCount++;
        }

        if (stringCount > 0) return typeof(string);
        if (dateCount > numCount) return typeof(DateTime);
        if (numCount > 0) return typeof(decimal);
        return typeof(string);
    }

    private object? GetCellValue(ExcelRange cell, Type targetType)
    {
        if (cell.Value == null) return null;

        try
        {
            if (targetType == typeof(string)) return cell.Text?.Trim();
            if (targetType == typeof(DateTime))
            {
                if (cell.Value is DateTime dt) return dt;
                if (DateTime.TryParse(cell.Text, out var parsed)) return parsed;
                return null;
            }
            if (targetType == typeof(decimal))
            {
                if (cell.Value is double d) return (decimal)d;
                if (decimal.TryParse(cell.Text, out var parsed)) return parsed;
                return null;
            }
            return cell.Value;
        }
        catch
        {
            return cell.Text?.Trim();
        }
    }

    private string GetUniqueColumnName(DataTable dt, string? headerText, int col)
    {
        var columnName = !string.IsNullOrEmpty(headerText)
            ? SanitizeName(headerText)
            : $"Column{col}";

        var finalName = columnName;
        var counter = 1;
        while (dt.Columns.Contains(finalName))
        {
            finalName = $"{columnName}_{counter++}";
        }
        return finalName;
    }

    private string SanitizeName(string name)
    {
        var sanitized = new string(name
            .Replace(" ", "_")
            .Replace("-", "_")
            .Replace("/", "_")
            .Where(c => char.IsLetterOrDigit(c) || c == '_')
            .ToArray());

        if (string.IsNullOrEmpty(sanitized) || char.IsDigit(sanitized.FirstOrDefault()))
        {
            sanitized = "_" + sanitized;
        }
        return sanitized;
    }

    private void AddMetadataColumns(DataTable dt)
    {
        dt.Columns.Add("_RowNumber", typeof(int));
        dt.Columns.Add("_SourceFile", typeof(string));
        dt.Columns.Add("_ImportedAt", typeof(DateTime));
    }
}