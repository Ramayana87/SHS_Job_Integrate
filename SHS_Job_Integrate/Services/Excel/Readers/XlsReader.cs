using System.Data;
using System.Text;
using ExcelDataReader;

namespace SHS_Job_Integrate.Services.Excel.Readers;

/// <summary>
/// Reader cho file .xls (Excel 97-2003)
/// </summary>
public class XlsReader : IFileReader
{
    public string[] SupportedExtensions => new[] { ".xls" };

    static XlsReader()
    {
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
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
        using var stream = File.Open(filePath, FileMode.Open, FileAccess.Read);
        using var reader = ExcelReaderFactory.CreateReader(stream);

        // Đọc toàn bộ data KHÔNG dùng header row tự động
        var conf = new ExcelDataSetConfiguration
        {
            ConfigureDataTable = _ => new ExcelDataTableConfiguration
            {
                UseHeaderRow = false  // Không dùng auto header
            }
        };

        var dataSet = reader.AsDataSet(conf);

        DataTable rawTable;
        if (!string.IsNullOrEmpty(sheetName))
        {
            rawTable = dataSet.Tables[sheetName]
                ?? throw new Exception($"Sheet '{sheetName}' not found in {filePath}");
        }
        else
        {
            rawTable = dataSet.Tables[0];
        }

        var actualHeaderRow = headerRow - 1; // 0-based
        var actualDataStartRow = (dataStartRow ?? (headerRow + 1)) - 1; // 0-based

        if (actualHeaderRow >= rawTable.Rows.Count)
            throw new Exception($"Header row {headerRow} is beyond the data range (max:  {rawTable.Rows.Count})");

        var dt = new DataTable();
        dt.TableName = SanitizeName(rawTable.TableName);

        // Tạo column luôn là string
        var headerRowData = rawTable.Rows[actualHeaderRow];
        for (var col = 0; col < rawTable.Columns.Count; col++)
        {
            var headerText = headerRowData[col]?.ToString()?.Trim();
            var columnName = GetUniqueColumnName(dt, headerText, col + 1);
            dt.Columns.Add(columnName, typeof(string));
        }

        AddMetadataColumns(dt);
        var sourceFileName = Path.GetFileName(filePath);
        var importedAt = DateTime.Now;
        var rowNumber = headerRow;

        for (var row = actualDataStartRow; row < rawTable.Rows.Count; row++)
        {
            ct.ThrowIfCancellationRequested();
            rowNumber++;

            var sourceRow = rawTable.Rows[row];
            var hasData = sourceRow.ItemArray.Any(x =>
                x != null && x != DBNull.Value && !string.IsNullOrWhiteSpace(x.ToString()));

            if (!hasData) continue;

            var newRow = dt.NewRow();
            for (var col = 0; col < rawTable.Columns.Count; col++)
            {
                var cellObj = sourceRow[col];
                if (cellObj == null || cellObj == DBNull.Value)
                {
                    newRow[col] = DBNull.Value;
                }
                else if (cellObj is DateTime dtCell)
                {
                    // Format thành chuỗi ISO để lưu nguyên (bạn xử lý convert trong store)
                    newRow[col] = dtCell.ToString("yyyy-MM-ddTHH:mm:ss");
                }
                else
                {
                    var txt = cellObj.ToString()?.Trim();
                    newRow[col] = string.IsNullOrEmpty(txt) ? DBNull.Value : (object)txt;
                }
            }

            newRow["_RowNumber"] = rowNumber;
            newRow["_SourceFile"] = sourceFileName;
            newRow["_ImportedAt"] = importedAt;
            dt.Rows.Add(newRow);
        }

        // Không convert column types ở đây — tất cả giữ string, store sẽ lo convert
        return Task.FromResult(dt);
    }

    public Task<List<string>> GetSheetNamesAsync(string filePath, CancellationToken ct = default)
    {
        using var stream = File.Open(filePath, FileMode.Open, FileAccess.Read);
        using var reader = ExcelReaderFactory.CreateReader(stream);

        var names = new List<string>();
        do
        {
            names.Add(reader.Name);
        } while (reader.NextResult());

        return Task.FromResult(names);
    }

    private void ConvertColumnTypes(DataTable dt)
    {
        var columnsToConvert = new List<(string Name, Type NewType, int Ordinal)>();

        foreach (DataColumn col in dt.Columns)
        {
            if (col.ColumnName.StartsWith("_")) continue;

            var values = dt.AsEnumerable()
                .Select(r => r[col]?.ToString())
                .Where(v => !string.IsNullOrEmpty(v))
                .Take(100)
                .ToList();

            if (values.Count == 0) continue;

            // Check decimal
            if (values.All(v => decimal.TryParse(v, out _)))
            {
                columnsToConvert.Add((col.ColumnName, typeof(decimal), col.Ordinal));
            }
            // Check DateTime
            else if (values.All(v => DateTime.TryParse(v, out _)))
            {
                columnsToConvert.Add((col.ColumnName, typeof(DateTime), col.Ordinal));
            }
        }

        foreach (var (name, newType, ordinal) in columnsToConvert)
        {
            var oldCol = dt.Columns[name]!;
            var newColName = name + "_temp";
            var newCol = dt.Columns.Add(newColName, newType);

            foreach (DataRow row in dt.Rows)
            {
                var value = row[oldCol]?.ToString();
                if (string.IsNullOrEmpty(value))
                {
                    row[newCol] = DBNull.Value;
                }
                else if (newType == typeof(decimal))
                {
                    row[newCol] = decimal.Parse(value);
                }
                else if (newType == typeof(DateTime))
                {
                    row[newCol] = DateTime.Parse(value);
                }
            }

            dt.Columns.Remove(oldCol);
            newCol.ColumnName = name;
            newCol.SetOrdinal(ordinal);
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

        if (string.IsNullOrEmpty(sanitized) || char.IsDigit(sanitized.First()))
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