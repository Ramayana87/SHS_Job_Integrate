using System.Data;
using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;

namespace SHS_Job_Integrate.Services.Excel.Readers;

/// <summary>
/// Reader cho file .csv
/// </summary>
public class CsvReader : IFileReader
{
    public string[] SupportedExtensions => new[] { ".csv", ".txt" };

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
        dt.TableName = SanitizeName(Path.GetFileNameWithoutExtension(filePath));

        var delimiter = DetectDelimiter(filePath);
        var allLines = File.ReadAllLines(filePath);

        if (allLines.Length == 0)
            return Task.FromResult(dt);

        // ✅ Sử dụng headerRow (1-based)
        var actualHeaderRow = headerRow - 1; // Convert to 0-based
        var actualDataStartRow = (dataStartRow ?? (headerRow + 1)) - 1;

        if (actualHeaderRow >= allLines.Length)
        {
            throw new Exception($"Header row {headerRow} is beyond the file (max: {allLines.Length} lines)");
        }

        // Parse header
        var headers = allLines[actualHeaderRow].Split(delimiter);
        foreach (var header in headers)
        {
            var columnName = GetUniqueColumnName(dt, header.Trim(), dt.Columns.Count + 1);
            dt.Columns.Add(columnName, typeof(string));
        }

        AddMetadataColumns(dt);
        var sourceFileName = Path.GetFileName(filePath);
        var importedAt = DateTime.Now;

        // Read data
        for (var i = actualDataStartRow; i < allLines.Length; i++)
        {
            ct.ThrowIfCancellationRequested();

            var line = allLines[i];
            if (string.IsNullOrWhiteSpace(line)) continue;

            var values = line.Split(delimiter);
            var dataRow = dt.NewRow();
            var hasData = false;

            for (var col = 0; col < Math.Min(values.Length, headers.Length); col++)
            {
                var value = values[col]?.Trim();
                dataRow[col] = string.IsNullOrEmpty(value) ? DBNull.Value : value;
                if (!string.IsNullOrEmpty(value)) hasData = true;
            }

            if (hasData)
            {
                dataRow["_RowNumber"] = i + 1;
                dataRow["_SourceFile"] = sourceFileName;
                dataRow["_ImportedAt"] = importedAt;
                dt.Rows.Add(dataRow);
            }
        }

        ConvertColumnTypes(dt);
        return Task.FromResult(dt);
    }

    public Task<List<string>> GetSheetNamesAsync(string filePath, CancellationToken ct = default)
    {
        // CSV không có sheets
        return Task.FromResult(new List<string> { Path.GetFileNameWithoutExtension(filePath) });
    }

    /// <summary>
    /// Auto-detect delimiter (comma, semicolon, tab, pipe)
    /// </summary>
    private string DetectDelimiter(string filePath)
    {
        var delimiters = new[] { ",", ";", "\t", "|" };
        var firstLine = File.ReadLines(filePath).FirstOrDefault() ?? "";

        var counts = delimiters.Select(d => new
        {
            Delimiter = d,
            Count = firstLine.Split(d).Length - 1
        }).ToList();

        return counts.OrderByDescending(x => x.Count).First().Delimiter;
    }

    /// <summary>
    /// Thử convert column types dựa trên data
    /// </summary>
    private void ConvertColumnTypes(DataTable dt)
    {
        var columnsToConvert = new List<(string Name, Type NewType)>();

        foreach (DataColumn col in dt.Columns)
        {
            if (col.ColumnName.StartsWith("_")) continue; // Skip metadata columns

            var values = dt.AsEnumerable()
                .Select(r => r[col]?.ToString())
                .Where(v => !string.IsNullOrEmpty(v))
                .Take(100)
                .ToList();

            if (values.Count == 0) continue;

            // Check if all values are numbers
            if (values.All(v => decimal.TryParse(v, out _)))
            {
                columnsToConvert.Add((col.ColumnName, typeof(decimal)));
            }
            // Check if all values are dates
            else if (values.All(v => DateTime.TryParse(v, out _)))
            {
                columnsToConvert.Add((col.ColumnName, typeof(DateTime)));
            }
        }

        // Convert columns
        foreach (var (name, newType) in columnsToConvert)
        {
            var oldCol = dt.Columns[name]!;
            var ordinal = oldCol.Ordinal;
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
            .Where(c => char.IsLetterOrDigit(c) || c == '_')
            .ToArray());

        if (string.IsNullOrEmpty(sanitized) || char.IsDigit(sanitized.First()))
        {
            sanitized = "_" + sanitized;
        }
        return sanitized.ToUpperInvariant();
    }

    private void AddMetadataColumns(DataTable dt)
    {
        dt.Columns.Add("_RowNumber", typeof(int));
        dt.Columns.Add("_SourceFile", typeof(string));
        dt.Columns.Add("_ImportedAt", typeof(DateTime));
    }
}