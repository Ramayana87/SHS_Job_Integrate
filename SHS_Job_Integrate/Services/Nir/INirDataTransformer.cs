using System.Data;

namespace SHS_Job_Integrate.Services.Nir;

public interface INirDataTransformer
{
    /// <summary>
    /// Transform dữ liệu Excel từ dạng ngang sang dạng dọc
    /// Input:   Sample Name | Date/Time | Comment | CH31 | CH30 | CH04 | CH27 | ... 
    /// Output:  ID | DateG | CharCode | Result
    /// </summary>
    DataTable TransformToResultTable(DataTable excelData);
}

public class NirDataTransformer : INirDataTransformer
{
    private readonly ILogger<NirDataTransformer> _logger;

    // Các cột cố định (không phải chỉ tiêu)
    private static readonly HashSet<string> FixedColumns = new(StringComparer.OrdinalIgnoreCase)
    {
        "Sample Name", "SampleName", "Sample_Name",
        "Date/Time", "DateTime", "Date_Time", "DateG",
        "Comment",
        "Input1", "Input2", "Input3", "Input4", "Input5", "Input6", "Input7", "Input8",
        "prodver",
        "_RowNumber", "_SourceFile", "_ImportedAt"
    };

    public NirDataTransformer(ILogger<NirDataTransformer> logger)
    {
        _logger = logger;
    }

    public DataTable TransformToResultTable(DataTable excelData)
    {
        // Tạo DataTable kết quả
        var resultTable = new DataTable("NirResults");
        resultTable.Columns.Add("ID", typeof(string));           // Sample Name
        resultTable.Columns.Add("DateG", typeof(DateTime));      // Date/Time
        resultTable.Columns.Add("CharCode", typeof(string));     // Mã chỉ tiêu (CH31, CH30, ...)
        resultTable.Columns.Add("Result", typeof(decimal));      // Giá trị

        if (excelData.Rows.Count == 0)
        {
            _logger.LogWarning("Excel data is empty");
            return resultTable;
        }

        // Tìm các cột chỉ tiêu (các cột không nằm trong FixedColumns và có thể parse thành số)
        var charCodeColumns = FindCharCodeColumns(excelData);
        _logger.LogInformation("Found {Count} CharCode columns:  {Columns}",
            charCodeColumns.Count, string.Join(", ", charCodeColumns));

        // Tìm cột Sample Name và Date/Time
        var sampleNameCol = FindColumn(excelData, "Sample Name", "SampleName", "Sample_Name");
        var dateTimeCol = FindColumn(excelData, "Date/Time", "DateTime", "Date_Time", "DateG");

        if (string.IsNullOrEmpty(sampleNameCol))
        {
            throw new Exception("Column 'Sample Name' not found in Excel data");
        }

        _logger.LogDebug("SampleName column: {Col}, DateTime column: {Col2}", sampleNameCol, dateTimeCol);

        // Group by Sample Name, lấy dòng cuối cùng (theo Date/Time hoặc thứ tự xuất hiện)
        var groupedData = GroupBySampleName(excelData, sampleNameCol, dateTimeCol);
        _logger.LogInformation("Grouped into {Count} unique samples", groupedData.Count);

        // Transform từng sample
        foreach (var (sampleName, row) in groupedData)
        {
            var dateTime = ParseDateTime(row, dateTimeCol);

            foreach (var charCode in charCodeColumns)
            {
                var value = row[charCode];

                // Chỉ thêm nếu có giá trị
                if (value != null && value != DBNull.Value)
                {
                    var result = ParseDecimal(value);
                    if (result.HasValue)
                    {
                        var newRow = resultTable.NewRow();
                        newRow["ID"] = sampleName;
                        newRow["DateG"] = dateTime;
                        newRow["CharCode"] = charCode;
                        newRow["Result"] = result.Value;
                        resultTable.Rows.Add(newRow);
                    }
                }
            }
        }

        _logger.LogInformation("Transformed to {Rows} result rows", resultTable.Rows.Count);

        return resultTable;
    }

    /// <summary>
    /// Tìm các cột chứa mã chỉ tiêu (CharCode)
    /// </summary>
    private List<string> FindCharCodeColumns(DataTable dt)
    {
        var charCodeColumns = new List<string>();

        foreach (DataColumn col in dt.Columns)
        {
            var colName = col.ColumnName;

            // Bỏ qua các cột cố định
            if (FixedColumns.Contains(colName))
                continue;

            // Kiểm tra xem cột có chứa dữ liệu số không
            var hasNumericData = dt.AsEnumerable()
                .Take(10) // Check 10 dòng đầu
                .Any(row =>
                {
                    var val = row[col];
                    if (val == null || val == DBNull.Value) return false;
                    return decimal.TryParse(val.ToString(), out _);
                });

            if (hasNumericData)
            {
                charCodeColumns.Add(colName);
            }
        }

        return charCodeColumns;
    }

    /// <summary>
    /// Tìm cột theo nhiều tên có thể có
    /// </summary>
    private string? FindColumn(DataTable dt, params string[] possibleNames)
    {
        foreach (var name in possibleNames)
        {
            if (dt.Columns.Contains(name))
                return name;
        }

        // Tìm theo partial match
        foreach (DataColumn col in dt.Columns)
        {
            foreach (var name in possibleNames)
            {
                if (col.ColumnName.Contains(name, StringComparison.OrdinalIgnoreCase))
                    return col.ColumnName;
            }
        }

        return null;
    }

    /// <summary>
    /// Group by Sample Name, lấy dòng cuối cùng nếu trùng
    /// </summary>
    private Dictionary<string, DataRow> GroupBySampleName(DataTable dt, string sampleNameCol, string? dateTimeCol)
    {
        var result = new Dictionary<string, DataRow>(StringComparer.OrdinalIgnoreCase);

        foreach (DataRow row in dt.Rows)
        {
            var sampleName = row[sampleNameCol]?.ToString()?.Trim();
            if (string.IsNullOrEmpty(sampleName))
                continue;

            // Nếu đã có sample này, so sánh DateTime để lấy dòng mới nhất
            if (result.TryGetValue(sampleName, out var existingRow))
            {
                if (!string.IsNullOrEmpty(dateTimeCol))
                {
                    var existingDate = ParseDateTime(existingRow, dateTimeCol);
                    var newDate = ParseDateTime(row, dateTimeCol);

                    // Lấy dòng có DateTime mới hơn
                    if (newDate > existingDate)
                    {
                        result[sampleName] = row;
                    }
                }
                else
                {
                    // Không có DateTime, lấy dòng cuối cùng
                    result[sampleName] = row;
                }
            }
            else
            {
                result[sampleName] = row;
            }
        }

        return result;
    }

    /// <summary>
    /// Parse DateTime từ DataRow
    /// </summary>
    private DateTime ParseDateTime(DataRow row, string? dateTimeCol)
    {
        if (string.IsNullOrEmpty(dateTimeCol))
            return DateTime.Today;

        var value = row[dateTimeCol];

        if (value == null || value == DBNull.Value)
            return DateTime.Today;

        if (value is DateTime dt)
            return dt;

        if (DateTime.TryParse(value.ToString(), out var parsed))
            return parsed;

        return DateTime.Today;
    }

    /// <summary>
    /// Parse decimal từ object
    /// </summary>
    private decimal? ParseDecimal(object? value)
    {
        if (value == null || value == DBNull.Value)
            return null;

        if (value is decimal d)
            return d;

        if (value is double dbl)
            return (decimal)dbl;

        if (value is float f)
            return (decimal)f;

        if (decimal.TryParse(value.ToString(), out var parsed))
            return parsed;

        return null;
    }
}