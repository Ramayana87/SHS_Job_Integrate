namespace SHS_Job_Integrate.Models;

public class ExcelDataRow
{
    public int RowNumber { get; set; }
    public Dictionary<string, object?> Columns { get; set; } = new();
    public string SourceFile { get; set; } = "";
    public DateTime ImportedAt { get; set; } = DateTime.Now;
}

public class ExcelImportResult
{
    public string FileName { get; set; } = "";
    public int TotalRows { get; set; }
    public int SuccessRows { get; set; }
    public int FailedRows { get; set; }
    public List<string> Errors { get; set; } = new();
    public TimeSpan Duration { get; set; }
}