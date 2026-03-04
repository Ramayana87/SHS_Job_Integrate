namespace SHS_Job_Integrate.Models;

public class GcLcSettings
{
    public string FolderPath { get; set; } = "";
    public string FilePattern { get; set; } = "*.txt";
    public string ProcessedPath { get; set; } = "archive/gclc/processed";
    public string ErrorPath { get; set; } = "archive/gclc/error";
    public bool UseDateFolder { get; set; } = true;
}

public class GcLcJobSettings
{
    public string CronExpression { get; set; } = "0 */15 * * * *";
    public bool EnableJob { get; set; } = true;
    public string ProcedureName { get; set; } = "SHS_Job_ImportGcLcData";
    public string TempTableName { get; set; } = "TEMP_GCLC_DATA";
}
