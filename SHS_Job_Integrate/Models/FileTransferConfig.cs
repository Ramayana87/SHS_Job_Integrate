namespace SHS_Job_Integrate.Models;

public class FileTransferConfig
{
    public string Mode { get; set; } = "SFTP"; // SFTP hoặc SMB
    public string LocalTempPath { get; set; } = "./temp";
    public string FilePattern { get; set; } = "*.xlsx;*.xls;*.csv";

    public ArchiveSettings Archive { get; set; } = new();

    public SftpConfig Sftp { get; set; } = new();
    public SmbConfig Smb { get; set; } = new();
}

public class ArchiveSettings
{
    /// <summary>
    /// Thư mục lưu file đã xử lý thành công
    /// </summary>
    public string ProcessedPath { get; set; } = "archive/processed";

    /// <summary>
    /// Thư mục lưu file xử lý lỗi
    /// </summary>
    public string ErrorPath { get; set; } = "archive/error";

    /// <summary>
    /// Thư mục backup file gốc trước khi xử lý
    /// </summary>
    public string BackupPath { get; set; } = "archive/backup";

    /// <summary>
    /// Tạo subfolder theo ngày (yyyy/MM/dd)
    /// </summary>
    public bool UseDateFolder { get; set; } = true;

    /// <summary>
    /// Giữ bản copy local sau khi xử lý
    /// </summary>
    public bool KeepLocalCopy { get; set; } = false;

    /// <summary>
    /// Ghi đè file nếu trùng tên
    /// </summary>
    public bool OverwriteExisting { get; set; } = false;

    /// <summary>
    /// Số ngày giữ file archive (0 = không tự động xóa)
    /// </summary>
    public int RetentionDays { get; set; } = 30;

    /// <summary>
    /// Backup file gốc trước khi xử lý
    /// </summary>
    public bool BackupBeforeProcess { get; set; } = true;
}

public class SftpConfig
{
    public string Host { get; set; } = "";
    public int Port { get; set; } = 22;
    public string Username { get; set; } = "";
    public string Password { get; set; } = "";
    public string RemotePath { get; set; } = "/data";
}

public class SmbConfig
{
    public string Host { get; set; } = "";
    public string Domain { get; set; } = "";
    public string Username { get; set; } = "";
    public string Password { get; set; } = "";
    public string RemotePath { get; set; } = "data";
}