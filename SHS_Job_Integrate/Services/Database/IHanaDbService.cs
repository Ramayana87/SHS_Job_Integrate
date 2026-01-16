using System.Data;

namespace SHS_Job_Integrate.Services.Database;

public interface IHanaDbService : IDisposable
{
    #region Connection & Transaction

    /// <summary>
    /// Mở connection
    /// </summary>
    Task<IDbConnection> OpenConnectionAsync(CancellationToken ct = default);

    /// <summary>
    /// Bắt đầu transaction
    /// </summary>
    IDbTransaction BeginTransaction(IsolationLevel level = IsolationLevel.ReadCommitted);

    /// <summary>
    /// Commit transaction
    /// </summary>
    void Commit();

    /// <summary>
    /// Rollback transaction
    /// </summary>
    void Rollback();

    #endregion

    #region Execute Methods

    /// <summary>
    /// Execute query không trả về data (INSERT, UPDATE, DELETE)
    /// </summary>
    Task<int> ExecuteNonQueryAsync(string query, CommandType commandType = CommandType.Text,
        CancellationToken ct = default, params DbParameter[] parameters);

    /// <summary>
    /// Execute query trả về scalar value
    /// </summary>
    Task<object?> ExecuteScalarAsync(string query, CommandType commandType = CommandType.Text,
        CancellationToken ct = default, params DbParameter[] parameters);

    /// <summary>
    /// Execute query trả về scalar value với generic type
    /// </summary>
    Task<T?> ExecuteScalarAsync<T>(string query, CommandType commandType = CommandType.Text,
        CancellationToken ct = default, params DbParameter[] parameters);

    /// <summary>
    /// Execute query trả về DataTable
    /// </summary>
    Task<DataTable> ExecuteDataTableAsync(string query, CommandType commandType = CommandType.Text,
        CancellationToken ct = default, params DbParameter[] parameters);

    /// <summary>
    /// Execute query trả về DataSet
    /// </summary>
    Task<DataSet> ExecuteDataSetAsync(string query, CommandType commandType = CommandType.Text,
        CancellationToken ct = default, params DbParameter[] parameters);

    /// <summary>
    /// Execute query và map kết quả sang List entity
    /// </summary>
    Task<List<T>> ExecuteListAsync<T>(string query, Func<IDataRecord, T> mapper,
        CommandType commandType = CommandType.Text, CancellationToken ct = default,
        params DbParameter[] parameters);

    #endregion

    #region Stored Procedure

    /// <summary>
    /// Gọi stored procedure
    /// </summary>
    Task<int> ExecuteProcedureAsync(string procedureName, CancellationToken ct = default,
        params DbParameter[] parameters);

    /// <summary>
    /// Gọi stored procedure với output parameters
    /// </summary>
    Task<Dictionary<string, object?>> ExecuteProcedureWithOutputAsync(string procedureName,
        CancellationToken ct = default, params DbParameter[] parameters);

    #endregion

    #region Bulk Operations

    /// <summary>
    /// Bulk copy DataTable vào bảng
    /// </summary>
    Task BulkCopyAsync(DataTable data, string destinationTable, int batchSize = 1000,
        CancellationToken ct = default);

    /// <summary>
    /// Bulk copy DataSet vào các bảng tương ứng
    /// </summary>
    Task BulkCopyAsync(DataSet data, int batchSize = 1000, CancellationToken ct = default);

    #endregion

    #region Table Operations

    /// <summary>
    /// Tạo bảng Column Table từ DataTable schema
    /// </summary>
    Task CreateColumnTableAsync(DataTable schema, string tableName, CancellationToken ct = default);

    /// <summary>
    /// Tạo bảng Temp Table từ DataTable schema
    /// </summary>
    Task CreateTempTableAsync(DataTable schema, string tableName, CancellationToken ct = default);

    /// <summary>
    /// Tạo Staging Table với tên unique
    /// </summary>
    Task<string> CreateStagingTableAsync(DataTable schema, string prefix = "STG", CancellationToken ct = default);

    /// <summary>
    /// Drop bảng
    /// </summary>
    Task DropTableAsync(string tableName, CancellationToken ct = default);

    /// <summary>
    /// Thêm column vào bảng
    /// </summary>
    Task AddColumnAsync(string tableName, string columnName, string dataType, bool allowNull = true,
        CancellationToken ct = default);

    /// <summary>
    /// Lấy structure của bảng
    /// </summary>
    Task<DataTable> GetTableStructureAsync(string tableName, CancellationToken ct = default);

    /// <summary>
    /// Lấy Max ID của bảng
    /// </summary>
    Task<int> GetMaxIdAsync(string tableName, string idColumn, CancellationToken ct = default);

    #endregion

    #region Execute With Temp Table

    /// <summary>
    /// Tạo bảng tạm, bulk copy data, thực thi query/procedure trong 1 transaction
    /// </summary>
    /// <param name="data">DataTable chứa dữ liệu</param>
    /// <param name="tempTableName">Tên bảng tạm (không cần prefix #)</param>
    /// <param name="queryOrProcedure">Query SQL hoặc tên Stored Procedure</param>
    /// <param name="commandType">Text hoặc StoredProcedure</param>
    /// <param name="ct">CancellationToken</param>
    /// <param name="parameters">Các tham số bổ sung (ngoài bảng tạm)</param>
    /// <returns>Số rows affected hoặc kết quả từ procedure</returns>
    Task<int> ExecuteScalarWithTempTableAsync(
        DataTable data,
        string tempTableName,
        string queryOrProcedure,
        CommandType commandType = CommandType.Text,
        CancellationToken ct = default,
        params DbParameter[] parameters);

    /// <summary>
    /// Tạo bảng tạm, bulk copy data, thực thi query và trả về DataTable kết quả
    /// </summary>
    Task<DataTable> ExecuteWithTempTableAsync(DataTable data, string tempTableName, string queryOrProcedure, CommandType commandType = CommandType.Text, CancellationToken ct = default, params DbParameter[] parameters);

    /// <summary>
    /// Tạo bảng tạm, bulk copy data, thực thi procedure với output parameters
    /// </summary>
    Task<Dictionary<string, object?>> ExecuteWithTempTableAndOutputAsync(
        DataTable data,
        string tempTableName,
        string procedureName,
        CancellationToken ct = default,
        params DbParameter[] parameters);

    #endregion

    #region Cleanup

    /// <summary>
    /// Cleanup các staging tables cũ
    /// </summary>
    Task CleanupOldStagingTablesAsync(string prefix = "STG", int hoursOld = 24, CancellationToken ct = default);

    #endregion

}

/// <summary>
/// Database Parameter wrapper để tương thích với cả SqlParameter và HanaParameter
/// </summary>
public class DbParameter
{
    public string Name { get; set; } = "";
    public object? Value { get; set; }
    public DbType? DbType { get; set; }
    public ParameterDirection Direction { get; set; } = ParameterDirection.Input;
    public int Size { get; set; }

    public DbParameter() { }

    public DbParameter(string name, object? value)
    {
        Name = name.StartsWith("@") ? name.Substring(1) : name;
        Value = value;
    }

    public DbParameter(string name, object? value, ParameterDirection direction) : this(name, value)
    {
        Direction = direction;
    }

    public DbParameter(string name, DbType dbType, int size, ParameterDirection direction = ParameterDirection.Output)
    {
        Name = name.StartsWith("@") ? name.Substring(1) : name;
        DbType = dbType;
        Size = size;
        Direction = direction;
    }
}