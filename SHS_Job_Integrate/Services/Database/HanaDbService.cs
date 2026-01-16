using System.Data;
using System.Globalization;
using System.Text;
using Sap.Data.Hana;

namespace SHS_Job_Integrate.Services.Database;

public class HanaDbService : IHanaDbService
{
    private readonly string _connectionString;
    private readonly ILogger<HanaDbService>? _logger;
    private HanaConnection? _connection;
    private HanaTransaction? _transaction;
    private bool _disposed;

    public HanaDbService(string connectionString, ILogger<HanaDbService>? logger = null)
    {
        _connectionString = $"{connectionString};Pooling=true;Max Pool Size=50;Min Pool Size=5";
        _logger = logger;
    }

    #region Connection & Transaction

    public async Task<IDbConnection> OpenConnectionAsync(CancellationToken ct = default)
    {
        if (_connection == null)
        {
            _connection = new HanaConnection(_connectionString);
        }

        if (_connection.State == ConnectionState.Closed)
        {
            await _connection.OpenAsync(ct);
            _logger?.LogDebug("HANA connection opened");
        }

        return _connection;
    }

    public IDbTransaction BeginTransaction(IsolationLevel level = IsolationLevel.ReadCommitted)
    {
        if (_connection == null || _connection.State != ConnectionState.Open)
        {
            throw new InvalidOperationException("Connection must be opened before starting a transaction");
        }

        _transaction = _connection.BeginTransaction(level);
        _logger?.LogDebug("Transaction started with isolation level: {Level}", level);
        return _transaction;
    }

    public void Commit()
    {
        _transaction?.Commit();
        _transaction = null;
        _logger?.LogDebug("Transaction committed");
    }

    public void Rollback()
    {
        _transaction?.Rollback();
        _transaction = null;
        _logger?.LogDebug("Transaction rolled back");
    }

    private async Task<HanaConnection> GetConnectionAsync(CancellationToken ct = default)
    {
        await OpenConnectionAsync(ct);
        return _connection!;
    }

    #endregion

    #region Execute Methods

    public async Task<int> ExecuteNonQueryAsync(string query, CommandType commandType = CommandType.Text,
        CancellationToken ct = default, params DbParameter[] parameters)
    {
        var conn = await GetConnectionAsync(ct);

        using var cmd = CreateCommand(conn, query, commandType, parameters);

        var result = await cmd.ExecuteNonQueryAsync(ct);
        _logger?.LogDebug("ExecuteNonQuery:  {Rows} rows affected", result);

        return result;
    }

    public async Task<object?> ExecuteScalarAsync(string query, CommandType commandType = CommandType.Text,
        CancellationToken ct = default, params DbParameter[] parameters)
    {
        var conn = await GetConnectionAsync(ct);

        using var cmd = CreateCommand(conn, query, commandType, parameters);

        var result = await cmd.ExecuteScalarAsync(ct);
        return result == DBNull.Value ? null : result;
    }

    public async Task<T?> ExecuteScalarAsync<T>(string query, CommandType commandType = CommandType.Text,
        CancellationToken ct = default, params DbParameter[] parameters)
    {
        var result = await ExecuteScalarAsync(query, commandType, ct, parameters);

        if (result == null) return default;

        return (T)Convert.ChangeType(result, typeof(T));
    }

    public async Task<DataTable> ExecuteDataTableAsync(string query, CommandType commandType = CommandType.Text,
        CancellationToken ct = default, params DbParameter[] parameters)
    {
        var conn = await GetConnectionAsync(ct);
        var dt = new DataTable();

        using var cmd = CreateCommand(conn, query, commandType, parameters);
        using var adapter = new HanaDataAdapter(cmd);

        adapter.Fill(dt);
        _logger?.LogDebug("ExecuteDataTable: {Rows} rows returned", dt.Rows.Count);

        return dt;
    }

    public async Task<DataSet> ExecuteDataSetAsync(string query, CommandType commandType = CommandType.Text,
        CancellationToken ct = default, params DbParameter[] parameters)
    {
        var conn = await GetConnectionAsync(ct);
        var ds = new DataSet();

        using var cmd = CreateCommand(conn, query, commandType, parameters);
        using var adapter = new HanaDataAdapter(cmd);

        adapter.Fill(ds);

        return ds;
    }

    public async Task<List<T>> ExecuteListAsync<T>(string query, Func<IDataRecord, T> mapper,
        CommandType commandType = CommandType.Text, CancellationToken ct = default,
        params DbParameter[] parameters)
    {
        var conn = await GetConnectionAsync(ct);
        var list = new List<T>();

        using var cmd = CreateCommand(conn, query, commandType, parameters);
        using var reader = await cmd.ExecuteReaderAsync(ct);

        while (await reader.ReadAsync(ct))
        {
            list.Add(mapper(reader));
        }

        return list;
    }

    #endregion

    #region Stored Procedure

    public async Task<int> ExecuteProcedureAsync(string procedureName, CancellationToken ct = default,
        params DbParameter[] parameters)
    {
        var conn = await GetConnectionAsync(ct);

        using var cmd = new HanaCommand(procedureName, conn, _transaction)
        {
            CommandType = CommandType.StoredProcedure,
            CommandTimeout = 60000
        };

        AddParameters(cmd, parameters);

        var result = await cmd.ExecuteNonQueryAsync(ct);
        _logger?.LogInformation("Executed procedure {Name}:  {Result}", procedureName, result);

        return result;
    }

    public async Task<Dictionary<string, object?>> ExecuteProcedureWithOutputAsync(string procedureName,
        CancellationToken ct = default, params DbParameter[] parameters)
    {
        var conn = await GetConnectionAsync(ct);

        using var cmd = new HanaCommand(procedureName, conn, _transaction)
        {
            CommandType = CommandType.StoredProcedure,
            CommandTimeout = 60000
        };

        AddParameters(cmd, parameters);

        await cmd.ExecuteNonQueryAsync(ct);

        // Collect output parameters
        var outputs = new Dictionary<string, object?>();
        foreach (var param in parameters.Where(p =>
            p.Direction == ParameterDirection.Output ||
            p.Direction == ParameterDirection.InputOutput))
        {
            var hanaParam = cmd.Parameters[param.Name];
            outputs[param.Name] = hanaParam.Value == DBNull.Value ? null : hanaParam.Value;
        }

        _logger?.LogInformation("Executed procedure {Name} with {Count} outputs", procedureName, outputs.Count);

        return outputs;
    }

    #endregion

    #region Bulk Operations

    public async Task BulkCopyAsync(DataTable data, string destinationTable, int batchSize = 1000,
        CancellationToken ct = default)
    {
        if (data.Rows.Count == 0)
        {
            _logger?.LogWarning("BulkCopy:  No data to copy");
            return;
        }

        var conn = await GetConnectionAsync(ct);
        var ownTransaction = _transaction == null;

        if (ownTransaction)
        {
            _transaction = conn.BeginTransaction();
        }

        try
        {
            using var bulkCopy = new HanaBulkCopy(conn, HanaBulkCopyOptions.Default, _transaction)
            {
                DestinationTableName = destinationTable,
                BatchSize = batchSize,
                BulkCopyTimeout = 60000
            };

            // Map columns
            foreach (DataColumn col in data.Columns)
            {
                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
            }

            await Task.Run(() => bulkCopy.WriteToServer(data), ct);

            if (ownTransaction)
            {
                _transaction.Commit();
                _transaction = null;
            }

            _logger?.LogInformation("BulkCopy:  {Rows} rows copied to {Table}", data.Rows.Count, destinationTable);
        }
        catch
        {
            if (ownTransaction)
            {
                _transaction?.Rollback();
                _transaction = null;
            }
            throw;
        }
    }

    public async Task BulkCopyAsync(DataSet data, int batchSize = 1000, CancellationToken ct = default)
    {
        var conn = await GetConnectionAsync(ct);
        _transaction = conn.BeginTransaction();

        try
        {
            foreach (DataTable dt in data.Tables)
            {
                ct.ThrowIfCancellationRequested();

                using var bulkCopy = new HanaBulkCopy(conn, HanaBulkCopyOptions.Default, _transaction)
                {
                    DestinationTableName = dt.TableName,
                    BatchSize = batchSize,
                    BulkCopyTimeout = 60000
                };

                foreach (DataColumn col in dt.Columns)
                {
                    bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                }

                await Task.Run(() => bulkCopy.WriteToServer(dt), ct);

                _logger?.LogDebug("BulkCopy: {Rows} rows copied to {Table}", dt.Rows.Count, dt.TableName);
            }

            _transaction.Commit();
            _logger?.LogInformation("BulkCopy: {Count} tables copied", data.Tables.Count);
        }
        catch
        {
            _transaction.Rollback();
            throw;
        }
        finally
        {
            _transaction = null;
        }
    }

    #endregion

    #region Table Operations

    public async Task CreateColumnTableAsync(DataTable schema, string tableName, CancellationToken ct = default)
    {
        var sql = BuildCreateTableSql(schema, tableName, isTemp: false);
        await ExecuteNonQueryAsync(sql, CommandType.Text, ct);
        _logger?.LogInformation("Created column table:  {Table}", tableName);
    }

    public async Task CreateTempTableAsync(DataTable schema, string tableName, CancellationToken ct = default)
    {
        var sql = BuildCreateTableSql(schema, tableName, isTemp: true);
        await ExecuteNonQueryAsync(sql, CommandType.Text, ct);
        _logger?.LogInformation("Created temp table: {Table}", tableName);
    }

    public async Task<string> CreateStagingTableAsync(DataTable schema, string prefix = "STG",
        CancellationToken ct = default)
    {
        var tableName = $"{prefix}_{DateTime.Now:yyyyMMddHHmmss}_{Guid.NewGuid():N}"[..40];
        await CreateColumnTableAsync(schema, tableName, ct);
        return tableName;
    }

    public async Task DropTableAsync(string tableName, CancellationToken ct = default)
    {
        try
        {
            var sql = $"DROP TABLE \"{tableName}\"";
            await ExecuteNonQueryAsync(sql, CommandType.Text, ct);
            _logger?.LogDebug("Dropped table: {Table}", tableName);
        }
        catch (Exception ex)
        {
            _logger?.LogDebug("Drop table {Table} failed (may not exist): {Error}", tableName, ex.Message);
        }
    }

    public async Task AddColumnAsync(string tableName, string columnName, string dataType,
        bool allowNull = true, CancellationToken ct = default)
    {
        var sql = $"ALTER TABLE \"{tableName}\" ADD (\"{columnName}\" {dataType} {(allowNull ? "NULL" : "NOT NULL")})";
        await ExecuteNonQueryAsync(sql, CommandType.Text, ct);
        _logger?.LogDebug("Added column {Column} to {Table}", columnName, tableName);
    }

    public async Task<DataTable> GetTableStructureAsync(string tableName, CancellationToken ct = default)
    {
        const string sql = @"
            SELECT ""COLUMN_NAME"", DEFAULT_VALUE AS COLUMN_DEFAULT, ""IS_NULLABLE"", DATA_TYPE_NAME AS DATA_TYPE 
            FROM TABLE_COLUMNS 
            WHERE SCHEMA_NAME = CURRENT_SCHEMA AND TABLE_NAME = ?  
            ORDER BY POSITION";

        var dt = await ExecuteDataTableAsync(sql, CommandType.Text, ct, new DbParameter("tableName", tableName));

        var result = new DataTable { TableName = tableName };

        foreach (DataRow row in dt.Rows)
        {
            var colName = row["COLUMN_NAME"].ToString() ?? "";
            var dataType = row["DATA_TYPE"].ToString()?.ToLower() ?? "nvarchar";
            var allowNull = row["IS_NULLABLE"].ToString() == "YES";
            var defaultValue = row["COLUMN_DEFAULT"]?.ToString() ?? "";

            var col = new DataColumn(colName)
            {
                DataType = MapHanaTypeToClrType(dataType),
                AllowDBNull = allowNull
            };

            if (!string.IsNullOrEmpty(defaultValue))
            {
                col.DefaultValue = ParseDefaultValue(defaultValue, col.DataType);
            }

            result.Columns.Add(col);
        }

        return result;
    }

    public async Task<int> GetMaxIdAsync(string tableName, string idColumn, CancellationToken ct = default)
    {
        var sql = $"SELECT TOP 1 \"{idColumn}\" FROM \"{tableName}\" ORDER BY \"{idColumn}\" DESC";
        var result = await ExecuteScalarAsync<int?>(sql, CommandType.Text, ct);
        return result ?? 0;
    }

    public async Task CleanupOldStagingTablesAsync(string prefix = "STG", int hoursOld = 24,
        CancellationToken ct = default)
    {
        var sql = $@"
            SELECT TABLE_NAME 
            FROM TABLES 
            WHERE TABLE_NAME LIKE '{prefix}_%' 
            AND CREATE_TIME < ADD_SECONDS(CURRENT_TIMESTAMP, -{hoursOld * 3600})";

        var dt = await ExecuteDataTableAsync(sql, CommandType.Text, ct);
        var count = 0;

        foreach (DataRow row in dt.Rows)
        {
            var tableName = row["TABLE_NAME"].ToString();
            if (!string.IsNullOrEmpty(tableName))
            {
                await DropTableAsync(tableName, ct);
                count++;
            }
        }

        _logger?.LogInformation("Cleaned up {Count} old staging tables", count);
    }

    #endregion

    #region Execute With Temp Table

    /// <summary>
    /// Tạo bảng tạm, bulk copy data, thực thi query/procedure trong 1 transaction
    /// </summary>
    public async Task<int> ExecuteScalarWithTempTableAsync(
        DataTable data,
        string tempTableName,
        string queryOrProcedure,
        CommandType commandType = CommandType.Text,
        CancellationToken ct = default,
        params DbParameter[] parameters)
    {
        var conn = await GetConnectionAsync(ct);
        var ownTransaction = _transaction == null;

        if (ownTransaction)
        {
            _transaction = conn.BeginTransaction();
        }

        try
        {
            // 1. Tạo LOCAL Temporary Table
            await CreateLocalTempTableAsync(conn, data, tempTableName, ct);
            _logger?.LogDebug("Created temp table: #{Table}", tempTableName);

            // 2. Bulk copy data vào bảng tạm
            var rowsCopied = await BulkCopyToTempTableAsync(conn, data, tempTableName, ct);
            _logger?.LogDebug("Copied {Rows} rows to #{Table}", rowsCopied, tempTableName);

            // 3. Thực thi query/procedure
            int result;
            if (commandType == CommandType.StoredProcedure)
            {
                result = await ExecuteProcedureInternalAsync(conn, queryOrProcedure, ct, parameters);
            }
            else
            {
                using var cmd = CreateCommand(conn, queryOrProcedure, commandType, parameters);
                result = await cmd.ExecuteNonQueryAsync(ct);
            }

            // 4. Commit
            if (ownTransaction)
            {
                _transaction.Commit();
                _logger?.LogInformation("ExecuteWithTempTable: Committed.  {Rows} rows processed", result);
            }

            return result;
        }
        catch (Exception ex)
        {
            // Rollback
            if (ownTransaction)
            {
                _transaction?.Rollback();
                _logger?.LogError(ex, "ExecuteWithTempTable: Rolled back due to error");
            }
            throw;
        }
        finally
        {
            if (ownTransaction)
            {
                _transaction = null;
            }

            // Drop temp table (optional - sẽ tự động drop khi session kết thúc)
            await DropLocalTempTableAsync(conn, tempTableName, ct);
        }
    }

    /// <summary>
    /// Tạo bảng tạm, bulk copy data, thực thi SELECT query và trả về DataTable kết quả
    /// </summary>
    public async Task<DataTable> ExecuteWithTempTableAsync(DataTable data, string tempTableName, string queryOrProcedure, CommandType commandType = CommandType.Text, CancellationToken ct = default, params DbParameter[] parameters)
    {
        var conn = await GetConnectionAsync(ct);
        var ownTransaction = _transaction == null;

        if (ownTransaction)
        {
            _transaction = conn.BeginTransaction();
        }

        try
        {
            // 1. Tạo LOCAL Temporary Table
            await CreateLocalTempTableAsync(conn, data, tempTableName, ct);
            _logger?.LogDebug("Created temp table: #{Table}", tempTableName);

            // 2. Bulk copy data vào bảng tạm
            var rowsCopied = await BulkCopyToTempTableAsync(conn, data, tempTableName, ct);
            _logger?.LogDebug("Copied {Rows} rows to #{Table}", rowsCopied, tempTableName);

            // 3. Thực thi SELECT query
            var result = new DataTable();
            using var cmd = CreateCommand(conn, queryOrProcedure, commandType, parameters);
            using var adapter = new HanaDataAdapter(cmd);
            adapter.Fill(result);

            _logger?.LogDebug("ExecuteWithTempTable:  Returned {Rows} rows", result.Rows.Count);

            // 4. Commit
            if (ownTransaction)
            {
                _transaction.Commit();
            }

            return result;
        }
        catch (Exception ex)
        {
            if (ownTransaction)
            {
                _transaction?.Rollback();
                _logger?.LogError(ex, "ExecuteWithTempTable: Rolled back due to error");
            }
            throw;
        }
        finally
        {
            if (ownTransaction)
            {
                _transaction = null;
            }

            await DropLocalTempTableAsync(conn, tempTableName, ct);
        }
    }

    /// <summary>
    /// Tạo bảng tạm, bulk copy data, thực thi procedure với output parameters
    /// </summary>
    public async Task<Dictionary<string, object?>> ExecuteWithTempTableAndOutputAsync(
        DataTable data,
        string tempTableName,
        string procedureName,
        CancellationToken ct = default,
        params DbParameter[] parameters)
    {
        var conn = await GetConnectionAsync(ct);
        var ownTransaction = _transaction == null;

        if (ownTransaction)
        {
            _transaction = conn.BeginTransaction();
        }

        try
        {
            // 1. Tạo LOCAL Temporary Table
            await CreateLocalTempTableAsync(conn, data, tempTableName, ct);
            _logger?.LogDebug("Created temp table: #{Table}", tempTableName);

            // 2. Bulk copy data vào bảng tạm
            var rowsCopied = await BulkCopyToTempTableAsync(conn, data, tempTableName, ct);
            _logger?.LogDebug("Copied {Rows} rows to #{Table}", rowsCopied, tempTableName);

            // 3. Thực thi procedure
            using var cmd = new HanaCommand(procedureName, conn, _transaction)
            {
                CommandType = CommandType.StoredProcedure,
                CommandTimeout = 60000
            };

            AddParameters(cmd, parameters);
            await cmd.ExecuteNonQueryAsync(ct);

            // 4. Collect output parameters
            var outputs = new Dictionary<string, object?>();
            foreach (var param in parameters.Where(p =>
                p.Direction == ParameterDirection.Output ||
                p.Direction == ParameterDirection.InputOutput))
            {
                var hanaParam = cmd.Parameters[param.Name];
                outputs[param.Name] = hanaParam.Value == DBNull.Value ? null : hanaParam.Value;
            }

            // 5. Commit
            if (ownTransaction)
            {
                _transaction.Commit();
                _logger?.LogInformation("ExecuteWithTempTableAndOutput: Committed with {Count} outputs", outputs.Count);
            }

            return outputs;
        }
        catch (Exception ex)
        {
            if (ownTransaction)
            {
                _transaction?.Rollback();
                _logger?.LogError(ex, "ExecuteWithTempTableAndOutput: Rolled back due to error");
            }
            throw;
        }
        finally
        {
            if (ownTransaction)
            {
                _transaction = null;
            }

            await DropLocalTempTableAsync(conn, tempTableName, ct);
        }
    }

    #endregion

    #region Private Temp Table Helpers

    /// <summary>
    /// Tạo LOCAL Temporary Table từ DataTable schema
    /// </summary>
    private async Task CreateLocalTempTableAsync(HanaConnection conn, DataTable schema, string tableName, CancellationToken ct)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"CREATE LOCAL TEMPORARY TABLE \"#{tableName}\" (");

        var columns = new List<string>();
        foreach (DataColumn col in schema.Columns)
        {
            var maxLength = GetMaxColumnLength(schema, col);
            var hanaType = MapClrTypeToHanaType(col.DataType, maxLength);
            columns.Add($"  \"{col.ColumnName}\" {hanaType} NULL");
        }

        sb.AppendLine(string.Join(",\n", columns));
        sb.AppendLine(")");

        using var cmd = new HanaCommand(sb.ToString(), conn, _transaction)
        {
            CommandTimeout = 60000
        };

        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Bulk copy data vào LOCAL Temporary Table
    /// Thử BulkCopy trước, nếu lỗi thì fallback sang INSERT batch
    /// </summary>
    private async Task<int> BulkCopyToTempTableAsync(HanaConnection conn, DataTable data, string tableName, CancellationToken ct)
    {
        if (data.Rows.Count == 0) return 0;

        try
        {
            // Thử dùng HanaBulkCopy trước
            using var bulkCopy = new HanaBulkCopy(conn, HanaBulkCopyOptions.Default, _transaction)
            {
                DestinationTableName = $"\"#{tableName}\"",
                BatchSize = 1000,
                BulkCopyTimeout = 60000
            };

            foreach (DataColumn col in data.Columns)
            {
                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
            }

            await Task.Run(() => bulkCopy.WriteToServer(data), ct);

            _logger?.LogDebug("BulkCopy: {Rows} rows copied to #{Table}", data.Rows.Count, tableName);
            return data.Rows.Count;
        }
        catch (Exception ex)
        {
            // ⚠️ Fallback sang INSERT batch nếu BulkCopy không hỗ trợ temp table
            _logger?.LogWarning(ex, "BulkCopy failed for temp table, falling back to batch INSERT");
            return await BatchInsertToTempTableAsync(conn, data, tableName, ct);
        }
    }

    /// <summary>
    /// Fallback:  INSERT batch với prepared statement
    /// </summary>
    private async Task<int> BatchInsertToTempTableAsync(HanaConnection conn, DataTable data, string tableName, CancellationToken ct)
    {
        var columns = data.Columns.Cast<DataColumn>().Select(c => $"\"{c.ColumnName}\"");
        var columnNames = string.Join(", ", columns);
        var paramPlaceholders = string.Join(", ", Enumerable.Range(0, data.Columns.Count).Select(_ => "?"));

        var sql = $"INSERT INTO \"#{tableName}\" ({columnNames}) VALUES ({paramPlaceholders})";

        using var cmd = new HanaCommand(sql, conn, _transaction)
        {
            CommandTimeout = 60000
        };

        // Prepare parameters once
        foreach (DataColumn col in data.Columns)
        {
            var param = cmd.CreateParameter();
            param.ParameterName = col.ColumnName;
            cmd.Parameters.Add(param);
        }

        var inserted = 0;

        foreach (DataRow row in data.Rows)
        {
            ct.ThrowIfCancellationRequested();

            for (var i = 0; i < data.Columns.Count; i++)
            {
                cmd.Parameters[i].Value = row[i] ?? DBNull.Value;
            }

            inserted += await cmd.ExecuteNonQueryAsync(ct);
        }

        _logger?.LogDebug("BatchInsert: {Rows} rows inserted to #{Table}", inserted, tableName);
        return inserted;
    }

    /// <summary>
    /// Drop LOCAL Temporary Table
    /// </summary>
    private async Task DropLocalTempTableAsync(HanaConnection conn, string tableName, CancellationToken ct)
    {
        try
        {
            using var cmd = new HanaCommand($"DROP TABLE \"#{tableName}\"", conn, _transaction)
            {
                CommandTimeout = 60000
            };
            await cmd.ExecuteNonQueryAsync(ct);
            _logger?.LogDebug("Dropped temp table: #{Table}", tableName);
        }
        catch
        {
            // Ignore - table may not exist or already dropped
        }
    }

    /// <summary>
    /// Execute procedure internal (dùng trong transaction)
    /// </summary>
    private async Task<int> ExecuteProcedureInternalAsync(HanaConnection conn, string procedureName,
        CancellationToken ct, params DbParameter[] parameters)
    {
        using var cmd = new HanaCommand(procedureName, conn, _transaction)
        {
            CommandType = CommandType.StoredProcedure,
            CommandTimeout = 60000
        };

        AddParameters(cmd, parameters);

        return await cmd.ExecuteNonQueryAsync(ct);
    }

    #endregion

    #region Private Helper Methods

    /// <summary>
    /// Tạo HanaCommand - KHÔNG wrap trong anonymous block cho SELECT/query đơn giản
    /// </summary>
    private HanaCommand CreateCommand(HanaConnection conn, string query, CommandType commandType,
        DbParameter[] parameters)
    {
        var cmd = new HanaCommand
        {
            Connection = conn,
            Transaction = _transaction,
            CommandTimeout = 60000,
            CommandType = commandType
        };

        // ✅ KHÔNG wrap trong DO BEGIN END cho queries thông thường
        // Chỉ cần thay thế @param hoặc : param thành ?  cho HANA
        cmd.CommandText = ConvertParameterPlaceholders(query, parameters);

        // Add parameters theo thứ tự
        AddParameters(cmd, parameters);

        return cmd;
    }

    /// <summary>
    /// Chuyển đổi @param hoặc : param thành ? cho HANA
    /// </summary>
    private string ConvertParameterPlaceholders(string query, DbParameter[] parameters)
    {
        var result = query;

        foreach (var param in parameters)
        {
            // Thay thế @paramName hoặc :paramName thành ? 
            result = result.Replace($"@{param.Name}", "?");
            result = result.Replace($":{param.Name}", "?");
        }

        return result;
    }

    /// <summary>
    /// Add parameters vào command theo thứ tự xuất hiện trong query
    /// </summary>
    private void AddParameters(HanaCommand cmd, DbParameter[] parameters)
    {
        foreach (var param in parameters)
        {
            var hanaParam = new HanaParameter
            {
                ParameterName = param.Name,
                Value = param.Value ?? DBNull.Value,
                Direction = param.Direction
            };

            if (param.DbType.HasValue)
            {
                hanaParam.DbType = param.DbType.Value;
            }

            if (param.Size > 0)
            {
                hanaParam.Size = param.Size;
            }

            cmd.Parameters.Add(hanaParam);
        }
    }

    private string BuildCreateTableSql(DataTable schema, string tableName, bool isTemp)
    {
        var sb = new StringBuilder();

        if (isTemp)
        {
            sb.AppendLine($"CREATE LOCAL TEMPORARY TABLE \"{tableName}\" (");
        }
        else
        {
            sb.AppendLine($"CREATE COLUMN TABLE \"{tableName}\" (");
        }

        var columns = new List<string>();
        foreach (DataColumn col in schema.Columns)
        {
            var maxLength = GetMaxColumnLength(schema, col);
            var hanaType = MapClrTypeToHanaType(col.DataType, maxLength);
            var nullability = col.AllowDBNull ? "NULL" : "NOT NULL";
            columns.Add($"  \"{col.ColumnName}\" {hanaType} {nullability}");
        }

        sb.AppendLine(string.Join(",\n", columns));
        sb.AppendLine(")");

        return sb.ToString();
    }

    private int GetMaxColumnLength(DataTable dt, DataColumn col)
    {
        if (col.DataType != typeof(string) || dt.Rows.Count == 0)
            return 0;

        return dt.AsEnumerable()
            .Select(r => r[col]?.ToString()?.Length ?? 0)
            .DefaultIfEmpty(50)
            .Max();
    }

    private string MapClrTypeToHanaType(Type type, int maxLength = 0)
    {
        return type switch
        {
            _ when type == typeof(string) => maxLength > 5000 ? "NCLOB" :
                                             maxLength > 0 ? $"NVARCHAR({Math.Max(maxLength, 50)})" :
                                             "NVARCHAR(5000)",
            _ when type == typeof(char) => "CHAR(1)",
            _ when type == typeof(int) => "INTEGER",
            _ when type == typeof(long) => "BIGINT",
            _ when type == typeof(short) => "SMALLINT",
            _ when type == typeof(byte) => "TINYINT",
            _ when type == typeof(decimal) => "DECIMAL(19,6)",
            _ when type == typeof(double) => "DOUBLE",
            _ when type == typeof(float) => "REAL",
            _ when type == typeof(DateTime) => "TIMESTAMP",
            _ when type == typeof(DateOnly) => "DATE",
            _ when type == typeof(TimeOnly) => "TIME",
            _ when type == typeof(TimeSpan) => "TIME",
            _ when type == typeof(bool) => "BOOLEAN",
            _ when type == typeof(byte[]) => "VARBINARY(5000)",
            _ when type == typeof(Guid) => "NVARCHAR(36)",
            _ => "NVARCHAR(5000)"
        };
    }

    private Type MapHanaTypeToClrType(string hanaType)
    {
        return hanaType.ToLower() switch
        {
            "integer" or "int" => typeof(int),
            "bigint" => typeof(long),
            "smallint" => typeof(short),
            "tinyint" => typeof(byte),
            "decimal" or "smalldecimal" => typeof(decimal),
            "double" or "float" or "real" => typeof(double),
            "date" or "datetime" or "timestamp" => typeof(DateTime),
            "time" => typeof(TimeSpan),
            "boolean" or "bit" => typeof(bool),
            _ => typeof(string)
        };
    }

    private object? ParseDefaultValue(string defaultValue, Type targetType)
    {
        if (string.IsNullOrEmpty(defaultValue))
            return null;

        // Remove parentheses
        var value = defaultValue.Trim('(', ')', '\'');

        try
        {
            if (targetType == typeof(int)) return int.Parse(value);
            if (targetType == typeof(long)) return long.Parse(value);
            if (targetType == typeof(decimal)) return decimal.Parse(value, CultureInfo.InvariantCulture);
            if (targetType == typeof(double)) return double.Parse(value, CultureInfo.InvariantCulture);
            if (targetType == typeof(DateTime)) return DateTime.TryParse(value, out var dt) ? dt : new DateTime(1900, 1, 1);
            if (targetType == typeof(bool)) return bool.Parse(value);
            return value;
        }
        catch
        {
            return null;
        }
    }

    #endregion

    #region Dispose

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (_disposed) return;

        if (disposing)
        {
            _transaction?.Dispose();
            _connection?.Dispose();
        }

        _disposed = true;
    }

    #endregion
}