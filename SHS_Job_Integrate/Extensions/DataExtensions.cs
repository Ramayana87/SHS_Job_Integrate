using System.Data;
using System.Globalization;

namespace SHS_Job_Integrate.Extensions;

public static class DataExtensions
{
    #region IDataReader Extensions

    public static List<T> ToList<T>(this IDataReader reader, Func<IDataRecord, T> mapper)
    {
        var list = new List<T>();
        while (reader.Read())
        {
            list.Add(mapper(reader));
        }
        return list;
    }

    public static IEnumerable<T> AsEnumerable<T>(this IDataReader reader, Func<IDataRecord, T> mapper)
    {
        while (reader.Read())
        {
            yield return mapper(reader);
        }
    }

    #endregion

    #region Object Parsing Extensions

    public static string ToSafeString(this object? obj)
    {
        return obj?.ToString()?.Trim() ?? string.Empty;
    }

    public static int ToInt(this object? obj, int defaultValue = 0)
    {
        if (obj == null || obj == DBNull.Value) return defaultValue;
        return int.TryParse(obj.ToString(), out var result) ? result : defaultValue;
    }

    public static long ToLong(this object? obj, long defaultValue = 0)
    {
        if (obj == null || obj == DBNull.Value) return defaultValue;
        return long.TryParse(obj.ToString(), out var result) ? result : defaultValue;
    }

    public static decimal ToDecimal(this object? obj, decimal defaultValue = 0)
    {
        if (obj == null || obj == DBNull.Value) return defaultValue;

        var value = obj.ToString();
        if (decimal.TryParse(value, out var result)) return result;
        if (decimal.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out result)) return result;

        return defaultValue;
    }

    public static double ToDouble(this object? obj, double defaultValue = 0)
    {
        if (obj == null || obj == DBNull.Value) return defaultValue;

        if (double.TryParse(obj.ToString(), out var result))
        {
            return double.IsInfinity(result) || double.IsNaN(result) ? defaultValue : result;
        }

        return defaultValue;
    }

    public static double ToDouble(this object? obj, NumberFormatInfo provider, double defaultValue = 0)
    {
        if (obj == null || obj == DBNull.Value) return defaultValue;

        if (double.TryParse(obj.ToString(), NumberStyles.Any, provider, out var result))
        {
            return double.IsInfinity(result) || double.IsNaN(result) ? defaultValue : result;
        }

        return defaultValue;
    }

    public static DateTime? ToDateTime(this object? obj)
    {
        if (obj == null || obj == DBNull.Value) return null;
        return DateTime.TryParse(obj.ToString(), out var result) ? result : null;
    }

    public static DateTime ToDateTime(this object? obj, DateTime defaultValue)
    {
        return obj.ToDateTime() ?? defaultValue;
    }

    public static bool ToBool(this object? obj, bool defaultValue = false)
    {
        if (obj == null || obj == DBNull.Value) return defaultValue;

        if (obj is bool b) return b;
        if (bool.TryParse(obj.ToString(), out var result)) return result;

        // Handle 0/1, Y/N, Yes/No
        var str = obj.ToString()?.ToUpper();
        return str switch
        {
            "1" or "Y" or "YES" or "TRUE" => true,
            "0" or "N" or "NO" or "FALSE" => false,
            _ => defaultValue
        };
    }

    public static Guid ToGuid(this object? obj, Guid defaultValue = default)
    {
        if (obj == null || obj == DBNull.Value) return defaultValue;
        return Guid.TryParse(obj.ToString(), out var result) ? result : defaultValue;
    }

    public static T? ToEnum<T>(this object? obj, T? defaultValue = default) where T : struct, Enum
    {
        if (obj == null || obj == DBNull.Value) return defaultValue;
        return Enum.TryParse<T>(obj.ToString(), true, out var result) ? result : defaultValue;
    }

    #endregion

    #region IDataRecord Extensions

    public static string GetSafeString(this IDataRecord record, string columnName)
    {
        try
        {
            var ordinal = record.GetOrdinal(columnName);
            return record.IsDBNull(ordinal) ? string.Empty : record.GetString(ordinal).Trim();
        }
        catch
        {
            return string.Empty;
        }
    }

    public static int GetSafeInt(this IDataRecord record, string columnName, int defaultValue = 0)
    {
        try
        {
            var ordinal = record.GetOrdinal(columnName);
            return record.IsDBNull(ordinal) ? defaultValue : record.GetInt32(ordinal);
        }
        catch
        {
            return defaultValue;
        }
    }

    public static decimal GetSafeDecimal(this IDataRecord record, string columnName, decimal defaultValue = 0)
    {
        try
        {
            var ordinal = record.GetOrdinal(columnName);
            return record.IsDBNull(ordinal) ? defaultValue : record.GetDecimal(ordinal);
        }
        catch
        {
            return defaultValue;
        }
    }

    public static DateTime? GetSafeDateTime(this IDataRecord record, string columnName)
    {
        try
        {
            var ordinal = record.GetOrdinal(columnName);
            return record.IsDBNull(ordinal) ? null : record.GetDateTime(ordinal);
        }
        catch
        {
            return null;
        }
    }

    public static bool GetSafeBool(this IDataRecord record, string columnName, bool defaultValue = false)
    {
        try
        {
            var ordinal = record.GetOrdinal(columnName);
            return record.IsDBNull(ordinal) ? defaultValue : record.GetBoolean(ordinal);
        }
        catch
        {
            return defaultValue;
        }
    }

    #endregion
}