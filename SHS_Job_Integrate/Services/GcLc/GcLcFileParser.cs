using System.Data;
using System.Globalization;

namespace SHS_Job_Integrate.Services.GcLc;

public class GcLcBlock
{
    public DateTime PrintedDate { get; set; }
    public string SampleName { get; set; } = "";
    public string SampleId { get; set; } = "";
    public DataTable Data { get; set; } = new DataTable();
}

public interface IGcLcFileParser
{
    IEnumerable<GcLcBlock> ParseFile(string filePath);
}

public class GcLcFileParser : IGcLcFileParser
{
    private readonly ILogger<GcLcFileParser> _logger;

    public GcLcFileParser(ILogger<GcLcFileParser> logger)
    {
        _logger = logger;
    }

    public IEnumerable<GcLcBlock> ParseFile(string filePath)
    {
        var lines = File.ReadAllLines(filePath);
        var blocks = new List<GcLcBlock>();
        var i = 0;

        while (i < lines.Length)
        {
            // Find start of a block
            if (lines[i].Trim() == "Quantify Sample Summary Report")
            {
                var block = TryParseBlock(lines, ref i);
                if (block != null)
                    blocks.Add(block);
            }
            else
            {
                i++;
            }
        }

        _logger.LogInformation("Parsed {Count} blocks from {File}", blocks.Count, Path.GetFileName(filePath));
        return blocks;
    }

    private GcLcBlock? TryParseBlock(string[] lines, ref int i)
    {
        // Line 0: "Quantify Sample Summary Report"
        i++; // skip "Quantify Sample Summary Report"

        // Skip blank lines
        while (i < lines.Length && string.IsNullOrWhiteSpace(lines[i])) i++;

        // Line with "Printed ..."
        if (i >= lines.Length) return null;
        var printedLine = lines[i].Trim();
        i++;

        // Parse date from "Printed Tue Dec 30 09:34:43 2025"
        DateTime printedDate = DateTime.Today;
        if (printedLine.StartsWith("Printed", StringComparison.OrdinalIgnoreCase))
        {
            // Format: "Printed DayOfWeek Mon DD HH:MM:SS YYYY"
            var parts = printedLine.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
            // parts: [Printed, DayOfWeek, Mon, DD, HH:MM:SS, YYYY]
            if (parts.Length >= 6)
            {
                var dateStr = $"{parts[2]} {parts[3]} {parts[5]}"; // "Dec 30 2025"
                if (DateTime.TryParseExact(dateStr, "MMM d yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out var d))
                    printedDate = d;
                else if (DateTime.TryParseExact(dateStr, "MMM dd yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out d))
                    printedDate = d;
                else
                    _logger.LogWarning("Could not parse printed date from '{Line}', defaulting to today", printedLine);
            }
        }

        // Skip blank lines
        while (i < lines.Length && string.IsNullOrWhiteSpace(lines[i])) i++;

        // Line with "Sample Name: ..."
        if (i >= lines.Length) return null;
        var sampleLine = lines[i].Trim();
        i++;

        string sampleName = "";
        string sampleId = "";
        if (sampleLine.StartsWith("Sample Name:", StringComparison.OrdinalIgnoreCase))
        {
            // "Sample Name: 171225 PES Test 01   Sample ID:  12.626"
            var rest = sampleLine.Substring("Sample Name:".Length);
            var sidIdx = rest.IndexOf("Sample ID:", StringComparison.OrdinalIgnoreCase);
            if (sidIdx >= 0)
            {
                sampleName = rest.Substring(0, sidIdx).Trim();
                sampleId = rest.Substring(sidIdx + "Sample ID:".Length).Trim();
            }
            else
            {
                sampleName = rest.Trim();
            }
        }

        // Skip blank lines
        while (i < lines.Length && string.IsNullOrWhiteSpace(lines[i])) i++;

        // Now read the data table header line (tab-separated)
        if (i >= lines.Length) return null;
        var headerLine = lines[i];
        i++;

        var headers = headerLine.Split('\t');
        // Trim each header; skip leading empty header (index col)
        var trimmedHeaders = headers.Select(h => h.Trim()).ToArray();

        var dt = new DataTable();
        foreach (var h in trimmedHeaders)
        {
            var colName = string.IsNullOrEmpty(h) ? "_idx" : h;
            // Avoid duplicate column names
            var safeColName = colName;
            var suffix = 1;
            while (dt.Columns.Contains(safeColName))
            {
                safeColName = $"{colName}_{suffix++}";
            }
            dt.Columns.Add(safeColName, typeof(string));
        }

        // Read data rows until blank line or next "Quantify Sample Summary Report"
        while (i < lines.Length)
        {
            var line = lines[i];
            if (string.IsNullOrWhiteSpace(line)) { i++; break; }
            if (line.Trim() == "Quantify Sample Summary Report") break;

            var cells = line.Split('\t');
            var row = dt.NewRow();
            for (int c = 0; c < dt.Columns.Count && c < cells.Length; c++)
            {
                row[c] = cells[c].Trim();
            }
            dt.Rows.Add(row);
            i++;
        }

        return new GcLcBlock
        {
            PrintedDate = printedDate,
            SampleName = sampleName,
            SampleId = sampleId,
            Data = dt
        };
    }
}
