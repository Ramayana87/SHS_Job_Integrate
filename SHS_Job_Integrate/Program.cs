using Hangfire;
using Hangfire.Dashboard;
using Hangfire.SqlServer;
using Serilog;
using Serilog.Events;
using SHS_Job_Integrate.Jobs;
using SHS_Job_Integrate.Models;
using SHS_Job_Integrate.Services.Archive;
using SHS_Job_Integrate.Services.Database;
using SHS_Job_Integrate.Services.Excel;
using SHS_Job_Integrate.Services.FileTransfer;
using SHS_Job_Integrate.Services.Nir;

// Configure Serilog với filter
Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .MinimumLevel.Override("Microsoft.AspNetCore", LogEventLevel.Warning)
    .MinimumLevel.Override("Hangfire", LogEventLevel.Warning)  // Tắt log Hangfire
    .MinimumLevel.Override("Hangfire.Server", LogEventLevel.Information) // Giữ log job execution
    .Enrich.FromLogContext()
    // Console - chỉ log job
    .WriteTo.Console(
        outputTemplate: "{Timestamp:HH:mm:ss} [{Level:u3}] {Message:lj}{NewLine}{Exception}")
    // File chính - tất cả log quan trọng
    .WriteTo.File(
        path: "logs/app-.log",
        rollingInterval: RollingInterval.Day,
        restrictedToMinimumLevel: LogEventLevel.Information)
    // File riêng cho Jobs - chỉ log từ namespace Jobs
    .WriteTo.Logger(lc => lc
        .Filter.ByIncludingOnly(e =>
            e.Properties.ContainsKey("SourceContext") &&
            e.Properties["SourceContext"].ToString().Contains("Jobs"))
        .WriteTo.File(
            path: "logs/jobs-.log",
            rollingInterval: RollingInterval.Day,
            outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff} [{Level:u3}] {Message: lj}{NewLine}{Exception}"))
    .CreateLogger();

try
{
    var builder = WebApplication.CreateBuilder(args);

    // Use Serilog - đúng cách cho . NET 8
    builder.Host.UseSerilog();

    // Add services
    builder.Services.Configure<FileTransferConfig>(
        builder.Configuration.GetSection("FileTransfer"));

    // Register File Transfer Services
    builder.Services.AddSingleton<SftpFileTransferService>();
    builder.Services.AddSingleton<SmbFileTransferService>();
    builder.Services.AddSingleton<IFileTransferFactory, FileTransferFactory>();

    // Register Archive Service
    builder.Services.AddScoped<IArchiveService, ArchiveService>();

    // Register Excel Service
    builder.Services.AddScoped<IExcelReaderService, ExcelReaderService>();

    // Register NIR Data Transformer
    builder.Services.AddScoped<INirDataTransformer, NirDataTransformer>();

    // Register HANA Database Service
    builder.Services.AddScoped<IHanaDbService>(sp =>
    new HanaDbService(
        builder.Configuration.GetConnectionString("HanaConnection")!,
        sp.GetRequiredService<ILogger<HanaDbService>>()));

    // Register Job
    builder.Services.AddScoped<ExcelImportJob>();
    builder.Services.AddScoped<ArchiveCleanupJob>();

    // Configure Hangfire
    builder.Services.AddHangfire(config => config
        .SetDataCompatibilityLevel(CompatibilityLevel.Version_180)
        .UseSimpleAssemblyNameTypeSerializer()
        .UseRecommendedSerializerSettings()
        .UseSqlServerStorage(
            builder.Configuration.GetConnectionString("HangfireConnection"),
            new SqlServerStorageOptions
            {
                CommandBatchMaxTimeout = TimeSpan.FromMinutes(5),
                SlidingInvisibilityTimeout = TimeSpan.FromMinutes(5),
                QueuePollInterval = TimeSpan.Zero,
                UseRecommendedIsolationLevel = true,
                DisableGlobalLocks = true,
                SchemaName = "Hangfire"
            }));

    // Global filter - no retry
    GlobalJobFilters.Filters.Add(new AutomaticRetryAttribute { Attempts = 0 });

    builder.Services.AddHangfireServer(options =>
    {
        options.WorkerCount = Environment.ProcessorCount * 2;
        options.Queues = new[] { "excel-import", "default" };
    });

    var app = builder.Build();

    // Use Serilog request logging
    app.UseSerilogRequestLogging();

    // Hangfire Dashboard - NO AUTHENTICATION
    app.UseHangfireDashboard("/hangfire", new DashboardOptions
    {
        Authorization = new[] { new AllowAllDashboardAuthorizationFilter() },
        DashboardTitle = "SHS Job Integrate - Dashboard",
        DisplayStorageConnectionString = false
    });

    // Configure recurring jobs
    var jobSettings = builder.Configuration.GetSection("JobSettings");
    if (jobSettings.GetValue<bool>("EnableJob"))
    {
        var cronExpression = jobSettings.GetValue<string>("CronExpression") ?? "0 */15 * * * *";

        RecurringJob.AddOrUpdate<ExcelImportJob>(
            "excel-import-job",
            "excel-import",  // Queue name đặt ở đây (parameter thứ 2)
            job => job.ExecuteAsync(CancellationToken.None),
            cronExpression,
            new RecurringJobOptions
            {
                TimeZone = TimeZoneInfo.Local
            });

        RecurringJob.AddOrUpdate<ArchiveCleanupJob>(
            "archive-cleanup-job",
            job => job.ExecuteAsync(CancellationToken.None),
            "0 0 2 * * *", // Hàng ngày lúc 2 giờ sáng
            new RecurringJobOptions
            {
                TimeZone = TimeZoneInfo.Local
            });
    }

    // Health check endpoint
    app.MapGet("/", () => Results.Ok(new
    {
        Status = "Running",
        Service = "SHS_Job_Integrate",
        Timestamp = DateTime.Now
    }));

    app.MapGet("/health", () => Results.Ok("Healthy"));

    Log.Information("SHS_Job_Integrate started.  Dashboard at /hangfire");

    app.Run();
}
catch (Exception ex)
{
    Log.Fatal(ex, "Application terminated unexpectedly");
}
finally
{
    Log.CloseAndFlush();
}

// Dashboard Authorization Filter - Allow ALL
public class AllowAllDashboardAuthorizationFilter : IDashboardAuthorizationFilter
{
    public bool Authorize(DashboardContext context) => true;
}