using System.Collections.Concurrent;

namespace JwtAuthApi.Logging;

// ロガープロバイダー：ロガーの生成を管理
public class SimpleFileLoggerProvider : ILoggerProvider
{
    private readonly string _logDirectory;
    public SimpleFileLoggerProvider(string logDirectory)
    {
        _logDirectory = logDirectory;
        if (!Directory.Exists(_logDirectory))
        {
            Directory.CreateDirectory(_logDirectory);
        }
    }

    public ILogger CreateLogger(string categoryName) => new SimpleFileLogger(_logDirectory);
    public void Dispose() { }
}

// ロガー本体：実際の書き込み処理
public class SimpleFileLogger : ILogger
{
    private readonly string _logDirectory;
    private static readonly object _lock = new();

    public SimpleFileLogger(string logDirectory) => _logDirectory = logDirectory;

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
    public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        if (!IsEnabled(logLevel)) return;

        var message = formatter(state, exception);
        var logLine = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} [{logLevel}] {message}";
        if (exception != null)
        {
            logLine += Environment.NewLine + exception.ToString();
        }

        // 日次でファイル名を生成
        var fileName = Path.Combine(_logDirectory, $"log-{DateTime.Now:yyyyMMdd}.txt");

        lock (_lock)
        {
            File.AppendAllText(fileName, logLine + Environment.NewLine);
        }
    }
}

// 登録を簡単にするための拡張メソッド
public static class SimpleFileLoggerExtensions
{
    public static ILoggingBuilder AddSimpleFile(this ILoggingBuilder builder, string logDirectory)
    {
        builder.AddProvider(new SimpleFileLoggerProvider(logDirectory));
        return builder;
    }
}