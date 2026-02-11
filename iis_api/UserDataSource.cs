using System.Xml;
using System.Xml.Linq; // ← これを追加
using System.Collections.Concurrent;

public class UserDataSource : IDisposable
{
    private readonly string _xmlPath = @"D:\www\apps\jwtauthapi\auth_data.xml";
    private readonly FileSystemWatcher _watcher;
    // 参照の差し替えだけで済むよう、読み取り専用のDictionaryとして保持
    private IReadOnlyDictionary<string, UserConfig> _cache = new Dictionary<string, UserConfig>();
    private readonly ILogger<UserDataSource> _logger;
    private readonly object _lock = new();

    public record UserConfig(string WindowsId, string Name, string Role, string AppCode);

    public UserDataSource(ILogger<UserDataSource> logger)
    {
        _logger = logger;
        LoadCache();

        _watcher = new FileSystemWatcher(Path.GetDirectoryName(_xmlPath)!, Path.GetFileName(_xmlPath));
        _watcher.NotifyFilter = NotifyFilters.LastWrite;
        _watcher.Changed += OnFileChanged;
        _watcher.EnableRaisingEvents = true;
    }

    private void OnFileChanged(object sender, FileSystemEventArgs e)
    {
        // 簡易的なデバウンス：連続したイベントを避ける
        lock (_lock)
        {
            LoadCache();
        }
    }

    private void LoadCache()
    {
        try
        {
            var newCache = new Dictionary<string, UserConfig>(StringComparer.OrdinalIgnoreCase);

            // FileShare.ReadWrite を指定して、他プロセスが書き込み中でも読み取れるようにする
            using var fs = new FileStream(_xmlPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            using var reader = XmlReader.Create(fs);

            while (reader.Read())
            {
                if (reader.NodeType == XmlNodeType.Element && reader.Name == "User")
                {
                    // 1要素ずつ読み取ることでメモリ消費を最小限にする
                    var element = XElement.ReadFrom(reader) as XElement;
                    if (element != null)
                    {
                        var config = new UserConfig(
                            (string?)element.Element("WindowsID") ?? "",
                            (string?)element.Element("Name") ?? "",
                            (string?)element.Element("Role") ?? "",
                            (string?)element.Element("AppCode") ?? ""
                        );
                        newCache[$"{config.WindowsId}_{config.AppCode}"] = config;
                    }
                }
            }

            // 参照の差し替え（スレッド安全）
            Interlocked.Exchange(ref _cache, newCache);
            _logger.LogInformation("Auth data XML reloaded: {Count} records.", newCache.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load auth_data.xml");
        }
    }

    public UserConfig? GetUser(string windowsId, string appCode)
    {
        var currentCache = _cache; // 現在の参照をローカルに保持
        return currentCache.TryGetValue($"{windowsId}_{appCode}", out var user) ? user : null;
    }

    public void Dispose() => _watcher.Dispose();
}