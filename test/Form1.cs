using System;
using System.Net.Http;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace WinFormsApp1
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
            // 初期値の設定
            txtBaseUrl.Text = "http://localhost/jwtauth";
            txtAppCode.Text = "APP01";
        }

        private void AppendLog(string text)
        {
            txtLog.AppendText($"[{DateTime.Now:HH:mm:ss}] {text}{Environment.NewLine}");
        }


        private async void btnGetToken_Click(object sender, EventArgs e)
        {
            txtResult.Text = "接続中...";

            // Windows認証を有効にしたHttpClientHandlerの作成
            var handler = new HttpClientHandler()
            {
                // これが重要：ログイン中のWindows資格情報を使用する
                UseDefaultCredentials = true
            };

            using (var client = new HttpClient(handler))
            {
                try
                {
                    string url = $"{txtBaseUrl.Text.TrimEnd('/')}/token?appCode={txtAppCode.Text}";

                    txtResult.AppendText(Environment.NewLine + $"Requesting: {url}");

                    var response = await client.GetAsync(url);

                    if (response.IsSuccessStatusCode)
                    {
                        var json = await response.Content.ReadAsStringAsync();
                        txtResult.Text = $"【成功】トークンを取得しました:" + Environment.NewLine + json;
                    }
                    else
                    {
                        txtResult.Text = $"【失敗】ステータスコード: {response.StatusCode}" + Environment.NewLine +
                                         $"詳細: {await response.Content.ReadAsStringAsync()}";

                        if (response.StatusCode == System.Net.HttpStatusCode.Unauthorized)
                        {
                            txtResult.AppendText(Environment.NewLine + "※Windows認証が拒否されました。サーバーの設定を確認してください。");
                        }
                    }
                }
                catch (Exception ex)
                {
                    txtResult.Text = $"【エラー】{ex.Message}";
                }
            }
        }

        private async void btnGetJwks_Click(object sender, EventArgs e)
        {
            // JWKSは認証不要（コード上）なのでシンプルに叩く
            using (var client = new HttpClient())
            {
                try
                {
                    string url = $"{txtBaseUrl.Text.TrimEnd('/')}/jwks";
                    var response = await client.GetStringAsync(url);
                    txtResult.Text = $"【JWKS公開鍵】" + Environment.NewLine + response;
                }
                catch (Exception ex)
                {
                    txtResult.Text = $"【エラー】{ex.Message}";
                }
            }
        }

        private async void button1_Click(object sender, EventArgs e)
        {
            // 1. txtResultにあるJSONからトークンを取得（簡易的な抽出）
            // 本来は System.Text.Json 等でパースすべきですが、ここでは既存のテキストから抽出すると仮定します。
            string token = ExtractToken(txtResult.Text);

            if (string.IsNullOrEmpty(token))
            {
                txtResult.AppendText(Environment.NewLine + "【エラー】有効なトークンが見つかりません。");
                return;
            }

            using (var client = new HttpClient())
            {
                try
                {
                    // 2. AuthorizationヘッダーにJWTをセット
                    client.DefaultRequestHeaders.Authorization =
                        new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", token);

                    // 3. APIを叩く
                    string url = "http://localhost:8000/login";
                    AppendLog($"Calling API: {url}");

                    var response = await client.PostAsync(url, null); // もしPOSTなら。GETならGetAsync

                    if (response.IsSuccessStatusCode)
                    {
                        var content = await response.Content.ReadAsStringAsync();
                        txtResult.Text = "【API接続成功】" + Environment.NewLine + content;
                    }
                    else
                    {
                        txtResult.Text = $"【API接続失敗】ステータス: {response.StatusCode}" + Environment.NewLine +
                                         await response.Content.ReadAsStringAsync();
                    }
                }
                catch (Exception ex)
                {
                    txtResult.Text = $"【エラー】{ex.Message}";
                }
            }
        }

        // 簡易的なトークン抽出メソッド（JSONの形式に合わせて調整してください）
        private string ExtractToken(string rawText)
        {
            // txtResult.Text に "【成功】... { "token": "xxx" }" と入っている場合を想定
            // 本来は btnGetToken_Click 内で string型変数に保持しておくのがスマートです。
            if (rawText.Contains("eyJ")) // JWTは通常 eyJ で始まります
            {
                int start = rawText.IndexOf("eyJ");
                int end = rawText.IndexOf("\"", start);
                if (end == -1) end = rawText.Length;
                return rawText.Substring(start, end - start).Trim();
            }
            return "";
        }
    }
}