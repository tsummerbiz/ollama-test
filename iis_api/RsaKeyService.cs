using System.Security.Cryptography;
using Microsoft.IdentityModel.Tokens;

public class RsaKeyService
{
    private readonly string _keyPath;
    private readonly RSA _rsa;
    public RsaSecurityKey SigningKey { get; }
    public string KeyId { get; } = Guid.NewGuid().ToString();

    public RsaKeyService(IConfiguration config)
    {
        _keyPath = @"D:\www\apps\jwtauthapi\AppKeys\jwt_sig_key.pem";
        var directory = Path.GetDirectoryName(_keyPath)!;

        if (!Directory.Exists(directory)) Directory.CreateDirectory(directory);

        _rsa = RSA.Create(2048);

        if (File.Exists(_keyPath))
        {
            var pem = File.ReadAllText(_keyPath);
            _rsa.ImportFromPem(pem);
        }
        else
        {
            var pem = _rsa.ExportPkcs8PrivateKeyPem();
            File.WriteAllText(_keyPath, pem);
        }

        SigningKey = new RsaSecurityKey(_rsa) { KeyId = this.KeyId };
    }

    public object GetPublicJwks()
    {
        var parameters = _rsa.ExportParameters(false);
        return new
        {
            keys = new[]
            {
                new {
                    kty = "RSA",
                    use = "sig",
                    alg = "RS256",
                    kid = KeyId,
                    n = Base64UrlEncoder.Encode(parameters.Modulus),
                    e = Base64UrlEncoder.Encode(parameters.Exponent)
                }
            }
        };
    }
}