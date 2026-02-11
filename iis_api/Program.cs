using System.Security.Claims;
using System.IdentityModel.Tokens.Jwt;
using Microsoft.AspNetCore.Authentication.Negotiate;
using Microsoft.IdentityModel.Tokens;
using JwtAuthApi.Logging; // 自作ロガーのNamespace

var builder = WebApplication.CreateBuilder(args);

// --- 標準ロギングの設定 ---
builder.Logging.ClearProviders();
builder.Logging.AddConsole();
builder.Logging.AddSimpleFile(@"D:\www\apps\jwtauthapi\logs");

// Services
builder.Services.AddSingleton<RsaKeyService>();
builder.Services.AddSingleton<UserDataSource>();
builder.Services.AddAuthentication(NegotiateDefaults.AuthenticationScheme).AddNegotiate();
builder.Services.AddAuthorization();

var app = builder.Build();

// JWKS Endpoint
app.MapGet("jwks", (RsaKeyService rsa) => Results.Ok(rsa.GetPublicJwks()));

// Token Issuance Endpoint
// 引数に ILogger<Program> logger を追加
app.MapGet("token", (HttpContext context, string appCode, UserDataSource dataSource, RsaKeyService rsa, ILogger<Program> logger) =>
{
    var user = context.User;
    var windowsId = user.Identity?.Name; // DOMAIN\Username

    if (string.IsNullOrEmpty(windowsId)) return Results.Unauthorized();

    logger.LogWarning("Access denied for user: {WindowsId} with AppCode: {AppCode}", windowsId, appCode);


    var userData = dataSource.GetUser(windowsId, appCode);
    if (userData == null)
    {
        // 修正：logger.LogWarning を使用
        logger.LogWarning("Access denied for user: {WindowsId} with AppCode: {AppCode}", windowsId, appCode);
        return Results.Forbid();
    }

    // JWT 生成
    var claims = new[]
    {
        new Claim(JwtRegisteredClaimNames.Sub, windowsId),
        new Claim(JwtRegisteredClaimNames.Jti, Guid.NewGuid().ToString()),
        new Claim("name", userData.Name),
        new Claim("role", userData.Role),
        new Claim("app_code", appCode)
    };

    var token = new JwtSecurityToken(
        issuer: "JwtAuthApi",
        audience: "Any",
        claims: claims,
        expires: DateTime.UtcNow.AddHours(8),
        signingCredentials: new SigningCredentials(rsa.SigningKey, SecurityAlgorithms.RsaSha256)
    );

    var jwt = new JwtSecurityTokenHandler().WriteToken(token);

    // 修正：logger.LogInformation を使用
    logger.LogInformation("Token issued for: {WindowsId}", windowsId);

    return Results.Ok(new { access_token = jwt, token_type = "Bearer" });
}).RequireAuthorization();

app.Run();