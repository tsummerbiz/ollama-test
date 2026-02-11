dotnet build

rem dotnet publish -c Release -r win-x64 --self-contained true -p:PublishSingleFile=true -p:DebugType=embedded -o C:\inetpub\wwwroot\JwtAuthApi\publish


dotnet publish -c Release -r win-x64 -o D:\www\apps\JwtAuthApi\publish


pause

