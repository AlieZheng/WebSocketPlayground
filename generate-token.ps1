# ========================================
# JWT Token Generator for WebSocket Service
# ========================================

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "JWT Token Generator" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

# Configuration (matches your appsettings.json)
$issuer = "your-issuer"
$audience = "your-audience"
$signingKey = "a-string-secret-at-least-256-bits-long"
$userId = "test-user-123"

# Create header
$header = @{
    alg = "HS256"
    typ = "JWT"
} | ConvertTo-Json -Compress

# Create payload
$now = [int][double]::Parse((Get-Date -Date (Get-Date).ToUniversalTime() -UFormat %s))
$exp = $now + (10 * 365 * 24 * 60 * 60) # 10 years from now

$payload = @{
    sub = $userId
    nameidentifier = $userId
    iss = $issuer
    aud = $audience
    iat = $now
    exp = $exp
} | ConvertTo-Json -Compress

Write-Host "Header:" -ForegroundColor Yellow
Write-Host $header -ForegroundColor White
Write-Host ""
Write-Host "Payload:" -ForegroundColor Yellow
Write-Host $payload -ForegroundColor White
Write-Host ""

# Base64Url encode function
function ConvertTo-Base64Url {
    param([string]$text)
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($text)
    $base64 = [Convert]::ToBase64String($bytes)
    $base64url = $base64.Replace('+', '-').Replace('/', '_').TrimEnd('=')
    return $base64url
}

# Encode header and payload
$headerEncoded = ConvertTo-Base64Url -text $header
$payloadEncoded = ConvertTo-Base64Url -text $payload

Write-Host "Encoded Header:" -ForegroundColor Yellow
Write-Host $headerEncoded -ForegroundColor White
Write-Host ""
Write-Host "Encoded Payload:" -ForegroundColor Yellow
Write-Host $payloadEncoded -ForegroundColor White
Write-Host ""

# Create signature
$message = "$headerEncoded.$payloadEncoded"
$hmac = New-Object System.Security.Cryptography.HMACSHA256
$hmac.Key = [System.Text.Encoding]::UTF8.GetBytes($signingKey)
$signatureBytes = $hmac.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($message))
$signatureBase64 = [Convert]::ToBase64String($signatureBytes)
$signatureEncoded = $signatureBase64.Replace('+', '-').Replace('/', '_').TrimEnd('=')

Write-Host "Signature (hex):" -ForegroundColor Yellow
Write-Host ([BitConverter]::ToString($signatureBytes).Replace('-','')) -ForegroundColor White
Write-Host ""

# Final token
$token = "$headerEncoded.$payloadEncoded.$signatureEncoded"

Write-Host "========================================" -ForegroundColor Green
Write-Host "YOUR JWT TOKEN (COPY THIS):" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host $token -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Green
Write-Host ""

Write-Host "Token Details:" -ForegroundColor Cyan
Write-Host "  User ID: $userId" -ForegroundColor White
Write-Host "  Issuer: $issuer" -ForegroundColor White
Write-Host "  Audience: $audience" -ForegroundColor White
Write-Host "  Signing Key: $signingKey" -ForegroundColor White
Write-Host "  Token Length: $($token.Length) characters" -ForegroundColor White
Write-Host ""

Write-Host "Next Steps:" -ForegroundColor Cyan
Write-Host "  1. Copy the token above (the yellow text)" -ForegroundColor White
Write-Host "  2. Open: https://localhost:7144/test-client.html" -ForegroundColor White
Write-Host "  3. Paste the token in the JWT Token field" -ForegroundColor White
Write-Host "  4. Click 'Connect'" -ForegroundColor White
Write-Host ""

Write-Host "✅ This token is signed with your EXACT configuration!" -ForegroundColor Green
Write-Host ""

# Also copy to clipboard if possible
try {
    Set-Clipboard -Value $token
    Write-Host "✅ Token copied to clipboard!" -ForegroundColor Green
    Write-Host ""
} catch {
    Write-Host "⚠️  Could not copy to clipboard automatically" -ForegroundColor Yellow
    Write-Host ""
}

