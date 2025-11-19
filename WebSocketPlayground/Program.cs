using System.IdentityModel.Tokens.Jwt;
using System.Text;
using KafkaFlow;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.IdentityModel.Tokens;
using StackExchange.Redis;
using WebSocketPlayground.Configuration;
using WebSocketPlayground.Hubs;
using WebSocketPlayground.Services;

var builder = WebApplication.CreateBuilder(args);

// Load configurations
var redisConfig = builder.Configuration.GetSection("RedisConfiguration").Get<RedisConfiguration>() 
    ?? throw new InvalidOperationException("RedisConfiguration not found");
var kafkaConfig = builder.Configuration.GetSection("KafkaConfiguration").Get<KafkaConfiguration>() 
    ?? throw new InvalidOperationException("KafkaConfiguration not found");
var jwtConfig = builder.Configuration.GetSection("JwtConfiguration").Get<JwtConfiguration>() 
    ?? throw new InvalidOperationException("JwtConfiguration not found");
var signalRConfig = builder.Configuration.GetSection("SignalRConfiguration").Get<SignalRConfiguration>() 
    ?? throw new InvalidOperationException("SignalRConfiguration not found");
var timeoutConfig = builder.Configuration.GetSection("TimeoutConfiguration").Get<TimeoutConfiguration>() 
    ?? throw new InvalidOperationException("TimeoutConfiguration not found");

// Register configuration objects as singletons
builder.Services.AddSingleton(redisConfig);
builder.Services.AddSingleton(kafkaConfig);
builder.Services.AddSingleton(jwtConfig);
builder.Services.AddSingleton(signalRConfig);
builder.Services.AddSingleton(timeoutConfig);

// Configure Redis
builder.Services.AddSingleton<IConnectionMultiplexer>(sp =>
{
    var configuration = ConfigurationOptions.Parse(redisConfig.ConnectionString);
    return ConnectionMultiplexer.Connect(configuration);
});

// Enable detailed error logging for JWT
Microsoft.IdentityModel.Logging.IdentityModelEventSource.ShowPII = true;

// Configure JWT Authentication
builder.Services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
    .AddJwtBearer(options =>
    {
        var signingKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(jwtConfig.SigningKey));
        
        options.TokenValidationParameters = new TokenValidationParameters
        {
            ValidateIssuer = true,
            ValidateAudience = true,
            ValidateLifetime = true,
            ValidateIssuerSigningKey = false, // We'll validate manually
            ValidIssuer = jwtConfig.Issuer,
            ValidAudience = jwtConfig.Audience,
            IssuerSigningKey = signingKey,
            RequireSignedTokens = false // We'll validate signature manually
        };
        
        // CRITICAL: Force use of JwtSecurityTokenHandler instead of JsonWebTokenHandler
        options.SecurityTokenValidators.Clear();
        options.SecurityTokenValidators.Add(new JwtSecurityTokenHandler
        {
            MapInboundClaims = false // Don't map claim names (keep as-is)
        });
        
        // .NET 8+ requires this to be explicitly set to true to use SecurityTokenValidators
        options.UseSecurityTokenValidators = true;

        // Enable reading JWT from cookies
        options.Events = new JwtBearerEvents
        {
            OnMessageReceived = context =>
            {
                string? token = null;
                
                // First check cookie
                if (context.Request.Cookies.TryGetValue(jwtConfig.CookieName, out var cookieToken))
                {
                    token = cookieToken;
                }
                // Fallback to query string for SignalR (for non-browser clients or testing)
                else if (context.Request.Query.TryGetValue("access_token", out var queryToken))
                {
                    token = queryToken;
                }

                if (!string.IsNullOrEmpty(token))
                {
                    // Manually validate the signature to bypass kid requirement
                    try
                    {
                        var parts = token.Split('.');
                        if (parts.Length == 3)
                        {
                            var headerAndPayload = parts[0] + "." + parts[1];
                            var signature = Base64UrlEncoder.DecodeBytes(parts[2]);
                            
                            // Compute expected signature
                            using var hmac = new System.Security.Cryptography.HMACSHA256(signingKey.Key);
                            var expectedSignature = hmac.ComputeHash(Encoding.UTF8.GetBytes(headerAndPayload));
                            
                            // Verify signature
                            if (!signature.SequenceEqual(expectedSignature))
                            {
                                var logger = context.HttpContext.RequestServices.GetRequiredService<ILogger<Program>>();
                                logger.LogError("JWT signature validation failed");
                                context.Fail("Invalid token signature");
                                return Task.CompletedTask;
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        var logger = context.HttpContext.RequestServices.GetRequiredService<ILogger<Program>>();
                        logger.LogError(ex, "Error during manual signature validation");
                        context.Fail("Token validation error");
                        return Task.CompletedTask;
                    }
                    
                    context.Token = token;
                }

                return Task.CompletedTask;
            },
            OnAuthenticationFailed = context =>
            {
                var logger = context.HttpContext.RequestServices.GetRequiredService<ILogger<Program>>();
                logger.LogError("JWT Authentication failed: {Exception}", context.Exception.Message);
                return Task.CompletedTask;
            },
            OnTokenValidated = context =>
            {
                var logger = context.HttpContext.RequestServices.GetRequiredService<ILogger<Program>>();
                logger.LogInformation("JWT Token validated successfully for user: {User}", 
                    context.Principal?.Identity?.Name ?? "Unknown");
                return Task.CompletedTask;
            }
        };
    });

builder.Services.AddAuthorization();

// Configure CORS
builder.Services.AddCors(options =>
{
    options.AddPolicy("ClientApp", policy =>
    {
        policy.SetIsOriginAllowed(origin =>
        {
            // Allow localhost, 127.0.0.1, and file:// origins for development
            if (string.IsNullOrEmpty(origin)) return true; // file:// protocol
            var uri = new Uri(origin);
            return uri.Host == "localhost" || 
                   uri.Host == "127.0.0.1" || 
                   uri.Scheme == "file" ||
                   origin.StartsWith("http://localhost") ||
                   origin.StartsWith("https://localhost");
        })
        .AllowCredentials() // Required for cookies!
        .AllowAnyHeader()
        .AllowAnyMethod();
    });
});

// Configure SignalR with Redis backplane
builder.Services.AddSignalR()
    .AddStackExchangeRedis(redisConfig.ConnectionString, options =>
    {
        options.Configuration.ChannelPrefix = RedisChannel.Literal("WebSocketPlayground");
    });

// Register application services
builder.Services.AddSingleton<IConnectionStateManager, ConnectionStateManager>();
builder.Services.AddSingleton<IActivityEventPublisher, ActivityEventPublisher>();
builder.Services.AddSingleton<IScheduledTaskManager, ScheduledTaskManager>();
builder.Services.AddHostedService<ScheduledTaskExecutor>();

// Configure KafkaFlow
builder.Services.AddKafka(kafka => kafka
    .AddCluster(cluster => cluster
        .WithBrokers(new[] { kafkaConfig.BootstrapServers })
        .CreateTopicIfNotExists(kafkaConfig.EventsTopic, 3, 1) // 3 partitions, 1 replica
        .CreateTopicIfNotExists(kafkaConfig.CommandsTopic, 3, 1)
        .AddProducer(
            "activity-events-producer",
            producer => producer
                .DefaultTopic(kafkaConfig.EventsTopic)
                .AddMiddlewares(m => m
                    .AddSerializer<KafkaFlow.Serializer.JsonCoreSerializer>()
                )
                .WithCompression(Confluent.Kafka.CompressionType.Gzip)
                .WithAcks(Acks.All)
        )
        .AddConsumer(consumer => consumer
            .Topic(kafkaConfig.CommandsTopic)
            .WithGroupId(kafkaConfig.ConsumerGroupId)
            .WithBufferSize(100)
            .WithWorkersCount(3)
            .WithAutoOffsetReset(AutoOffsetReset.Latest)
            .AddMiddlewares(middlewares => middlewares
                .AddDeserializer<KafkaFlow.Serializer.JsonCoreDeserializer>()
                .AddTypedHandlers(h => h
                    .AddHandler<EndSessionCommandHandler>()
                )
            )
        )
    )
);

var app = builder.Build();

// Start KafkaFlow bus
var kafkaBus = app.Services.CreateKafkaBus();
await kafkaBus.StartAsync();

// Enable static files for serving test-client.html
app.UseStaticFiles();

app.UseCors("ClientApp");

app.UseAuthentication();
app.UseAuthorization();

app.MapGet("/", () => "Student Activity WebSocket Service is running! Visit /test-client.html to test the connection.");
app.MapGet("/test", () => Results.Redirect("/test-client.html"));

// Token generator endpoint for development
app.MapGet("/generate-token", () =>
{
    var signingKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(jwtConfig.SigningKey));
    var credentials = new SigningCredentials(signingKey, SecurityAlgorithms.HmacSha256);
    
    var claims = new[]
    {
        new System.Security.Claims.Claim(JwtRegisteredClaimNames.Sub, "test-user-123"),
        new System.Security.Claims.Claim("nameidentifier", "test-user-123"),
        new System.Security.Claims.Claim(JwtRegisteredClaimNames.Iss, jwtConfig.Issuer),
        new System.Security.Claims.Claim(JwtRegisteredClaimNames.Aud, jwtConfig.Audience),
        new System.Security.Claims.Claim(JwtRegisteredClaimNames.Iat, DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString())
    };
    
    var token = new JwtSecurityToken(
        issuer: jwtConfig.Issuer,
        audience: jwtConfig.Audience,
        claims: claims,
        expires: DateTime.UtcNow.AddYears(10),
        signingCredentials: credentials
    );
    
    var tokenHandler = new JwtSecurityTokenHandler();
    var tokenString = tokenHandler.WriteToken(token);
    
    return Results.Ok(new
    {
        token = tokenString,
        instructions = "Copy the token above and paste it in the JWT Token field at /test-client.html",
        userId = "test-user-123",
        issuer = jwtConfig.Issuer,
        audience = jwtConfig.Audience,
        expires = token.ValidTo
    });
});

// Map SignalR hub
app.MapHub<StudentActivityHub>(signalRConfig.HubPath);

app.Run();

