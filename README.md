# WebSocket Playground - Student Activity Monitoring Service

This is a SignalR-based microservice that tracks student assignment attempts in real-time using WebSocket connections.

## Architecture Overview

### Key Features
- **JWT Authentication**: Students are authenticated via JWT tokens (supports both HttpOnly cookies and query string for testing)
- **Manual Signature Validation**: Custom signature validation bypasses `kid` requirement for symmetric keys
- **Single Active Connection Enforcement**: Only one active connection per student+assignment with session switching support
- **Redis-backed State Management**: Distributed connection state using Redis for multi-instance scalability
- **Grace Period Reconnection**: 30-second grace period for network hiccups before marking students offline
- **Kafka Integration**: Fully implemented - publishes activity events and consumes session termination commands
- **Pending Connection Queue**: New connections wait for old sessions to terminate with 10-second timeout

## Project Structure

```
WebSocketPlayground/
├── Configuration/
│   ├── RedisConfiguration.cs          # Redis connection settings
│   ├── KafkaConfiguration.cs          # Kafka broker and topic settings
│   ├── JwtConfiguration.cs            # JWT authentication settings
│   ├── SignalRConfiguration.cs        # SignalR hub path configuration
│   └── TimeoutConfiguration.cs        # Grace period and timeout settings
├── Models/
│   ├── ConnectionState.cs             # Active/pending connection state
│   ├── GracePeriodState.cs            # Disconnection grace period state
│   ├── StudentActivityStartedEvent.cs # Activity start event
│   ├── StudentActivityEndedEvent.cs   # Activity end event
│   └── EndSessionCommand.cs           # Kafka command to terminate sessions
├── Services/
│   ├── IConnectionStateManager.cs     # Connection state management interface
│   ├── ConnectionStateManager.cs      # Redis-backed implementation
│   ├── IActivityEventPublisher.cs     # Event publisher interface
│   ├── ActivityEventPublisher.cs      # Kafka producer (fully implemented)
│   └── KafkaCommandConsumer.cs        # Kafka consumer for commands (fully implemented)
├── Hubs/
│   └── StudentActivityHub.cs          # SignalR hub for student connections
├── Program.cs                         # Application startup and configuration
└── appsettings.json                   # Configuration values

```

## Configuration

Update `appsettings.json` with your environment settings:

### Redis Configuration
```json
"RedisConfiguration": {
  "ConnectionString": "localhost:6379"
}
```

### Kafka Configuration
```json
"KafkaConfiguration": {
  "BootstrapServers": "localhost:9092",
  "CommandsTopic": "student-activity-commands",
  "EventsTopic": "student-activity-events",
  "ConsumerGroupId": "websocket-service"
}
```

### JWT Configuration
```json
"JwtConfiguration": {
  "Issuer": "your-issuer",
  "Audience": "your-audience",
  "SigningKey": "your-super-secret-key-min-32-characters-long",
  "CookieName": "access_token"
}
```

### SignalR Configuration
```json
"SignalRConfiguration": {
  "HubPath": "/hubs/studentActivity"
}
```

### Timeout Configuration
```json
"TimeoutConfiguration": {
  "GracePeriodSeconds": 30,
  "ConflictResolutionTimeoutSeconds": 30
}
```

## Connection Flow

### 1. Student Starts Assignment
```
Client → SignalR Hub: Connect with JWT cookie + query params (assignmentId, attemptId)
Hub → Validates JWT and extracts userId
Hub → Checks for existing active connection from this user
  If exists: Detect duplicate connection
  If not: Check for grace period (reconnection)
Hub → Creates active connection
Hub → Publishes StudentActivityStartedEvent to Kafka
```

### 2. Duplicate Connection Detected (Simplified WebSocket-Only Flow)
```
Client A: Active connection for Assignment 1
Client B: Attempts connection for Assignment 1 (same student)
Hub → Detects existing active connection from same userId
Hub → Creates conflict state in Redis (35s TTL)
Hub → Sends "SessionConflict" message to both Client A and Client B with session details
Hub → Starts 30-second auto-rejection timeout timer
Student → Sees conflict notification on both clients
Student → Chooses which session to keep via WebSocket message
Client → Sends "ResolveSessionConflict" with choice ("KeepNew" or "KeepOld")
Hub → Processes choice:
  If "KeepNew": Terminates Client A, activates Client B
  If "KeepOld": Rejects Client B, keeps Client A active
Hub → Sends "ConflictResolved" to both clients with result
Hub → Cleans up conflict state
Hub → Publishes activity events accordingly

Timeout Scenario:
If no response within 30 seconds → Auto-reject new connection (Client B)
```

### 3. Student Disconnects
```
Client → Disconnects (network issue/browser close)
Hub → OnDisconnectedAsync triggered
Hub → Checks if connection is part of unresolved conflict
  If yes: Auto-resolve (keep whichever connection is still alive)
  If no: Start 30-second grace period
Hub → After grace period: Publish StudentActivityEndedEvent
```

### 4. Student Reconnects During Grace Period
```
Client → Reconnects within 30 seconds
Hub → Detects grace period state in Redis
Hub → Cancels grace period timer
Hub → Reestablishes active connection (no new StartedEvent)
```


## Redis Key Patterns

The service uses the following Redis key patterns for state management:

- `signalr:connection:{attemptId}` - Active connection state
- `signalr:conflict:{userId}` - Conflict state when duplicate connection detected (35s TTL)
- `signalr:grace:{attemptId}` - Grace period state (35s TTL)
- `signalr:user:{userId}:{assignmentId}` - User-to-attempt index

## SignalR Client Messages

### Server → Client Messages

**SessionConflict**: Sent when a duplicate connection is detected
```javascript
connection.on("SessionConflict", (data) => {
  console.log(data.message); // "Existing session detected" or "Another connection attempt detected"
  console.log("Old Attempt:", data.oldAttemptId, "New Attempt:", data.newAttemptId);
  console.log("Is Old Connection:", data.isOldConnection);
  // Show UI to let user choose: "KeepNew" or "KeepOld"
});
```

**ConflictResolved**: Sent when conflict is resolved
```javascript
connection.on("ConflictResolved", (data) => {
  console.log(data.result); // "activated", "active", "rejected", or "terminated"
  console.log(data.message);
  if (data.result === "rejected" || data.result === "terminated") {
    connection.stop();
  }
});
```

**ConflictTimeout**: Sent when conflict resolution times out (30s)
```javascript
connection.on("ConflictTimeout", (data) => {
  console.log(data.message); // "Connection timed out - no response to conflict resolution"
  connection.stop();
});
```

**ConnectionRejected**: Sent when connection is rejected for various reasons
```javascript
connection.on("ConnectionRejected", (reason) => {
  console.log(reason); // "Missing userId in token", "Missing required parameters", etc.
  connection.stop();
});
```

**ForceDisconnect**: Sent when session is terminated by administrative command
```javascript
connection.on("ForceDisconnect", (reason) => {
  console.log(reason); // "Session ended by administrative command"
  connection.stop();
});
```

### Client → Server Messages

**ResolveSessionConflict**: Sent by client to resolve connection conflict
```javascript
// Keep the new connection
await connection.invoke("ResolveSessionConflict", "KeepNew");

// OR keep the old connection
await connection.invoke("ResolveSessionConflict", "KeepOld");
```

## Client Connection Example

### JavaScript/TypeScript (Browser)

```javascript
import * as signalR from "@microsoft/signalr";

// Cookie with JWT is automatically sent
const connection = new signalR.HubConnectionBuilder()
  .withUrl("https://your-server/hubs/studentActivity?assignmentId=123&attemptId=456")
  .withAutomaticReconnect()
  .build();

// Handle session conflict
connection.on("SessionConflict", async (data) => {
  console.log("Session conflict detected:", data);
  
  // Show UI to user asking which session to keep
  const choice = await showConflictDialog(data); // Returns "KeepNew" or "KeepOld"
  
  // Send resolution back to server
  await connection.invoke("ResolveSessionConflict", choice);
});

// Handle conflict resolution result
connection.on("ConflictResolved", (data) => {
  if (data.result === "activated" || data.result === "active") {
    showNotification("Your session is active!");
  } else if (data.result === "rejected" || data.result === "terminated") {
    showNotification("Your session was terminated");
    connection.stop();
  }
});

// Handle conflict timeout
connection.on("ConflictTimeout", (data) => {
  showError("Connection timed out: " + data.message);
  connection.stop();
});

// Handle connection rejection
connection.on("ConnectionRejected", (reason) => {
  showError("Connection rejected: " + reason);
  connection.stop();
});

// Handle administrative force disconnect
connection.on("ForceDisconnect", (reason) => {
  showNotification("Session ended: " + reason);
  connection.stop();
});

// Start connection
await connection.start();
console.log("Connected to Student Activity Hub");

// Cleanup on page unload
window.addEventListener("beforeunload", () => {
  connection.stop();
});
```

## Kafka Integration

The service has **fully implemented** Kafka integration for event-driven architecture:

### Event Publishing (ActivityEventPublisher)

The service publishes two types of events to the `student-activity-events` topic:

**StudentActivityStartedEvent** - Published when a student connects:
```json
{
  "userId": "user-123",
  "assignmentId": "assignment-001",
  "attemptId": "attempt-001",
  "timestamp": "2024-11-17T10:30:00Z"
}
```

**StudentActivityEndedEvent** - Published when grace period expires or student disconnects:
```json
{
  "userId": "user-123",
  "assignmentId": "assignment-001",
  "attemptId": "attempt-001",
  "timestamp": "2024-11-17T10:35:00Z",
  "reason": "Disconnected" | "GracePeriodExpired" | "SwitchedAssignment"
}
```

### Producer Configuration
- **Idempotence enabled**: Guarantees exactly-once delivery
- **Acks = All**: Ensures message is written to all in-sync replicas
- **Snappy compression**: Reduces network bandwidth
- **Automatic retries**: 3 retry attempts on failure

### Command Consumption (KafkaCommandConsumer)

The service consumes `EndSessionCommand` messages from the `student-activity-commands` topic.

**IMPORTANT**: `EndSessionCommand` is now **ONLY used for administrative forced logouts**, not for duplicate session resolution. Duplicate sessions are handled entirely within the WebSocket/SignalR flow via the SessionConflict mechanism.

**Use cases for EndSessionCommand:**
- Administrative actions (e.g., teacher forcibly ending a student's session)
- System-initiated disconnections (e.g., maintenance, security)
- External triggers that require immediate session termination

**EndSessionCommand format:**
```json
{
  "userId": "user-123",
  "assignmentId": "assignment-001",
  "connectionId": "xyz-connection-id"
}
```

When a command is received, the service:
1. Validates the command (checks if userId, assignmentId, and connectionId match)
2. Removes the connection from active state
3. Publishes `StudentActivityEndedEvent`
4. Sends `ForceDisconnect` message to the client
5. Commits the Kafka offset (manual commit for reliability)

### Consumer Configuration
- **Manual offset commit**: Ensures at-least-once processing
- **Low latency settings**: FetchWaitMaxMs = 100ms for quick response
- **Idempotent handling**: Duplicate commands are safely ignored
- **Automatic reconnection**: Handles broker failures gracefully

## Running the Service

### Quick Start with Docker

The project includes a `docker-compose.yml` for running Redis and Kafka locally:

```bash
# Start infrastructure (Redis + Kafka)
docker-compose up -d

# Verify services are running
docker-compose ps

# View logs
docker-compose logs -f

# Stop infrastructure
docker-compose down
```

This will start:
- **Redis** on `localhost:6379`
- **Kafka (Redpanda)** on `localhost:9092`
- **Redpanda Console** on `http://localhost:8080` (Kafka UI)

### Prerequisites
- .NET 9.0 SDK
- Docker and Docker Compose (for running Redis and Kafka locally)
- OR manually install Redis (localhost:6379) and Kafka (localhost:9092)

### Steps
1. **Start infrastructure** (if using Docker):
   ```bash
   docker-compose up -d
   ```

2. Update `appsettings.json` with your JWT settings

3. Run the service:
   ```bash
   cd WebSocketPlayground
   dotnet run
   ```

The service will be available at:
- HTTP: http://localhost:5228
- HTTPS: https://localhost:7144

SignalR Hub endpoint:
- https://localhost:7144/hubs/studentActivity

Test utilities:
- Token generator: https://localhost:7144/generate-token
- Test client: https://localhost:7144/test-client.html

## Testing

### Quick Start Testing

1. **Get a test JWT token:**
   ```
   https://localhost:7144/generate-token
   ```
   This endpoint returns a JSON response with a valid JWT token for testing.

2. **Use the built-in test client:**
   ```
   https://localhost:7144/test-client.html
   ```
   - Paste the token from step 1
   - Enter assignmentId and attemptId
   - Click "Connect"

### Manual Testing with Browser Console
```javascript
// Set a test JWT cookie (in real app, this comes from login)
document.cookie = "access_token=your-jwt-token; path=/";

// Connect to hub
const connection = new signalR.HubConnectionBuilder()
  .withUrl("https://localhost:7144/hubs/studentActivity?assignmentId=test-assignment&attemptId=test-attempt-1")
  .build();

await connection.start();
```

## Monitoring and Debugging

The service logs all connection lifecycle events:
- Connection attempts with userId, assignmentId, attemptId
- Duplicate connection detection
- Grace period start/cancel/expiry
- Pending connection promotion/timeout
- Event publishing

Check logs for troubleshooting:
```bash
dotnet run --verbosity detailed
```

## Production Considerations

1. **JWT Secret Key**: Use a strong secret key (min 32 characters) and store in secure configuration (Azure Key Vault, AWS Secrets Manager, etc.)

2. **CORS Origins**: Update the CORS policy in Program.cs with actual client domain(s) instead of allowing all localhost origins

3. **Redis Clustering**: For production, use Redis Cluster or Sentinel for high availability

4. **SignalR Scale-out**: The Redis backplane is configured for multi-instance deployment

5. **Kafka Production Setup**: 
   - Use multiple brokers for high availability
   - Configure appropriate retention policies for event topics
   - Set up monitoring for consumer lag
   - Consider using schema registry for message validation

6. **Health Checks**: Add health check endpoints for Redis, Kafka, and SignalR connectivity monitoring

7. **Logging**: Configure structured logging (Serilog, Application Insights) for production monitoring

8. **Security**: 
   - Remove the `/generate-token` endpoint in production
   - Remove or secure the test client endpoint
   - Use proper certificate validation for Kafka SSL/TLS

## License

[Your License Here]

