# Black Swan Twitter Scraping Service

[![Node.js](https://img.shields.io/badge/node.js-%3E%3D18.0.0-green.svg)](https://nodejs.org/)
[![License](https://img.shields.io/badge/license-MIT-yellow.svg)](LICENSE)
[![Author](https://img.shields.io/badge/author-Muhammad%20Bilal%20Motiwala-purple.svg)](https://github.com/bilalmotiwala)

A high-performance Twitter monitoring service that collects tweets from monitored accounts with intelligent thread detection and smart deduplication. This service is part of the Black Swan project ecosystem for comprehensive social media monitoring and analysis.

## 🚀 Key Features

### Improvements

- **🧵 Advanced Thread Detection**: Simplified and more reliable thread detection algorithm with better pattern recognition
- **📦 Consolidated Collections**: Streamlined to only 2 data collections (`tweet_global` + `tweet_tokens`) for better organization
- **🚀 Intelligent Duplicate Detection**: Enhanced with batch processing and intelligent caching for up to 90% performance improvement
- **💾 Optimized Memory Management**: Improved memory usage with automatic cache cleanup and performance monitoring
- **🔧 Robust Error Handling**: Better error handling and recovery mechanisms with graceful degradation
- **🪙 Token Account Management**: Comprehensive cryptocurrency-related tweet monitoring via token accounts

### Core Capabilities

- **Real-time Twitter Monitoring**: Continuous monitoring of specified Twitter accounts
- **Intelligent Thread Detection**: Automatically detects and combines tweet threads
- **Smart Deduplication**: Advanced duplicate filtering with in-memory caching
- **Token Mention Tracking**: Monitors cryptocurrency token mentions across Twitter
- **RESTful API**: Comprehensive API for data access and service management
- **Real-time Logging**: Server-Sent Events (SSE) for real-time log streaming
- **Firebase Integration**: Secure data storage with Firestore
- **Rate Limiting**: Built-in rate limiting to respect Twitter API limits

## 📋 Table of Contents

- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [API Documentation](#api-documentation)
- [Architecture](#architecture)
- [Data Collections](#data-collections)
- [Performance](#performance)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

## 🛠 Installation

### Prerequisites

- **Node.js**: Version 18.0.0 or higher
- **npm**: Version 8.0.0 or higher
- **Firebase Project**: With Firestore enabled
- **Twitter Account**: For authentication and API access

### Step 1: Clone the Repository

```bash
git clone <repository-url>
cd blackswan-twitter-scraping-service
```

### Step 2: Install Dependencies

```bash
npm install
```

### Step 3: Firebase Setup

1. Create a Firebase project at [Firebase Console](https://console.firebase.google.com/)
2. Enable Firestore Database
3. Generate a service account key:
   - Go to Project Settings → Service Accounts
   - Click "Generate new private key"
   - Download the JSON file
   - Rename it to `serviceAccountKey.json` and place it in the project root

### Step 4: Environment Configuration

1. Copy the example environment file:

```bash
cp .env.example .env
```

2. Edit `.env` with your configuration (see [Configuration](#configuration) section)

### Step 5: Start the Service

```bash
# Development mode with auto-restart
npm run dev

# Production mode
npm start
```

## ⚙️ Configuration

### Environment Variables

Create a `.env` file in the project root with the following variables:

```env
# Server Configuration
PORT=8081
NODE_ENV=development

# Twitter Authentication (Cookie-based - Preferred Method)
TWITTER_COOKIES_AUTH_TOKEN=your_auth_token_here
TWITTER_COOKIES_CT0=your_ct0_token_here
TWITTER_COOKIES_GUEST_ID=your_guest_id_here

# Twitter Authentication (Username/Password - Fallback Method)
TWITTER_USERNAME=your_twitter_username
TWITTER_EMAIL=your_twitter_email
TWITTER_PASSWORD=your_twitter_password
TWITTER_2FA_SECRET=your_2fa_secret_here
```

### Processing Configuration

The service uses the following default configuration (modifiable in `index.js`):

```javascript
const PROCESSING_CONFIG = {
  TWEET_LOOKBACK_MINUTES: 1440, // 24 hours
  PROCESSING_INTERVAL: 600000, // 10 minutes
  RATE_LIMIT_DELAY: 5000, // 5 seconds between accounts
  TOKEN_RATE_LIMIT_DELAY: 10000, // 10 seconds between token searches
  MAX_TWEETS_PER_ACCOUNT: 100, // Maximum tweets per account
  MAX_TWEETS_PER_TOKEN_SEARCH: 100, // Maximum tweets per token search
  DUPLICATE_CACHE_SIZE: 25000, // Cache size for duplicate detection
  BATCH_DUPLICATE_CHECK_SIZE: 25, // Batch size for Firestore queries
};
```

### Twitter Authentication Methods

#### Method 1: Cookie-based Authentication (Recommended)

1. Log into Twitter in your browser
2. Open Developer Tools (F12)
3. Go to Application/Storage → Cookies → https://twitter.com
4. Copy the following cookies:
   - `auth_token`
   - `ct0`
   - `guest_id`
5. Add them to your `.env` file

#### Method 2: Username/Password Authentication (Fallback)

1. Use your Twitter username, email, and password
2. For 2FA accounts, provide the 2FA secret
3. Add them to your `.env` file

## 🚀 Usage

### Starting the Service

```bash
# Start the service
npm start

# The service will automatically:
# 1. Initialize Firebase connection
# 2. Authenticate with Twitter
# 3. Load monitored accounts from Firestore
# 4. Begin processing cycle
```

### Service Endpoints

Once started, the service provides the following endpoints:

- **API Documentation**: `GET http://localhost:8081/`
- **Health Check**: `GET http://localhost:8081/health`
- **Real-time Logs**: `GET http://localhost:8081/logs/stream`
- **Public Logs**: `GET http://localhost:8081/publicStream`

### Adding Monitored Accounts

#### Via API

```bash
curl -X POST http://localhost:8081/accounts \
  -H "Content-Type: application/json" \
  -d '{
    "username": "elonmusk",
    "priority": 1,
    "category": "crypto"
  }'
```

#### Via Firestore

Add documents to the `twitter_accounts` collection:

```javascript
{
  "username": "elonmusk",
  "priority": 1,
  "category": "crypto",
  "isActive": true,
  "createdAt": "2024-01-01T00:00:00Z",
  "processCount": 0,
  "errorCount": 0
}
```

### Adding Token Accounts

Add documents to the `twitter_token_accounts` collection:

```javascript
{
  "username": "bitcoin",
  "tokenSymbol": "BTC",
  "tokenAddress": "0x...",
  "priority": 1,
  "category": "token",
  "isActive": true,
  "createdAt": "2024-01-01T00:00:00Z",
  "processCount": 0,
  "errorCount": 0
}
```

## 📚 API Documentation

### General Endpoints

| Method | Endpoint       | Description                                  |
| ------ | -------------- | -------------------------------------------- |
| `GET`  | `/`            | API documentation and service information    |
| `GET`  | `/health`      | Health check and comprehensive statistics    |
| `GET`  | `/logs/stream` | Real-time log streaming (Server-Sent Events) |
| `GET`  | `/stats`       | Detailed processing statistics               |
| `POST` | `/start`       | Start the processing engine                  |
| `POST` | `/stop`        | Stop the processing engine                   |

### Account Management

| Method | Endpoint    | Description                          |
| ------ | ----------- | ------------------------------------ |
| `GET`  | `/accounts` | List all monitored Twitter accounts  |
| `POST` | `/accounts` | Add a new Twitter account to monitor |

### Data Access

| Method | Endpoint                | Description                                     |
| ------ | ----------------------- | ----------------------------------------------- |
| `GET`  | `/recent-tweets`        | Get recent tweets (global or token collection)  |
| `GET`  | `/recent-threads`       | Get recent threads (global or token collection) |
| `GET`  | `/recent-token-tweets`  | Get recent token mention tweets                 |
| `GET`  | `/recent-token-threads` | Get recent token mention threads                |

### Token Management

| Method | Endpoint                   | Description                       |
| ------ | -------------------------- | --------------------------------- |
| `GET`  | `/token-accounts`          | List all monitored token accounts |
| `POST` | `/toggle-token-processing` | Enable/disable token processing   |

### Monitoring

| Method | Endpoint           | Description                             |
| ------ | ------------------ | --------------------------------------- |
| `GET`  | `/duplicate-stats` | Duplicate detection performance metrics |
| `GET`  | `/thread-stats`    | Thread detection analytics              |

### Example API Calls

#### Get Recent Tweets

```bash
# Get recent tweets from global collection
curl "http://localhost:8081/recent-tweets?collection=global&hours=1&limit=50"

# Get recent tweets from token collection
curl "http://localhost:8081/recent-tweets?collection=tokens&hours=1&limit=50"

# Get recent tweets for specific username
curl "http://localhost:8081/recent-tweets?username=elonmusk&hours=24"
```

#### Get Processing Statistics

```bash
curl "http://localhost:8081/stats"
```

#### Toggle Token Processing

```bash
curl -X POST http://localhost:8081/toggle-token-processing \
  -H "Content-Type: application/json" \
  -d '{"enabled": true}'
```

## 🏗 Architecture

### System Components

```
┌─────────────────────────────────────────────────────────────┐
│                    Black Swan Twitter Service v2.0          │
├─────────────────────────────────────────────────────────────┤
│  Express.js API Server                                      │
│  ├── RESTful Endpoints                                      │
│  ├── Real-time Log Streaming (SSE)                          │
│  └── Health Monitoring                                      │
├─────────────────────────────────────────────────────────────┤
│  Twitter Processing Engine                                  │
│  ├── TwitterServiceManager                                  │
│  ├── EnhancedDuplicateDetector                              │
│  ├── ImprovedThreadDetector                                 │
│  ├── UnifiedTweetStorageManager                             │
│  ├── AccountManager                                         │
│  └── TokenAccountsManager                                   │
├─────────────────────────────────────────────────────────────┤
│  External Integrations                                      │
│  ├── Twitter API (via @elizaos/plugin-twitter)              │
│  └── Firebase Firestore                                     │
└─────────────────────────────────────────────────────────────┘
```

### Processing Flow

1. **Authentication**: Authenticate with Twitter API using cookies or credentials
2. **Account Loading**: Load monitored accounts from Firestore
3. **Data Collection**: Fetch tweets from monitored accounts
4. **Thread Detection**: Identify and combine tweet threads
5. **Duplicate Filtering**: Remove duplicate tweets using intelligent caching
6. **Data Storage**: Store new tweets and threads in Firestore
7. **Statistics Update**: Update processing statistics and account status
8. **Rate Limiting**: Respect Twitter API rate limits
9. **Cycle Repeat**: Repeat the process based on configured interval

### Thread Detection Algorithm

The service uses multiple methods to detect tweet threads:

1. **Reply Chains**: Tweets replying to the same user
2. **Numbered Patterns**: "1/5", "2/5" format
3. **Sequence Patterns**: "1.", "2." format
4. **Parentheses Patterns**: "(1/5)" format
5. **Bracket Patterns**: "[1/5]" format
6. **Emoji Patterns**: 🧵 thread emoji
7. **Keyword Patterns**: "Thread:", "Part 1" format
8. **Conversation Continuity**: Same conversation ID
9. **Rapid Succession**: Tweets within 10 minutes

## 📊 Data Collections

### Firestore Collections

#### `tweet_global`

Stores all tweets and threads from account monitoring.

**Document Structure:**

```javascript
{
  // Core tweet data
  tweetId: "1234567890",
  username: "elonmusk",
  authorId: "44196397",
  authorName: "Elon Musk",
  authorHandle: "elonmusk",
  authorVerified: true,

  // Content
  text: "Tweet content here",
  fullText: "Full tweet content here",

  // Timestamps
  createdAt: "2024-01-01T00:00:00Z",
  collectedAt: "2024-01-01T00:00:00Z",

  // Engagement metrics
  likes: 1000,
  retweets: 500,
  replies: 100,
  quotes: 50,
  views: 10000,

  // Tweet types
  isRetweet: false,
  isReply: false,
  isQuote: false,
  isPinned: false,

  // Thread information
  isThread: false,
  threadId: null,
  threadPosition: null,
  threadTotalParts: null,

  // Content entities
  hashtags: ["#bitcoin", "#crypto"],
  mentions: ["@twitter"],
  urls: ["https://example.com"],
  media: [],
  symbols: ["$BTC"],

  // Metadata
  language: "en",
  source: "Twitter Web App",
  collectedBy: "blackswan-twitter-backend-service-v2",
  processingVersion: "2.0.0"
}
```

#### `tweet_tokens`

Stores all tweets and threads from token account searches.

**Document Structure:**

```javascript
{
  // Same structure as tweet_global plus:
  tokenAccountId: "token_account_123",
  tokenSymbol: "BTC",
  tokenAddress: "0x...",
  sourceUsername: "bitcoin",

  collectionContext: {
    method: "token_account_search",
    sourceTokenAccount: {...},
    searchQuery: "BTC",
    sourceType: "token_mention"
  }
}
```

#### `twitter_accounts`

Configuration for monitored Twitter accounts.

**Document Structure:**

```javascript
{
  username: "elonmusk",
  priority: 1,
  category: "crypto",
  isActive: true,
  createdAt: "2024-01-01T00:00:00Z",
  lastProcessed: "2024-01-01T00:00:00Z",
  processCount: 100,
  errorCount: 0,
  lastError: null,
  lastTweetCount: 50
}
```

#### `twitter_token_accounts`

Configuration for token-specific Twitter accounts.

**Document Structure:**

```javascript
{
  username: "bitcoin",
  tokenSymbol: "BTC",
  tokenAddress: "0x...",
  priority: 1,
  category: "token",
  isActive: true,
  createdAt: "2024-01-01T00:00:00Z",
  lastProcessed: "2024-01-01T00:00:00Z",
  processCount: 50,
  errorCount: 0,
  lastError: null,
  lastTweetCount: 25
}
```

## ⚡ Performance

### Optimization Features

- **Intelligent Caching**: In-memory cache for duplicate detection reduces database queries by up to 90%
- **Batch Processing**: Firestore queries are batched to respect the 30-item IN limit
- **Rate Limiting**: Built-in delays prevent Twitter API rate limit violations
- **Memory Management**: Automatic cache cleanup prevents memory bloat
- **Error Recovery**: Graceful error handling with automatic retry mechanisms

### Performance Metrics

The service tracks comprehensive performance metrics:

- **Processing Statistics**: Total accounts processed, tweets saved, threads saved
- **Duplicate Detection**: Cache hits, duplicates filtered, cache efficiency
- **Thread Detection**: Threads found, detection methods used, average thread length
- **Error Tracking**: Processing errors, token account errors, error rates
- **Timing Information**: Processing times, uptime, last processing timestamps

### Monitoring Endpoints

- `/health` - Overall service health and statistics
- `/stats` - Detailed processing statistics
- `/duplicate-stats` - Duplicate detection performance
- `/thread-stats` - Thread detection analytics

## 📈 Monitoring

### Real-time Logging

The service provides real-time log streaming via Server-Sent Events:

```javascript
// Connect to log stream
const eventSource = new EventSource("http://localhost:8081/logs/stream");

eventSource.onmessage = function (event) {
  const logEntry = JSON.parse(event.data);
  console.log(`[${logEntry.timestamp}] ${logEntry.level}: ${logEntry.message}`);
};
```

### Health Monitoring

Monitor service health with the `/health` endpoint:

```bash
curl http://localhost:8081/health
```

**Response includes:**

- Service status and version
- Processing engine statistics
- Account and token account statistics
- Performance metrics
- Log streaming statistics

### Performance Monitoring

Track performance with dedicated endpoints:

```bash
# Duplicate detection performance
curl http://localhost:8081/duplicate-stats

# Thread detection analytics
curl http://localhost:8081/thread-stats
```

## 🔧 Troubleshooting

### Common Issues

#### Authentication Errors

**Problem**: Twitter authentication fails
**Solutions**:

1. Verify your Twitter credentials in `.env`
2. Check if cookies are expired (cookie-based auth)
3. Ensure 2FA secret is correct (if using 2FA)
4. Try the fallback authentication method

#### Firestore Connection Issues

**Problem**: Cannot connect to Firestore
**Solutions**:

1. Verify `serviceAccountKey.json` is in the project root
2. Check Firebase project configuration
3. Ensure Firestore is enabled in your Firebase project
4. Verify network connectivity

#### Rate Limiting

**Problem**: Twitter API rate limit exceeded
**Solutions**:

1. Increase `RATE_LIMIT_DELAY` in configuration
2. Reduce `MAX_TWEETS_PER_ACCOUNT`
3. Monitor rate limit status in logs
4. Implement exponential backoff

#### Memory Issues

**Problem**: High memory usage
**Solutions**:

1. Reduce `DUPLICATE_CACHE_SIZE`
2. Increase cache cleanup frequency
3. Monitor memory usage in logs
4. Restart service periodically

### Debug Mode

Enable debug logging by setting:

```env
NODE_ENV=development
```

### Log Analysis

The service provides comprehensive logging:

- **🔄 Processing**: Account processing status
- **📱 Data Collection**: Tweet fetching results
- **🧵 Thread Detection**: Thread detection results
- **💾 Storage**: Database operations
- **❌ Errors**: Error details and stack traces
- **📊 Statistics**: Performance metrics

### Support

For additional support:

1. Check or create an issue for bugs or feature requests
2. Review the logs for error details
3. Verify your configuration
4. Test with a minimal setup

## 🤝 Contributing

We welcome contributions! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Setup

```bash
# Install dependencies
npm install

# Start in development mode
npm run dev

# Run tests (if available)
npm test
```

### Code Style

- Use ES6+ features
- Follow existing code patterns
- Add comprehensive comments
- Include error handling
- Update documentation

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 👨‍💻 Author

**Muhammad Bilal Motiwala**

- GitHub: [@bilalmotiwala](https://github.com/bilalmotiwala)
- Email: [bilal@oaiaolabs.com](mailto:bilal@oaiaolabs.com)

## 🙏 Acknowledgments

- [@elizaos/plugin-twitter](https://github.com/elizaos/plugin-twitter) for Twitter API integration
- [Firebase](https://firebase.google.com/) for database services
- [Express.js](https://expressjs.com/) for the web framework
- The open-source community for inspiration and support

---

**🚀 Happy Twitter Feed Scraping!**
