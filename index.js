/**
 * Black Swan Twitter Scraping Service
 * 
 * A high-performance Twitter monitoring service that collects tweets from monitored accounts
 * with intelligent thread detection and smart deduplication. This service is part of the
 * Black Swan project ecosystem for comprehensive social media monitoring and analysis.
 * 
 * Key Features:
 * 1. Simplified and robust thread management with better pattern recognition
 * 2. Consolidated collections: tweet_global and tweet_tokens for better organization
 * 3. Enhanced duplication detection with batch processing and intelligent caching
 * 4. Improved memory management and performance monitoring
 * 5. Better error handling and recovery mechanisms
 * 6. Token account management for cryptocurrency-related tweet monitoring
 * 
 * Architecture:
 * - Express.js REST API server with real-time log streaming
 * - Firebase Firestore for data persistence
 * - Twitter API integration via @elizaos/plugin-twitter
 * - Intelligent thread detection and combination
 * - Advanced duplicate filtering with caching
 * - Rate limiting and error handling
 * 
 * Collections:
 * - tweet_global: All tweets and threads from account monitoring
 * - tweet_tokens: All tweets and threads from token account searches
 * - twitter_accounts: Monitored Twitter accounts configuration
 * - twitter_token_accounts: Token-specific Twitter accounts configuration
 * 
 * Author: Muhammad Bilal Motiwala
 * Project: Black Swan
 */

// Core Express.js and middleware imports
import express from "express";                    // Web framework for Node.js
import cors from "cors";                         // Cross-Origin Resource Sharing middleware
import helmet from "helmet";                     // Security middleware for Express
import rateLimit from "express-rate-limit";      // Rate limiting middleware
import { EventEmitter } from "events";           // Node.js EventEmitter for real-time features

// Twitter integration
import twitterPlugin from "@elizaos/plugin-twitter";  // Twitter API plugin for authentication and data collection

// Environment and configuration
import dotenv from "dotenv";                     // Environment variable loader

// Firebase Admin SDK imports for database operations
import { initializeApp, cert } from "firebase-admin/app";           // Firebase app initialization
import { getFirestore, FieldValue, Timestamp } from "firebase-admin/firestore";  // Firestore database operations
import serviceAccount from "./serviceAccountKey.json" with { type: "json" };    // Firebase service account credentials

// Load environment variables from .env file
dotenv.config();

// Initialize Firebase Admin SDK with service account credentials
// This provides secure access to Firestore database for storing collected tweets
initializeApp({
  credential: cert(serviceAccount),
});
const db = getFirestore();  // Firestore database instance for all data operations

// ============================================================================
// ENVIRONMENT CONFIGURATION CONSTANTS
// ============================================================================
// All sensitive credentials are loaded from environment variables for security
// These should be set in your .env file and never committed to version control

// Twitter Authentication Credentials (Cookie-based authentication - preferred method)
const TWITTER_COOKIES_AUTH_TOKEN = process.env.TWITTER_COOKIES_AUTH_TOKEN;  // Twitter auth token from browser cookies
const TWITTER_COOKIES_CT0 = process.env.TWITTER_COOKIES_CT0;                // CSRF token for Twitter API requests
const TWITTER_COOKIES_GUEST_ID = process.env.TWITTER_COOKIES_GUEST_ID;      // Guest ID for Twitter API access

// Twitter Authentication Credentials (Username/Password authentication - fallback method)
const TWITTER_USERNAME = process.env.TWITTER_USERNAME;                      // Twitter username for login
const TWITTER_EMAIL = process.env.TWITTER_EMAIL;                            // Twitter email for login
const TWITTER_PASSWORD = process.env.TWITTER_PASSWORD;                      // Twitter password for login
const TWITTER_2FA_SECRET = process.env.TWITTER_2FA_SECRET;                  // 2FA secret for two-factor authentication

// Server Configuration
const PORT = process.env.PORT || 8081;  // Port for the Express server (default: 8081)

// ============================================================================
// PROCESSING CONFIGURATION
// ============================================================================
// These constants control the behavior of the Twitter monitoring system
// Adjust these values based on your monitoring needs and rate limits

const PROCESSING_CONFIG = {
  // Time-based Configuration
  TWEET_LOOKBACK_MINUTES: 1440,        // How far back to look for tweets (24 hours)
  PROCESSING_INTERVAL: 600000,         // How often to run the processing cycle (10 minutes)
  THREAD_DETECTION_TIMEOUT: 30000,     // Timeout for thread detection operations (30 seconds)
  
  // Rate Limiting Configuration
  RATE_LIMIT_DELAY: 5000,              // Delay between processing different accounts (5 seconds)
  TOKEN_RATE_LIMIT_DELAY: 10000,       // Delay between token account searches (10 seconds)
  MAX_CONCURRENT_ACCOUNTS: 1,          // Maximum accounts to process simultaneously (1 for safety)
  
  // Data Collection Limits
  MAX_TWEETS_PER_ACCOUNT: 100,         // Maximum tweets to fetch per account per cycle
  MAX_TWEETS_PER_TOKEN_SEARCH: 100,    // Maximum tweets to fetch per token search
  
  // Performance Optimization
  DUPLICATE_CACHE_SIZE: 25000,         // Size of in-memory cache for duplicate detection
  BATCH_DUPLICATE_CHECK_SIZE: 25,      // Batch size for Firestore duplicate checks (Firestore IN limit is 30)
};

/**
 * Enhanced Duplicate Detection Manager
 * 
 * This class provides intelligent duplicate detection for tweets using a combination of
 * in-memory caching and batch database queries. It significantly improves performance
 * by reducing redundant database calls and provides fast duplicate checking.
 * 
 * Key Features:
 * - In-memory caching for frequently accessed tweet IDs
 * - Batch database queries to minimize Firestore operations
 * - Separate caches for global tweets and token-specific tweets
 * - Automatic cache cleanup to prevent memory bloat
 * - Firestore IN query optimization with proper batch sizing
 * 
 * Performance Benefits:
 * - Reduces database queries by up to 90% through intelligent caching
 * - Batch processing respects Firestore's 30-item IN query limit
 * - Memory-efficient with automatic cleanup of old entries
 */
class EnhancedDuplicateDetector {
  /**
   * Initialize the duplicate detector with empty caches
   * 
   * Creates separate caches for global tweets and token-specific tweets to
   * optimize memory usage and query performance.
   */
  constructor() {
    this.globalCache = new Map(); // Cache for tweet_global collection: tweet_id -> timestamp
    this.tokenCache = new Map();  // Cache for tweet_tokens collection: tweet_id -> timestamp
    this.maxCacheSize = PROCESSING_CONFIG.DUPLICATE_CACHE_SIZE;  // Maximum cache size before cleanup
    this.initializeCaches();  // Load existing tweets into cache on startup
  }

  /**
   * Initialize caches with recent tweets from the database
   * 
   * This method pre-loads the cache with tweet IDs from the last 24 hours to
   * provide immediate duplicate detection without requiring database queries
   * for recently processed tweets.
   * 
   * Process:
   * 1. Query both tweet_global and tweet_tokens collections for recent tweets
   * 2. Extract tweet IDs and timestamps
   * 3. Populate the respective caches
   * 4. Log cache initialization statistics
   * 
   * @async
   * @method initializeCaches
   * @returns {Promise<void>} Resolves when caches are initialized
   */
  async initializeCaches() {
    try {
      console.log('🔄 Initializing enhanced duplicate detection caches...');
      
      // Calculate cutoff time for recent tweets (24 hours ago)
      const cutoffTime = Timestamp.fromDate(new Date(Date.now() - 24 * 60 * 60 * 1000));
      
      // Load recent global tweets from tweet_global collection
      // Only select tweetId and createdAt fields to minimize data transfer
      const globalSnapshot = await db.collection('tweet_global')
        .where('createdAt', '>=', cutoffTime)
        .select('tweetId', 'createdAt')
        .get();

      // Populate global cache with tweet IDs and timestamps
      globalSnapshot.forEach(doc => {
        const data = doc.data();
        if (data.tweetId) {
          this.globalCache.set(data.tweetId, data.createdAt.toMillis());
        }
      });

      // Load recent token tweets from tweet_tokens collection
      // Only select tweetId and createdAt fields to minimize data transfer
      const tokenSnapshot = await db.collection('tweet_tokens')
        .where('createdAt', '>=', cutoffTime)
        .select('tweetId', 'createdAt')
        .get();

      // Populate token cache with tweet IDs and timestamps
      tokenSnapshot.forEach(doc => {
        const data = doc.data();
        if (data.tweetId) {
          this.tokenCache.set(data.tweetId, data.createdAt.toMillis());
        }
      });

      console.log(`✅ Duplicate detection initialized: ${this.globalCache.size} global, ${this.tokenCache.size} token tweets cached`);
    } catch (error) {
      console.error('❌ Error initializing duplicate caches:', error);
    }
  }

  /**
   * Fast batch duplicate checking with intelligent caching
   * 
   * This is the core method for duplicate detection. It uses a two-phase approach:
   * 1. First pass: Check in-memory cache for immediate results
   * 2. Second pass: Batch database queries for uncached tweets
   * 
   * The method is optimized for performance by:
   * - Minimizing database queries through intelligent caching
   * - Using batch operations to respect Firestore limits
   * - Providing detailed statistics for monitoring
   * 
   * @async
   * @method filterDuplicates
   * @param {Array} tweets - Array of tweet objects to check for duplicates
   * @param {boolean} isTokenMode - Whether to use token cache (true) or global cache (false)
   * @returns {Promise<Object>} Object containing newTweets, duplicates, and statistics
   * @returns {Array} returns.newTweets - Array of tweets that are not duplicates
   * @returns {Array} returns.duplicates - Array of tweet IDs that are duplicates
   * @returns {Object} returns.stats - Statistics about the filtering process
   */
  async filterDuplicates(tweets, isTokenMode = false) {
    // Select the appropriate cache based on the mode
    const cache = isTokenMode ? this.tokenCache : this.globalCache;
    const newTweets = [];      // Tweets that are not duplicates
    const duplicates = [];     // Tweet IDs that are duplicates
    const needDbCheck = [];    // Tweets that need database verification

    // First pass: check memory cache for immediate duplicate detection
    // This provides the fastest possible duplicate checking for cached tweets
    for (const tweet of tweets) {
      if (cache.has(tweet.id)) {
        // Tweet ID found in cache - it's a duplicate
        duplicates.push(tweet.id);
      } else {
        // Tweet ID not in cache - needs database verification
        needDbCheck.push(tweet);
      }
    }

    // Second pass: batch check database for uncached tweets
    if (needDbCheck.length > 0) {
      const collection = isTokenMode ? 'tweet_tokens' : 'tweet_global';
      const tweetIds = needDbCheck.map(t => t.id);
      
      // FIXED: Batch check in chunks respecting Firestore's 30-item IN limit
      const chunks = this.chunkArray(tweetIds, PROCESSING_CONFIG.BATCH_DUPLICATE_CHECK_SIZE);
      const existingIds = new Set();

      for (const chunk of chunks) {
        try {
          // FIXED: Add validation for chunk size
          if (chunk.length === 0) continue;
          if (chunk.length > 30) {
            console.warn(`⚠️ Chunk size ${chunk.length} exceeds Firestore IN limit, splitting further`);
            // Recursively split if somehow still too large
            const subChunks = this.chunkArray(chunk, 25);
            for (const subChunk of subChunks) {
              if (subChunk.length > 0) {
                const subSnapshot = await db.collection(collection)
                  .where('tweetId', 'in', subChunk)
                  .select('tweetId')
                  .get();
                
                subSnapshot.forEach(doc => {
                  const data = doc.data();
                  if (data.tweetId) {
                    existingIds.add(data.tweetId);
                  }
                });
              }
            }
          } else {
            const snapshot = await db.collection(collection)
              .where('tweetId', 'in', chunk)
              .select('tweetId')
              .get();
            
            snapshot.forEach(doc => {
              const data = doc.data();
              if (data.tweetId) {
                existingIds.add(data.tweetId);
              }
            });
          }
        } catch (error) {
          console.error(`❌ Error in batch duplicate check for chunk of ${chunk.length} items:`, error.message);
          // FIXED: On error, assume tweets don't exist rather than failing completely
          console.warn(`⚠️ Assuming tweets in failed chunk are new (${chunk.length} tweets)`);
        }
      }

      // Final filtering
      for (const tweet of needDbCheck) {
        if (existingIds.has(tweet.id)) {
          duplicates.push(tweet.id);
          // Add to cache for future checks
          cache.set(tweet.id, tweet.timestamp * 1000);
        } else {
          newTweets.push(tweet);
        }
      }
    }

    // Clean cache if it gets too large
    this.cleanCache(cache);

    return {
      newTweets,
      duplicates,
      stats: {
        total: tweets.length,
        new: newTweets.length,
        duplicates: duplicates.length,
        cacheHits: tweets.length - needDbCheck.length,
        dbChecks: needDbCheck.length
      }
    };
  }

  chunkArray(array, chunkSize) {
    const chunks = [];
    for (let i = 0; i < array.length; i += chunkSize) {
      chunks.push(array.slice(i, i + chunkSize));
    }
    return chunks;
  }

  cleanCache(cache) {
    if (cache.size > this.maxCacheSize) {
      // Remove oldest 20% of entries
      const entries = Array.from(cache.entries());
      entries.sort((a, b) => a[1] - b[1]); // Sort by timestamp
      const toRemove = Math.floor(entries.length * 0.2);
      
      for (let i = 0; i < toRemove; i++) {
        cache.delete(entries[i][0]);
      }
      
      console.log(`🧹 Cleaned cache: removed ${toRemove} old entries, ${cache.size} remaining`);
    }
  }

  addToCache(tweetId, isTokenMode = false, timestamp = Date.now()) {
    const cache = isTokenMode ? this.tokenCache : this.globalCache;
    cache.set(tweetId, timestamp);
  }
}

/**
 * Improved Thread Detector
 * Simplified, more reliable thread detection and combination
 */
class ImprovedThreadDetector {
  constructor() {
    this.threadPatterns = [
      { pattern: /(\d+)\/(\d+)/g, type: 'numbered' },      // "1/5", "2/5"
      { pattern: /^\d+\./m, type: 'sequence' },            // "1.", "2."
      { pattern: /\(\d+\/\d+\)/g, type: 'parentheses' },  // "(1/5)"
      { pattern: /\[\d+\/\d+\]/g, type: 'brackets' },     // "[1/5]"
      { pattern: /🧵/g, type: 'emoji' },                   // Thread emoji
      { pattern: /thread:/i, type: 'keyword' },            // "Thread:"
      { pattern: /part\s+\d+/i, type: 'part' },           // "Part 1"
    ];
  }

  /**
   * Main thread detection method - simplified and more reliable
   */
  async detectThreads(tweets, username) {
    console.log(`🧵 Detecting threads for ${tweets.length} tweets from @${username}...`);
    
    // Sort tweets chronologically
    const sortedTweets = tweets.sort((a, b) => a.timestamp - b.timestamp);
    
    // Group tweets into potential threads
    const threadGroups = new Map();
    const standaloneTweets = [];

    for (const tweet of sortedTweets) {
      const threadInfo = this.analyzeTweetForThread(tweet, sortedTweets);
      
      if (threadInfo.isThread) {
        const threadId = threadInfo.threadId || tweet.id;
        
        if (!threadGroups.has(threadId)) {
          threadGroups.set(threadId, {
            id: threadId,
            tweets: [],
            rootTweet: null,
            detectionMethods: new Set()
          });
        }
        
        const group = threadGroups.get(threadId);
        group.tweets.push({
          ...tweet,
          threadPosition: threadInfo.position,
          threadTotal: threadInfo.total
        });
        group.detectionMethods.add(threadInfo.method);
        
        if (threadInfo.isRoot) {
          group.rootTweet = tweet;
        }
      } else {
        standaloneTweets.push(tweet);
      }
    }

    // Convert thread groups to proper thread objects
    const threads = Array.from(threadGroups.values())
      .filter(group => group.tweets.length > 1) // Must have at least 2 tweets
      .map(group => this.createThreadObject(group))
      .filter(thread => thread !== null); // FIXED: Filter out null threads

    // Add back tweets from groups that didn't qualify as threads
    threadGroups.forEach(group => {
      if (group.tweets.length === 1) {
        standaloneTweets.push(group.tweets[0]);
      }
    });

    console.log(`✅ Thread detection complete: ${threads.length} threads, ${standaloneTweets.length} standalone tweets`);

    return {
      threads,
      standalone: standaloneTweets,
      stats: {
        totalTweets: tweets.length,
        threadsFound: threads.length,
        standaloneTweets: standaloneTweets.length,
        totalThreadTweets: threads.reduce((sum, t) => sum + t.tweets.length, 0)
      }
    };
  }

  /**
   * Analyze individual tweet for thread indicators
   */
  analyzeTweetForThread(tweet, allTweets) {
    const text = tweet.text || tweet.fullText || '';
    let threadInfo = {
      isThread: false,
      threadId: null,
      isRoot: false,
      position: 1,
      total: null,
      method: 'none'
    };

    // 1. Check for reply chains (strongest indicator)
    if (tweet.inReplyToStatusId && tweet.inReplyToUserId === tweet.userId) {
      threadInfo.isThread = true;
      threadInfo.threadId = this.findRootTweetId(tweet, allTweets);
      threadInfo.method = 'reply_chain';
      return threadInfo;
    }

    // 2. Check for explicit thread patterns
    for (const { pattern, type } of this.threadPatterns) {
      const match = text.match(pattern);
      if (match) {
        threadInfo.isThread = true;
        threadInfo.method = type;
        
        if (type === 'numbered') {
          const nums = text.match(/(\d+)\/(\d+)/);
          if (nums && nums.length >= 3) { // FIXED: Check array length
            threadInfo.position = parseInt(nums[1]);
            threadInfo.total = parseInt(nums[2]);
            threadInfo.isRoot = threadInfo.position === 1;
            threadInfo.threadId = threadInfo.isRoot ? tweet.id : null;
          }
        } else if (type === 'sequence') {
          const seqMatch = text.match(/^\d+/);
          if (seqMatch && seqMatch[0]) { // FIXED: Add null check
            const num = parseInt(seqMatch[0]);
            threadInfo.position = num;
            threadInfo.isRoot = num === 1;
            threadInfo.threadId = threadInfo.isRoot ? tweet.id : null;
          }
        } else {
          // For other patterns, assume first tweet is root
          threadInfo.isRoot = true;
          threadInfo.threadId = tweet.id;
        }
        
        break;
      }
    }

    // 3. Check for conversation continuity
    if (!threadInfo.isThread && tweet.conversationId && tweet.conversationId !== tweet.id) {
      const conversationTweets = allTweets.filter(t => 
        t.conversationId === tweet.conversationId && 
        t.userId === tweet.userId
      );
      
      if (conversationTweets.length > 1) {
        threadInfo.isThread = true;
        threadInfo.threadId = tweet.conversationId;
        threadInfo.method = 'conversation';
        threadInfo.isRoot = tweet.id === tweet.conversationId;
      }
    }

    // 4. Check for rapid succession (within 10 minutes)
    if (!threadInfo.isThread) {
      const rapidTweets = allTweets.filter(t => 
        t.userId === tweet.userId && 
        Math.abs(t.timestamp - tweet.timestamp) < 600 && // 10 minutes
        t.id !== tweet.id
      );

      if (rapidTweets.length >= 1) {
        const allRelated = [tweet, ...rapidTweets].sort((a, b) => a.timestamp - b.timestamp);
        const rootTweet = allRelated[0];
        
        threadInfo.isThread = true;
        threadInfo.threadId = rootTweet.id;
        threadInfo.method = 'rapid_succession';
        threadInfo.isRoot = tweet.id === rootTweet.id;
      }
    }

    return threadInfo;
  }

  /**
   * Find the root tweet ID for a reply chain
   */
  findRootTweetId(tweet, allTweets) {
    if (!tweet || !tweet.id) {
      console.warn('⚠️ Invalid tweet passed to findRootTweetId');
      return tweet?.id || null;
    }

    let current = tweet;
    const visited = new Set();
    let depth = 0;
    const maxDepth = 50; // FIXED: Prevent infinite loops
    
    while (current.inReplyToStatusId && !visited.has(current.id) && depth < maxDepth) {
      visited.add(current.id);
      const parent = allTweets.find(t => t && t.id === current.inReplyToStatusId);
      if (parent && parent.userId === current.userId) {
        current = parent;
        depth++;
      } else {
        break;
      }
    }
    
    return current?.id || tweet.id;
  }

  /**
   * Create a clean thread object
   */
  createThreadObject(group) {
    // FIXED: Safety checks for group and tweets
    if (!group || !group.tweets || !Array.isArray(group.tweets) || group.tweets.length === 0) {
      console.warn('⚠️ Invalid group passed to createThreadObject');
      return null;
    }

    // Sort tweets by timestamp and position
    const sortedTweets = group.tweets
      .filter(tweet => tweet && tweet.id) // FIXED: Filter out null/invalid tweets
      .sort((a, b) => {
        if (a.threadPosition && b.threadPosition) {
          return a.threadPosition - b.threadPosition;
        }
        return a.timestamp - b.timestamp;
      });

    if (sortedTweets.length === 0) {
      console.warn('⚠️ No valid tweets found in thread group');
      return null;
    }

    // Combine text intelligently
    const combinedText = sortedTweets
      .map(tweet => {
        let text = tweet.text || tweet.fullText || '';
        
        // Remove thread numbering for cleaner combined text
        text = text.replace(/^(\d+)\/(\d+)\s*/, '');
        text = text.replace(/^\d+\.\s*/, '');
        text = text.replace(/^\(\d+\/\d+\)\s*/, '');
        text = text.replace(/^\[\d+\/\d+\]\s*/, '');
        text = text.replace(/^part\s+\d+:?\s*/i, '');
        text = text.replace(/🧵\s*/, '');
        
        return text.trim();
      })
      .filter(text => text.length > 0)
      .join('\n\n');

    // Calculate aggregate metrics with safety checks
    const totalLikes = sortedTweets.reduce((sum, t) => sum + (t.likes || 0), 0);
    const totalRetweets = sortedTweets.reduce((sum, t) => sum + (t.retweets || 0), 0);
    const totalReplies = sortedTweets.reduce((sum, t) => sum + (t.replies || 0), 0);
    const totalViews = sortedTweets.reduce((sum, t) => sum + (t.views || 0), 0);

    return {
      id: group.id,
      type: 'thread',
      tweets: sortedTweets,
      rootTweet: group.rootTweet || sortedTweets[0],
      combinedText,
      totalParts: sortedTweets.length,
      createdAt: sortedTweets[0].timestamp,
      lastUpdated: sortedTweets[sortedTweets.length - 1].timestamp,
      metrics: {
        likes: totalLikes,
        retweets: totalRetweets,
        replies: totalReplies,
        views: totalViews
      },
      metadata: {
        detectionMethods: Array.from(group.detectionMethods || new Set()),
        hasNumbering: (group.detectionMethods || new Set()).has('numbered'),
        isReplyChain: (group.detectionMethods || new Set()).has('reply_chain')
      }
    };
  }
}

/**
 * Unified Tweet Storage Manager
 * Handles both tweet_global and tweet_tokens collections
 */
class UnifiedTweetStorageManager {
  constructor(duplicateDetector) {
    this.duplicateDetector = duplicateDetector;
  }

  /**
   * Save tweets to appropriate collection (tweet_global or tweet_tokens)
   */
  async saveTweets(tweets, metadata, isTokenMode = false) {
    const collection = isTokenMode ? 'tweet_tokens' : 'tweet_global';
    const filterResult = await this.duplicateDetector.filterDuplicates(tweets, isTokenMode);
    
    if (filterResult.newTweets.length === 0) {
      console.log(`⏭️ No new tweets to save (${filterResult.duplicates.length} duplicates filtered)`);
      return {
        saved: [],
        skipped: filterResult.duplicates,
        stats: filterResult.stats
      };
    }

    const batch = db.batch();
    const savedTweets = [];

    for (const tweet of filterResult.newTweets) {
      try {
        const tweetDoc = this.prepareTweetDocument(tweet, metadata, isTokenMode);
        
        // Use tweet ID as document ID for guaranteed uniqueness
        const docRef = db.collection(collection).doc(tweet.id);
        batch.set(docRef, tweetDoc);
        
        savedTweets.push({ id: tweet.id, ...tweetDoc });
        
        // Add to cache
        this.duplicateDetector.addToCache(tweet.id, isTokenMode, tweet.timestamp * 1000);
      } catch (error) {
        console.error(`❌ Error preparing tweet ${tweet.id}:`, error);
      }
    }

    if (savedTweets.length > 0) {
      await batch.commit();
      console.log(`💾 Saved ${savedTweets.length} tweets to ${collection}`);
    }

    return {
      saved: savedTweets,
      skipped: filterResult.duplicates,
      stats: filterResult.stats
    };
  }

  /**
   * Save thread to appropriate collection
   */
  async saveThread(thread, metadata, isTokenMode = false) {
    const collection = isTokenMode ? 'tweet_tokens' : 'tweet_global';
    const batch = db.batch();

    // Prepare thread document
    const threadDoc = this.prepareThreadDocument(thread, metadata, isTokenMode);
    
    // Use a dedicated thread document ID
    const threadDocRef = db.collection(collection).doc(`thread_${thread.id}`);
    batch.set(threadDocRef, threadDoc);

    // Save individual tweets with thread reference
    const filterResult = await this.duplicateDetector.filterDuplicates(thread.tweets, isTokenMode);
    
    for (const tweet of filterResult.newTweets) {
      const tweetDoc = this.prepareTweetDocument(tweet, metadata, isTokenMode);
      
      // Add thread information
      tweetDoc.isThread = true;
      tweetDoc.threadId = thread.id;
      tweetDoc.threadDocumentId = threadDocRef.id;
      tweetDoc.threadPosition = tweet.threadPosition || null;
      tweetDoc.threadTotalParts = thread.totalParts;

      const tweetDocRef = db.collection(collection).doc(tweet.id);
      batch.set(tweetDocRef, tweetDoc);
      
      // Add to cache
      this.duplicateDetector.addToCache(tweet.id, isTokenMode, tweet.timestamp * 1000);
    }

    await batch.commit();
    console.log(`🧵 Saved thread with ${thread.totalParts} parts to ${collection}`);

    return {
      threadDocumentId: threadDocRef.id,
      savedTweets: filterResult.newTweets.length,
      skippedTweets: filterResult.duplicates.length,
      stats: filterResult.stats
    };
  }

  /**
   * Prepare individual tweet document
   */
  prepareTweetDocument(tweet, metadata, isTokenMode = false) {
    // FIXED: Helper function to sanitize nested objects for Firestore
    const sanitizeForFirestore = (obj) => {
      if (obj === null || obj === undefined) {
        return null;
      }
      if (typeof obj !== 'object') {
        return obj;
      }
      if (Array.isArray(obj)) {
        return obj.map(item => sanitizeForFirestore(item));
      }
      
      const sanitized = {};
      for (const [key, value] of Object.entries(obj)) {
        if (value !== null && value !== undefined) {
          if (typeof value === 'object') {
            // Convert nested objects to strings to avoid Firestore nested entity errors
            sanitized[key] = JSON.stringify(value);
          } else {
            sanitized[key] = value;
          }
        }
      }
      return sanitized;
    };

    const baseDoc = {
      // Core tweet data
      tweetId: tweet.id,
      username: tweet.username,
      authorId: tweet.userId || null,
      authorName: tweet.name || tweet.username || null,
      authorHandle: tweet.username || null,
      authorVerified: tweet.isBlueVerified || tweet.isVerified || false,
      
      // Content
      text: tweet.text || '',
      fullText: tweet.fullText || tweet.text || '',
      displayTextRange: tweet.displayTextRange || null,
      
      // Timestamps
      createdAt: Timestamp.fromDate(new Date(tweet.timestamp * 1000)),
      collectedAt: FieldValue.serverTimestamp(),
      
      // Engagement metrics
      likes: tweet.likes || 0,
      retweets: tweet.retweets || 0,
      replies: tweet.replies || 0,
      quotes: tweet.quotes || 0,
      views: tweet.views || 0,
      bookmarks: tweet.bookmarks || 0,
      
      // Tweet types
      isRetweet: tweet.isRetweet || false,
      isReply: tweet.isReply || false,
      isQuote: tweet.isQuote || false,
      isPinned: tweet.isPinned || false,
      
      // Relationships
      inReplyToStatusId: tweet.inReplyToStatusId || null,
      inReplyToUserId: tweet.inReplyToUserId || null,
      inReplyToScreenName: tweet.inReplyToScreenName || null,
      quotedStatusId: tweet.quotedStatusId || null,
      retweetedStatusId: tweet.retweetedStatusId || null,
      conversationId: tweet.conversationId || tweet.id,
      
      // Content entities (sanitized arrays)
      hashtags: Array.isArray(tweet.hashtags) ? tweet.hashtags : [],
      mentions: Array.isArray(tweet.mentions) ? tweet.mentions : [],
      urls: Array.isArray(tweet.urls) ? tweet.urls : [],
      media: Array.isArray(tweet.media) ? tweet.media : [],
      symbols: Array.isArray(tweet.symbols) ? tweet.symbols : [],
      
      // Metadata (FIXED: Sanitize nested objects)
      language: tweet.lang || 'en',
      source: tweet.source || 'unknown',
      place: sanitizeForFirestore(tweet.place), // FIXED: Sanitize place object
      coordinates: sanitizeForFirestore(tweet.coordinates), // FIXED: Sanitize coordinates
      
      // Thread information (will be updated if part of thread)
      isThread: false,
      threadId: null,
      threadPosition: null,
      threadTotalParts: null,
      
      // Processing metadata
      collectedBy: 'blackswan-twitter-scraping-service',
      processingVersion: '1.0.0'
    };

    // Add collection-specific context
    if (isTokenMode) {
      baseDoc.collectionContext = {
        method: 'token_account_search',
        sourceTokenAccount: metadata.tokenAccount || null,
        searchQuery: metadata.searchQuery || null,
        sourceType: 'token_mention'
      };
      
      // Add token account-specific fields
      if (metadata.tokenAccount) {
        baseDoc.tokenAccountId = metadata.tokenAccount.id;
        baseDoc.tokenSymbol = metadata.tokenAccount.tokenSymbol;
        baseDoc.tokenAddress = metadata.tokenAccount.tokenAddress;
        baseDoc.sourceUsername = metadata.tokenAccount.username;
      }
    } else {
      baseDoc.collectionContext = {
        method: 'account_monitoring',
        sourceAccount: metadata.username || null,
        sourceType: 'monitored_account'
      };
    }

    return baseDoc;
  }

  /**
   * Prepare thread document
   */
  prepareThreadDocument(thread, metadata, isTokenMode = false) {
    const baseDoc = {
      threadId: thread.id,
      mainTweetId: thread.rootTweet.id,
      combinedText: thread.combinedText,
      totalParts: thread.totalParts,
      isComplete: true,
      createdAt: Timestamp.fromDate(new Date(thread.createdAt * 1000)),
      lastUpdated: Timestamp.fromDate(new Date(thread.lastUpdated * 1000)),
      
      // Aggregate metrics
      likes: thread.metrics.likes,
      retweets: thread.metrics.retweets,
      replies: thread.metrics.replies,
      views: thread.metrics.views,
      
      // Content aggregates
      hashtags: [...new Set(thread.tweets.flatMap(t => t.hashtags || []))],
      mentions: [...new Set(thread.tweets.flatMap(t => t.mentions || []))],
      urls: [...new Set(thread.tweets.flatMap(t => t.urls || []))],
      
      // Thread metadata
      detectionMethods: thread.metadata.detectionMethods,
      hasNumbering: thread.metadata.hasNumbering,
      isReplyChain: thread.metadata.isReplyChain,
      
      // Processing metadata
      collectedAt: FieldValue.serverTimestamp(),
      collectedBy: 'blackswan-twitter-scraping-service',
      processingVersion: '1.0.0',
      documentType: 'thread'
    };

    // Add collection-specific context
    if (isTokenMode) {
      baseDoc.collectionContext = {
        method: 'token_account_search',
        sourceTokenAccount: metadata.tokenAccount || null,
        sourceType: 'token_mention'
      };
      
      if (metadata.tokenAccount) {
        baseDoc.tokenAccountId = metadata.tokenAccount.id;
        baseDoc.tokenSymbol = metadata.tokenAccount.tokenSymbol;
        baseDoc.tokenAddress = metadata.tokenAccount.tokenAddress;
        baseDoc.sourceUsername = metadata.tokenAccount.username;
      }
    } else {
      baseDoc.collectionContext = {
        method: 'account_monitoring',
        sourceAccount: metadata.username || null,
        sourceType: 'monitored_account'
      };
      
      baseDoc.username = metadata.username;
    }

    return baseDoc;
  }
}

/**
 * Mock Agent Runtime Class for Twitter Plugin Compatibility
 */
class MockAgentRuntime {
  constructor() {
    this.agentId = "blackswan-twitter-monitor";
    this.character = {
      name: "BlackSwanTwitterBot",
      templates: {},
      settings: {},
      secrets: {},
    };
    this.settings = new Map();
    this.cache = new Map();
    this.entities = new Map();
    this.memories = new Map();
    this.worlds = new Map();
  }

  getSetting(key) {
    return process.env[key] || this.settings.get(key);
  }

  setSetting(key, value, persist = true) {
    this.settings.set(key, value);
  }

  getCache(key) {
    return Promise.resolve(this.cache.get(key));
  }

  setCache(key, value) {
    this.cache.set(key, value);
    return Promise.resolve();
  }

  async getEntityById(entityId) {
    return this.entities.get(entityId) || null;
  }

  async updateEntity(entity) {
    this.entities.set(entity.id, entity);
    return Promise.resolve();
  }

  async getMemoryById(memoryId) {
    return this.memories.get(memoryId) || null;
  }

  async createMemory(memory, tableName = "messages") {
    this.memories.set(memory.id, { ...memory, tableName });
    return Promise.resolve();
  }

  async getMemories(options) {
    const allMemories = Array.from(this.memories.values());
    return allMemories
      .filter((m) => {
        if (options.roomId && m.roomId !== options.roomId) return false;
        if (options.tableName && m.tableName !== options.tableName) return false;
        return true;
      })
      .slice(0, options.count || 10);
  }

  async getMemoriesByRoomIds(options) {
    const allMemories = Array.from(this.memories.values());
    return allMemories.filter((m) => {
      if (options.tableName && m.tableName !== options.tableName) return false;
      if (options.roomIds && !options.roomIds.includes(m.roomId)) return false;
      return true;
    });
  }

  async ensureWorldExists(world) {
    this.worlds.set(world.id, world);
    return Promise.resolve();
  }

  async ensureAgentExists() {
    const agent = {
      id: this.agentId,
      names: [this.character.name],
      metadata: {},
    };
    this.entities.set(this.agentId, agent);
    return Promise.resolve();
  }

  async ensureConnection() { return Promise.resolve(); }
  async evaluate() { return Promise.resolve(); }
  async emitEvent() { return Promise.resolve(); }
}

/**
 * Twitter Service Manager
 * 
 * This class manages all Twitter API interactions including authentication,
 * tweet fetching, and search operations. It provides a unified interface
 * for the Twitter plugin and handles both cookie-based and credential-based
 * authentication methods.
 * 
 * Key Features:
 * - Dual authentication methods (cookies preferred, credentials fallback)
 * - Automatic authentication retry and error handling
 * - Rate limiting and request management
 * - Tweet fetching from user timelines
 * - Twitter search functionality for token mentions
 * - Session management and cleanup
 * 
 * Authentication Methods:
 * 1. Cookie-based (Preferred): Uses auth_token, ct0, and guest_id from browser
 * 2. Credential-based (Fallback): Uses username, password, email, and 2FA
 */
class TwitterServiceManager {
  /**
   * Initialize the Twitter service manager
   * 
   * Sets up the initial state for Twitter authentication and API interactions.
   * All properties start as null/false and are populated during authentication.
   */
  constructor() {
    this.twitterService = null;           // Twitter plugin service instance
    this.twitterClient = null;            // Twitter client for API operations
    this.runtime = null;                  // Mock runtime for Twitter plugin
    this.isAuthenticated = false;         // Authentication status flag
    this.isInitialized = false;           // Initialization status flag
    this.authenticationInProgress = false; // Prevents concurrent authentication attempts
  }

  async authenticate() {
    if (this.authenticationInProgress) {
      throw new Error("Authentication already in progress");
    }

    if (this.isAuthenticated) {
      console.log("🔐 Twitter client already authenticated");
      return true;
    }

    this.authenticationInProgress = true;

    try {
      console.log("🔐 Initializing Twitter client...");
      this.runtime = new MockAgentRuntime();

      const authToken = TWITTER_COOKIES_AUTH_TOKEN;
      const ct0Token = TWITTER_COOKIES_CT0;
      const guestId = TWITTER_COOKIES_GUEST_ID;

      const hasCookies = authToken && ct0Token && guestId;

      if (hasCookies) {
        console.log("🍪 Using cookie authentication...");
        return await this.authenticateWithCookies(authToken, ct0Token, guestId);
      } else {
        console.log("🔑 Using username/password authentication...");
        return await this.authenticateWithCredentials();
      }
    } catch (error) {
      console.error("❌ Authentication failed:", error.message);
      this.isAuthenticated = false;
      this.isInitialized = false;
      throw error;
    } finally {
      this.authenticationInProgress = false;
    }
  }

  async authenticateWithCookies(authToken, ct0Token, guestId) {
    try {
      const cookiesArray = [
        {
          name: "auth_token",
          value: authToken,
          domain: ".x.com",
          path: "/",
          secure: true,
          httpOnly: true,
          sameSite: "None",
        },
        {
          name: "ct0",
          value: ct0Token,
          domain: ".x.com",
          path: "/",
          secure: true,
          httpOnly: false,
          sameSite: "Lax",
        },
        {
          name: "guest_id",
          value: guestId,
          domain: ".x.com",
          path: "/",
          secure: true,
          httpOnly: false,
          sameSite: "None",
        },
      ];

      const cookiesJson = JSON.stringify(cookiesArray);
      this.runtime.setSetting("TWITTER_COOKIES", cookiesJson);
      this.runtime.setSetting("TWITTER_DRY_RUN", false);
      this.runtime.setSetting("TWITTER_ENABLE_POST_GENERATION", false);
      this.runtime.setSetting("TWITTER_INTERACTION_ENABLE", false);
      this.runtime.setSetting("TWITTER_TIMELINE_ENABLE", false);
      this.runtime.setSetting("TWITTER_SPACES_ENABLE", false);

      console.log("🍪 Initializing Twitter service with cookies...");

      const TwitterService = twitterPlugin.services[0];
      this.twitterService = await TwitterService.start(this.runtime);

      this.twitterClient = this.twitterService.getClient(
        this.runtime.agentId,
        this.runtime.agentId
      );

      if (!this.twitterClient) {
        throw new Error("Failed to create Twitter client instance with cookies");
      }

      const isLoggedIn = await this.twitterClient.client.twitterClient.isLoggedIn();

      if (!isLoggedIn) {
        throw new Error("Cookie authentication failed - cookies may be expired or invalid");
      }

      this.isAuthenticated = true;
      this.isInitialized = true;
      console.log("✅ Cookie authentication successful!");

      return true;
    } catch (error) {
      console.error("❌ Cookie authentication failed:", error.message);
      console.log("🔄 Falling back to username/password authentication...");
      return await this.authenticateWithCredentials();
    }
  }

  async authenticateWithCredentials() {
    const credentials = {
      TWITTER_USERNAME: TWITTER_USERNAME,
      TWITTER_PASSWORD: TWITTER_PASSWORD,
      TWITTER_EMAIL: TWITTER_EMAIL,
      TWITTER_2FA_SECRET: TWITTER_2FA_SECRET,
      TWITTER_DRY_RUN: false,
      TWITTER_ENABLE_POST_GENERATION: false,
      TWITTER_INTERACTION_ENABLE: false,
      TWITTER_TIMELINE_ENABLE: false,
      TWITTER_SPACES_ENABLE: false,
    };

    for (const [key, value] of Object.entries(credentials)) {
      this.runtime.setSetting(key, value);
    }

    if (!credentials.TWITTER_USERNAME || !credentials.TWITTER_PASSWORD || !credentials.TWITTER_EMAIL) {
      throw new Error("Missing required credentials. Please check your .env file.");
    }

    console.log(`📧 Logging in as: ${credentials.TWITTER_USERNAME}`);

    const TwitterService = twitterPlugin.services[0];
    this.twitterService = await TwitterService.start(this.runtime);

    this.twitterClient = this.twitterService.getClient(
      this.runtime.agentId,
      this.runtime.agentId
    );

    if (!this.twitterClient) {
      throw new Error("Failed to create Twitter client instance");
    }

    this.isAuthenticated = true;
    this.isInitialized = true;
    console.log("✅ Credential authentication successful!");

    return true;
  }

  async getUserTweets(username, limit = PROCESSING_CONFIG.MAX_TWEETS_PER_ACCOUNT) {
    if (!this.isAuthenticated) {
      throw new Error("Client not authenticated. Call authenticate() first.");
    }

    try {
      const tweets = [];
      const tweetGenerator = this.twitterClient.client.twitterClient.getTweets(username, limit);

      for await (const tweet of tweetGenerator) {
        tweets.push(tweet);
        if (tweets.length >= limit) break;
      }

      return tweets;
    } catch (error) {
      console.error(`❌ Error fetching tweets for @${username}:`, error.message);
      throw error;
    }
  }

  async searchTweets(query, limit = PROCESSING_CONFIG.MAX_TWEETS_PER_TOKEN_SEARCH) {
    if (!this.isAuthenticated) {
      throw new Error("Client not authenticated. Call authenticate() first.");
    }

    try {
      const tweets = [];
      const tweetIterator = this.twitterClient.client.twitterClient.searchTweets(
        query, 
        limit,
        0 // searchMode: 0 = Top
      );

      for await (const tweet of tweetIterator) {
        tweets.push(tweet);
        if (tweets.length >= limit) break;
      }

      return tweets;
    } catch (error) {
      console.error(`❌ Twitter search error for "${query}":`, error.message);
      
      if (error.message?.includes('rate limit') || error.message?.includes('Rate limit')) {
        console.warn(`⚠️ Rate limited for query "${query}", skipping...`);
        return [];
      }
      
      console.warn(`⚠️ Search failed for "${query}": ${error.message}`);
      return [];
    }
  }

  async logout() {
    try {
      if (this.twitterClient && this.isAuthenticated) {
        try {
          await this.twitterClient.client.twitterClient.logout();
          console.log("👋 Logged out successfully");
        } catch (error) {
          console.error("⚠️ Logout error:", error.message);
        }
      }

      this.twitterService = null;
      this.twitterClient = null;
      this.runtime = null;
      this.isAuthenticated = false;
      this.isInitialized = false;

      return true;
    } catch (error) {
      console.error("⚠️ Cleanup error:", error.message);
      this.twitterService = null;
      this.twitterClient = null;
      this.runtime = null;
      this.isAuthenticated = false;
      this.isInitialized = false;
      return false;
    }
  }

  getStatus() {
    return {
      isInitialized: this.isInitialized,
      isAuthenticated: this.isAuthenticated,
      authenticationInProgress: this.authenticationInProgress,
      authenticatedUser: this.isAuthenticated ? TWITTER_USERNAME : null,
      lastStatusCheck: new Date().toISOString(),
    };
  }
}

/**
 * Account Manager
 * Manages Twitter accounts to monitor from Firestore
 */
class AccountManager {
  constructor() {
    this.accounts = [];
    this.lastFetchTime = 0;
    this.accountStatusMap = new Map();
  }

  async loadAccountsFromFirestore() {
    try {
      console.log('📋 Loading Twitter accounts from Firestore...');
      
      const snapshot = await db.collection('twitter_accounts')
        .where('isActive', '==', true)
        .orderBy('priority', 'desc')
        .get();

      this.accounts = [];
      snapshot.forEach(doc => {
        const data = doc.data();
        this.accounts.push({
          id: doc.id,
          username: data.username,
          priority: data.priority || 1,
          category: data.category || 'general',
          isActive: data.isActive !== false,
          lastProcessed: data.lastProcessed?.toDate() || null,
          processCount: data.processCount || 0,
          errorCount: data.errorCount || 0,
          lastError: data.lastError || null
        });
      });

      this.lastFetchTime = Date.now();
      console.log(`✅ Loaded ${this.accounts.length} active Twitter accounts`);
      
      return this.accounts;
    } catch (error) {
      console.error('❌ Error loading accounts from Firestore:', error);
      return [];
    }
  }

  async getAccountsToProcess() {
    if (Date.now() - this.lastFetchTime > 10 * 60 * 1000) {
      await this.loadAccountsFromFirestore();
    }

    const now = Date.now();
    const cutoffTime = now - 60 * 1000;

    return this.accounts.filter(account => {
      const lastProcessed = this.accountStatusMap.get(account.username)?.lastProcessed || 0;
      return lastProcessed < cutoffTime;
    });
  }

  async updateAccountStatus(username, success, error = null, tweetCount = 0) {
    const now = Date.now();
    
    this.accountStatusMap.set(username, {
      lastProcessed: now,
      success,
      error,
      tweetCount
    });

    try {
      const accountDoc = this.accounts.find(acc => acc.username === username);
      if (accountDoc) {
        await db.collection('twitter_accounts').doc(accountDoc.id).update({
          lastProcessed: FieldValue.serverTimestamp(),
          processCount: FieldValue.increment(1),
          ...(success ? {} : { 
            errorCount: FieldValue.increment(1),
            lastError: error 
          }),
          ...(tweetCount > 0 ? { lastTweetCount: tweetCount } : {})
        });
      }
    } catch (error) {
      console.error(`❌ Error updating account status for @${username}:`, error);
    }
  }

  getProcessingStats() {
    const total = this.accounts.length;
    const processed = this.accountStatusMap.size;
    const recentlyProcessed = Array.from(this.accountStatusMap.values())
      .filter(status => Date.now() - status.lastProcessed < 60000).length;

    return {
      totalAccounts: total,
      processedAccounts: processed,
      recentlyProcessed,
      accountsPerMinute: recentlyProcessed
    };
  }
}

/**
 * Token Accounts Manager
 * Manages token accounts to monitor from Firestore (similar to AccountManager)
 */
class TokenAccountsManager {
  constructor() {
    this.tokenAccounts = [];
    this.lastFetchTime = 0;
    this.tokenAccountStatusMap = new Map();
  }

  async loadTokenAccountsFromFirestore() {
    try {
      console.log('📋 Loading token accounts from twitter_token_accounts...');
      
      const snapshot = await db.collection('twitter_token_accounts')
        .where('isActive', '==', true)
        .orderBy('priority', 'desc')
        .get();

      this.tokenAccounts = [];
      snapshot.forEach(doc => {
        const data = doc.data();
        this.tokenAccounts.push({
          id: doc.id,
          username: data.username,
          tokenSymbol: data.tokenSymbol,
          tokenAddress: data.tokenAddress,
          priority: data.priority || 1,
          category: data.category || 'token',
          isActive: data.isActive !== false,
          lastProcessed: data.lastProcessed?.toDate() || null,
          processCount: data.processCount || 0,
          errorCount: data.errorCount || 0,
          lastError: data.lastError || null
        });
      });

      this.lastFetchTime = Date.now();
      console.log(`✅ Loaded ${this.tokenAccounts.length} active token accounts`);
      
      return this.tokenAccounts;
    } catch (error) {
      console.error('❌ Error loading token accounts from Firestore:', error);
      return [];
    }
  }

  async getTokenAccountsToProcess() {
    if (Date.now() - this.lastFetchTime > 10 * 60 * 1000) {
      await this.loadTokenAccountsFromFirestore();
    }

    const now = Date.now();
    const cutoffTime = now - 60 * 1000;

    return this.tokenAccounts.filter(tokenAccount => {
      const lastProcessed = this.tokenAccountStatusMap.get(tokenAccount.id)?.lastProcessed || 0;
      return lastProcessed < cutoffTime;
    });
  }

  async updateTokenAccountStatus(tokenAccountId, success, error = null, tweetCount = 0) {
    const now = Date.now();
    
    this.tokenAccountStatusMap.set(tokenAccountId, {
      lastProcessed: now,
      success,
      error,
      tweetCount
    });

    try {
      const tokenAccount = this.tokenAccounts.find(acc => acc.id === tokenAccountId);
      if (tokenAccount) {
        await db.collection('twitter_token_accounts').doc(tokenAccountId).update({
          lastProcessed: FieldValue.serverTimestamp(),
          processCount: FieldValue.increment(1),
          ...(success ? {} : { 
            errorCount: FieldValue.increment(1),
            lastError: error 
          }),
          ...(tweetCount > 0 ? { lastTweetCount: tweetCount } : {})
        });
      }
    } catch (error) {
      console.error(`❌ Error updating token account status for ${tokenAccountId}:`, error);
    }
  }

  getProcessingStats() {
    const total = this.tokenAccounts.length;
    const processed = this.tokenAccountStatusMap.size;
    const recentlyProcessed = Array.from(this.tokenAccountStatusMap.values())
      .filter(status => Date.now() - status.lastProcessed < 60000).length;

    return {
      totalTokenAccounts: total,
      processedTokenAccounts: processed,
      recentlyProcessed,
      tokenAccountsPerMinute: recentlyProcessed
    };
  }

  getAllTokenAccounts() {
    return this.tokenAccounts;
  }

  getTokenAccount(tokenAccountId) {
    return this.tokenAccounts.find(acc => acc.id === tokenAccountId);
  }
}

/**
 * Improved Token Account Processor
 */
class ImprovedTokenAccountProcessor {
  constructor(twitterManager, storageManager, threadDetector) {
    this.twitterManager = twitterManager;
    this.storageManager = storageManager;
    this.threadDetector = threadDetector;
  }

  async searchTweetsForTokenAccount(tokenAccount) {
    try {
      console.log(`🔍 Searching tweets for token account @${tokenAccount.username} (${tokenAccount.tokenSymbol})...`);

      const searchQueries = [
        `${tokenAccount.tokenSymbol.toUpperCase()}`,
        tokenAccount.tokenAddress
      ];

      let allTweets = [];

      for (const query of searchQueries) {
        try {
          console.log(`🔍 Searching for: "${query}"`);
          const tweets = await this.twitterManager.searchTweets(query);
          
          if (tweets && tweets.length > 0) {
            // Add token account metadata to each tweet
            const enrichedTweets = tweets.map(tweet => ({
              ...tweet,
              searchQuery: query,
              foundTokenAccount: tokenAccount
            }));
            
            console.log(`📱 Found ${tweets.length} tweets for query "${query}"`);
            allTweets = allTweets.concat(enrichedTweets);
          }
          
          await this.delay(1000);
        } catch (error) {
          console.error(`❌ Error searching for "${query}":`, error.message);
        }
      }

      if (allTweets.length === 0) {
        console.log(`⏰ No tweets found for token account ${tokenAccount.tokenSymbol}`);
        return { tweetsSaved: 0, threadsSaved: 0 };
      }

      // Remove duplicates and filter by age
      const uniqueTweets = this.removeDuplicateTweets(allTweets);
      const cutoffTime = (Date.now() / 1000) - (PROCESSING_CONFIG.TWEET_LOOKBACK_MINUTES * 60);
      const recentTweets = uniqueTweets.filter(tweet => tweet.timestamp >= cutoffTime);

      if (recentTweets.length === 0) {
        console.log(`⏰ No recent tweets found for token account ${tokenAccount.tokenSymbol} in last 24 hours`);
        return { tweetsSaved: 0, threadsSaved: 0 };
      }

      console.log(`📱 Found ${recentTweets.length} unique recent tweets for token account ${tokenAccount.tokenSymbol}`);

      // Detect threads with improved error handling
      let threadAnalysis;
      try {
        threadAnalysis = await this.threadDetector.detectThreads(
          recentTweets, 
          `TOKEN_ACCOUNT_${tokenAccount.tokenSymbol}`
        );
      } catch (error) {
        console.error(`❌ Thread detection failed for ${tokenAccount.tokenSymbol}:`, error.message);
        // FIXED: Fall back to treating all tweets as standalone
        threadAnalysis = {
          threads: [],
          standalone: recentTweets,
          stats: {
            totalTweets: recentTweets.length,
            threadsFound: 0,
            standaloneTweets: recentTweets.length,
            totalThreadTweets: 0
          }
        };
      }

      let tweetsSaved = 0;
      let threadsSaved = 0;

      const metadata = { tokenAccount };

      // Save threads with error handling
      for (const thread of threadAnalysis.threads) {
        try {
          await this.storageManager.saveThread(thread, metadata, true);
          threadsSaved++;
        } catch (error) {
          console.error(`❌ Failed to save thread for ${tokenAccount.tokenSymbol}:`, error.message);
        }
      }

      // Save standalone tweets with error handling
      if (threadAnalysis.standalone.length > 0) {
        try {
          const saveResult = await this.storageManager.saveTweets(
            threadAnalysis.standalone, 
            metadata, 
            true
          );
          tweetsSaved = saveResult.saved.length;
        } catch (error) {
          console.error(`❌ Failed to save tweets for ${tokenAccount.tokenSymbol}:`, error.message);
          // FIXED: Try to save tweets individually on batch failure
          console.log(`🔄 Attempting individual tweet saves for ${tokenAccount.tokenSymbol}...`);
          for (const tweet of threadAnalysis.standalone) {
            try {
              const individualResult = await this.storageManager.saveTweets([tweet], metadata, true);
              tweetsSaved += individualResult.saved.length;
            } catch (individualError) {
              console.error(`❌ Individual tweet save failed for ${tweet.id}:`, individualError.message);
            }
          }
        }
      }

      return { tweetsSaved, threadsSaved };

    } catch (error) {
      console.error(`❌ Error processing token account ${tokenAccount.tokenSymbol}:`, error.message);
      throw error;
    }
  }

  removeDuplicateTweets(tweets) {
    const seen = new Set();
    return tweets.filter(tweet => {
      if (seen.has(tweet.id)) {
        return false;
      }
      seen.add(tweet.id);
      return true;
    });
  }

  delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

/**
 * Log Streamer Classes (keeping original implementations)
 */
class LogStreamer extends EventEmitter {
  constructor() {
    super();
    this.clients = new Set();
    this.logBuffer = [];
    this.maxBufferSize = 1000;
    this.originalConsole = {};
    
    this.originalConsole.log = console.log;
    this.originalConsole.error = console.error;
    this.originalConsole.warn = console.warn;
    this.originalConsole.info = console.info;
    this.originalConsole.debug = console.debug;
    
    this.setupConsoleCapture();
  }

  setupConsoleCapture() {
    const self = this;
    
    const wrapConsoleMethod = (methodName, originalMethod) => {
      return function(...args) {
        originalMethod.apply(console, args);
        
        const logEntry = {
          timestamp: new Date().toISOString(),
          level: methodName.toUpperCase(),
          message: args.map(arg => 
            typeof arg === 'object' ? JSON.stringify(arg, null, 2) : String(arg)
          ).join(' '),
          raw_args: args
        };
        
        self.addToBuffer(logEntry);
        self.emit('log', logEntry);
      };
    };
    
    console.log = wrapConsoleMethod('log', this.originalConsole.log);
    console.error = wrapConsoleMethod('error', this.originalConsole.error);
    console.warn = wrapConsoleMethod('warn', this.originalConsole.warn);
    console.info = wrapConsoleMethod('info', this.originalConsole.info);
    console.debug = wrapConsoleMethod('debug', this.originalConsole.debug);
  }

  addToBuffer(logEntry) {
    this.logBuffer.push(logEntry);
    
    if (this.logBuffer.length > this.maxBufferSize) {
      this.logBuffer = this.logBuffer.slice(-this.maxBufferSize);
    }
  }

  addClient(response) {
    const clientId = Date.now() + Math.random();
    
    const client = {
      id: clientId,
      response: response,
      connected: true
    };
    
    this.clients.add(client);
    
    response.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-transform',
      'Connection': 'keep-alive',
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, OPTIONS',
      'Access-Control-Allow-Headers': 'Accept, Cache-Control, Content-Type, X-Requested-With',
      'Access-Control-Expose-Headers': 'Content-Type',
      'X-Accel-Buffering': 'no'
    });
    
    this.sendToClient(client, {
      timestamp: new Date().toISOString(),
      level: 'INFO',
      message: '🚀 Connected to Twitter Scraping Service log stream'
    });
    
    this.logBuffer.forEach(logEntry => {
      this.sendToClient(client, logEntry);
    });
    
    response.on('close', () => {
      client.connected = false;
      this.clients.delete(client);
      console.log(`📱 [LOG_STREAM] Client ${clientId} disconnected (${this.clients.size} remaining)`);
    });
    
    console.log(`📱 [LOG_STREAM] Client ${clientId} connected (${this.clients.size} total)`);
    
    return client;
  }

  sendToClient(client, logEntry) {
    if (!client.connected) return;
    
    try {
      const data = JSON.stringify(logEntry);
      client.response.write(`data: ${data}\n\n`);
    } catch (error) {
      client.connected = false;
      this.clients.delete(client);
    }
  }

  broadcastLog(logEntry) {
    const disconnectedClients = Array.from(this.clients).filter(client => !client.connected);
    disconnectedClients.forEach(client => this.clients.delete(client));
    
    this.clients.forEach(client => {
      this.sendToClient(client, logEntry);
    });
  }

  getStats() {
    return {
      connectedClients: this.clients.size,
      bufferSize: this.logBuffer.length,
      maxBufferSize: this.maxBufferSize
    };
  }

  restoreOriginalConsole() {
    console.log = this.originalConsole.log;
    console.error = this.originalConsole.error;
    console.warn = this.originalConsole.warn;
    console.info = this.originalConsole.info;
    console.debug = this.originalConsole.debug;
  }
}

class PublicLogStreamer extends EventEmitter {
  constructor() {
    super();
    this.clients = new Set();
    this.logBuffer = [];
    this.maxBufferSize = 1000;
  }

  emitPublicLog(message, type = 'Info') {
    const logEntry = {
      timestamp: new Date().toISOString(),
      type: type,
      message: message,
    };

    this.addToBuffer(logEntry);
    this.emit("publicLog", logEntry);
    this.broadcastLog(logEntry);
  }

  addToBuffer(logEntry) {
    this.logBuffer.push(logEntry);

    if (this.logBuffer.length > this.maxBufferSize) {
      this.logBuffer = this.logBuffer.slice(-this.maxBufferSize);
    }
  }

  addClient(response) {
    const clientId = Date.now() + Math.random();

    const client = {
      id: clientId,
      response: response,
      connected: true,
    };

    this.clients.add(client);

    response.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache, no-transform",
      "Connection": "keep-alive",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, OPTIONS",
      "Access-Control-Allow-Headers": "Accept, Cache-Control, Content-Type, X-Requested-With",
      "Access-Control-Expose-Headers": "Content-Type",
      "X-Accel-Buffering": "no",
    });

    this.sendToClient(client, {
      timestamp: new Date().toISOString(),
      type: "Info",
      message: "Connected to Twitter Public Stream",
    });

    this.logBuffer.forEach((logEntry) => {
      this.sendToClient(client, logEntry);
    });

    response.on("close", () => {
      client.connected = false;
      this.clients.delete(client);
    });

    return client;
  }

  sendToClient(client, logEntry) {
    if (!client.connected) return;

    try {
      const data = JSON.stringify(logEntry);
      client.response.write(`data: ${data}\n\n`);
    } catch (error) {
      client.connected = false;
      this.clients.delete(client);
    }
  }

  broadcastLog(logEntry) {
    const disconnectedClients = Array.from(this.clients).filter(
      (client) => !client.connected
    );
    disconnectedClients.forEach((client) => this.clients.delete(client));

    this.clients.forEach((client) => {
      this.sendToClient(client, logEntry);
    });
  }

  getStats() {
    return {
      connectedClients: this.clients.size,
      bufferSize: this.logBuffer.length,
      maxBufferSize: this.maxBufferSize,
    };
  }
}

/**
 * Main Improved Twitter Processing Engine
 * 
 * This is the core orchestrator class that manages the entire Twitter monitoring
 * system. It coordinates all components including authentication, data collection,
 * processing, and storage operations.
 * 
 * Architecture:
 * - TwitterServiceManager: Handles Twitter API authentication and data fetching
 * - EnhancedDuplicateDetector: Provides intelligent duplicate detection
 * - ImprovedThreadDetector: Detects and combines tweet threads
 * - UnifiedTweetStorageManager: Manages data storage in Firestore
 * - AccountManager: Manages monitored Twitter accounts
 * - TokenAccountsManager: Manages token-specific accounts
 * - ImprovedTokenAccountProcessor: Processes token-related tweets
 * 
 * Processing Flow:
 * 1. Authenticate with Twitter API
 * 2. Load accounts from Firestore
 * 3. Process accounts sequentially with rate limiting
 * 4. Detect threads and filter duplicates
 * 5. Store new tweets and threads in Firestore
 * 6. Update account processing statistics
 * 7. Repeat cycle based on configured interval
 */
class ImprovedTwitterProcessingEngine {
  /**
   * Initialize the Twitter processing engine with all required components
   * 
   * @param {PublicLogStreamer} publicLogStreamer - Optional log streamer for public logs
   */
  constructor(publicLogStreamer = null) {
    // Core service managers
    this.publicLogStreamer = publicLogStreamer;                    // Public log streaming for real-time updates
    this.twitterManager = new TwitterServiceManager();             // Twitter API authentication and operations
    this.duplicateDetector = new EnhancedDuplicateDetector();      // Intelligent duplicate detection
    this.threadDetector = new ImprovedThreadDetector();            // Thread detection and combination
    this.storageManager = new UnifiedTweetStorageManager(this.duplicateDetector); // Data storage management
    
    // Account management
    this.accountManager = new AccountManager();                    // Monitored Twitter accounts
    this.tokenAccountsManager = new TokenAccountsManager();        // Token-specific accounts
    this.tokenAccountProcessor = new ImprovedTokenAccountProcessor( // Token tweet processing
      this.twitterManager, 
      this.storageManager, 
      this.threadDetector
    );
    
    // Processing state
    this.isRunning = false;                    // Main processing loop status
    this.isTokenProcessingEnabled = false;     // Token processing toggle
    
    // Performance and statistics tracking
    this.processingStats = {
      // Account processing statistics
      totalProcessed: 0,                       // Total accounts processed
      totalTweetsSaved: 0,                     // Total tweets saved to database
      totalThreadsSaved: 0,                    // Total threads saved to database
      
      // Token processing statistics
      totalTokenAccountsProcessed: 0,          // Total token accounts processed
      totalTokenTweetsSaved: 0,                // Total token tweets saved
      totalTokenThreadsSaved: 0,               // Total token threads saved
      
      // Error tracking
      errors: 0,                               // Total account processing errors
      tokenAccountErrors: 0,                   // Total token account processing errors
      
      // Timing information
      startTime: null,                         // Engine start timestamp
      lastProcessingTime: null,                // Last account processing timestamp
      lastTokenAccountProcessingTime: null,    // Last token processing timestamp
      
      // Performance metrics
      duplicatesFiltered: 0,                   // Total duplicates filtered out
      cacheHits: 0                            // Total cache hits for performance
    };
  }

  async start() {
    if (this.isRunning) {
      console.log('⚠️ Processing engine already running');
      return;
    }

    try {
      console.log('🚀 Starting Improved BlackSwan Twitter Processing Engine v2.0...');
      
      await this.twitterManager.authenticate();
      await this.accountManager.loadAccountsFromFirestore();
      await this.tokenAccountsManager.loadTokenAccountsFromFirestore();
      
      this.isRunning = true;
      this.processingStats.startTime = Date.now();
      
      this.startProcessingLoop();
      
      console.log('✅ Improved Twitter Processing Engine started successfully');
      console.log('🆕 New features: Enhanced thread detection, consolidated collections, faster duplicate filtering');
    } catch (error) {
      console.error('❌ Failed to start processing engine:', error);
      throw error;
    }
  }

  async startProcessingLoop() {
    console.log('🔄 Starting improved sequential processing loop...');
    
    while (this.isRunning) {
      try {
        console.log('\n📊 === ACCOUNTS PROCESSING PHASE ===');
        await this.processAccountBatch();
        
        if (this.isTokenProcessingEnabled) {
          console.log('\n🪙 === TOKEN ACCOUNTS PROCESSING PHASE ===');
          await this.processTokenAccountBatch();
        }
        
        console.log('⏳ Waiting for next processing cycle...');
        await this.delay(PROCESSING_CONFIG.PROCESSING_INTERVAL);
      } catch (error) {
        console.error('❌ Error in processing loop:', error);
        this.processingStats.errors++;
        await this.delay(10000);
      }
    }
  }

  async processAccountBatch() {
    const startTime = Date.now();
    console.log('\n📊 Starting improved account processing batch...');
    
    const accountsToProcess = await this.accountManager.getAccountsToProcess();
    
    if (accountsToProcess.length === 0) {
      console.log('⏳ No accounts ready for processing');
      return;
    }

    console.log(`🎯 Processing ALL ${accountsToProcess.length} accounts...`);

    let batchStats = {
      processed: 0,
      tweetsSaved: 0,
      threadsSaved: 0,
      errors: 0,
      duplicatesFiltered: 0,
      cacheHits: 0
    };

    for (const account of accountsToProcess) {
      try {
        const result = await this.processAccount(account);
        
        batchStats.processed++;
        batchStats.tweetsSaved += result.tweetsSaved;
        batchStats.threadsSaved += result.threadsSaved;
        batchStats.duplicatesFiltered += result.duplicatesFiltered || 0;
        batchStats.cacheHits += result.cacheHits || 0;
        
        await this.accountManager.updateAccountStatus(
          account.username, 
          true, 
          null, 
          result.tweetsSaved + result.threadsSaved
        );
        
        await this.delay(PROCESSING_CONFIG.RATE_LIMIT_DELAY);
        
      } catch (error) {
        console.error(`❌ Error processing @${account.username}:`, error.message);
        batchStats.errors++;
        
        await this.accountManager.updateAccountStatus(
          account.username, 
          false, 
          error.message
        );
      }
    }

    this.processingStats.totalProcessed += batchStats.processed;
    this.processingStats.totalTweetsSaved += batchStats.tweetsSaved;
    this.processingStats.totalThreadsSaved += batchStats.threadsSaved;
    this.processingStats.errors += batchStats.errors;
    this.processingStats.duplicatesFiltered += batchStats.duplicatesFiltered;
    this.processingStats.cacheHits += batchStats.cacheHits;
    this.processingStats.lastProcessingTime = Date.now();

    const processingTime = Date.now() - startTime;
    const accountsPerMinute = this.accountManager.getProcessingStats().accountsPerMinute;

    console.log(`✅ Account batch completed in ${processingTime}ms`);
    console.log(`📈 Improved stats: ${batchStats.processed} accounts, ${batchStats.tweetsSaved} tweets, ${batchStats.threadsSaved} threads`);
    console.log(`🚀 Performance: ${batchStats.duplicatesFiltered} duplicates filtered, ${batchStats.cacheHits} cache hits`);
    console.log(`⚡ Current rate: ${accountsPerMinute} accounts/minute`);
  }

  async processAccount(account) {
    const startTime = Date.now();
    console.log(`🔍 Processing @${account.username}...`);

    try {
      const tweets = await this.twitterManager.getUserTweets(
        account.username, 
        PROCESSING_CONFIG.MAX_TWEETS_PER_ACCOUNT
      );

      if (tweets.length === 0) {
        console.log(`📭 No recent tweets found for @${account.username}`);
        return { tweetsSaved: 0, threadsSaved: 0, duplicatesFiltered: 0, cacheHits: 0 };
      }

      const cutoffTime = (Date.now() / 1000) - (PROCESSING_CONFIG.TWEET_LOOKBACK_MINUTES * 60);
      const recentTweets = tweets.filter(tweet => tweet.timestamp >= cutoffTime);

      if (recentTweets.length === 0) {
        console.log(`⏰ No tweets in last 24 hours for @${account.username}`);
        return { tweetsSaved: 0, threadsSaved: 0, duplicatesFiltered: 0, cacheHits: 0 };
      }

      console.log(`📱 Found ${recentTweets.length} recent tweets for @${account.username}`);

      // Detect threads with improved error handling
      let threadAnalysis;
      try {
        threadAnalysis = await this.threadDetector.detectThreads(
          recentTweets, 
          account.username
        );
      } catch (error) {
        console.error(`❌ Thread detection failed for @${account.username}:`, error.message);
        // FIXED: Fall back to treating all tweets as standalone
        threadAnalysis = {
          threads: [],
          standalone: recentTweets,
          stats: {
            totalTweets: recentTweets.length,
            threadsFound: 0,
            standaloneTweets: recentTweets.length,
            totalThreadTweets: 0
          }
        };
      }

      let tweetsSaved = 0;
      let threadsSaved = 0;
      let totalDuplicatesFiltered = 0;
      let totalCacheHits = 0;

      const metadata = { username: account.username };

      // Save threads with error handling
      for (const thread of threadAnalysis.threads) {
        try {
          const result = await this.storageManager.saveThread(thread, metadata, false);
          threadsSaved++;
          totalDuplicatesFiltered += result.stats?.duplicates || 0;
          totalCacheHits += result.stats?.cacheHits || 0;
        } catch (error) {
          console.error(`❌ Failed to save thread for @${account.username}:`, error.message);
        }
      }

      // Save standalone tweets with error handling
      if (threadAnalysis.standalone.length > 0) {
        try {
          const saveResult = await this.storageManager.saveTweets(
            threadAnalysis.standalone, 
            metadata, 
            false
          );
          tweetsSaved = saveResult.saved.length;
          totalDuplicatesFiltered += saveResult.stats?.duplicates || 0;
          totalCacheHits += saveResult.stats?.cacheHits || 0;
        } catch (error) {
          console.error(`❌ Failed to save tweets for @${account.username}:`, error.message);
          // FIXED: Try to save tweets individually on batch failure
          console.log(`🔄 Attempting individual tweet saves for @${account.username}...`);
          for (const tweet of threadAnalysis.standalone) {
            try {
              const individualResult = await this.storageManager.saveTweets([tweet], metadata, false);
              tweetsSaved += individualResult.saved.length;
              totalDuplicatesFiltered += individualResult.stats?.duplicates || 0;
              totalCacheHits += individualResult.stats?.cacheHits || 0;
            } catch (individualError) {
              console.error(`❌ Individual tweet save failed for ${tweet.id}:`, individualError.message);
            }
          }
        }
      }

      const processingTime = Date.now() - startTime;
      console.log(`✅ @${account.username} processed in ${processingTime}ms - ${tweetsSaved} tweets, ${threadsSaved} threads`);

      if (this.publicLogStreamer && (tweetsSaved > 0 || threadsSaved > 0)) {
        this.publicLogStreamer.emitPublicLog(`Latest tweets fetched for ${account.username}`);
      }

      return { 
        tweetsSaved, 
        threadsSaved, 
        duplicatesFiltered: totalDuplicatesFiltered,
        cacheHits: totalCacheHits
      };

    } catch (error) {
      console.error(`❌ Failed to process @${account.username}:`, error.message);
      throw error;
    }
  }

  async processTokenAccountBatch() {
    if (!this.isTokenProcessingEnabled) return;
    
    const startTime = Date.now();
    console.log('\n🪙 Starting improved token account processing batch...');
    
    const tokenAccountsToProcess = await this.tokenAccountsManager.getTokenAccountsToProcess();
    
    if (tokenAccountsToProcess.length === 0) {
      console.log('⏳ No token accounts ready for processing');
      return;
    }

    console.log(`🎯 Processing ALL ${tokenAccountsToProcess.length} token accounts...`);

    let batchStats = {
      processed: 0,
      tweetsSaved: 0,
      threadsSaved: 0,
      errors: 0,
      duplicatesFiltered: 0,
      cacheHits: 0
    };

    for (const tokenAccount of tokenAccountsToProcess) {
      try {
        const result = await this.processTokenAccountMentions(tokenAccount);
        
        batchStats.processed++;
        batchStats.tweetsSaved += result.tweetsSaved;
        batchStats.threadsSaved += result.threadsSaved;
        batchStats.duplicatesFiltered += result.duplicatesFiltered || 0;
        batchStats.cacheHits += result.cacheHits || 0;
        
        await this.tokenAccountsManager.updateTokenAccountStatus(
          tokenAccount.id, 
          true, 
          null, 
          result.tweetsSaved + result.threadsSaved
        );
        
        await this.delay(PROCESSING_CONFIG.TOKEN_RATE_LIMIT_DELAY);
        
      } catch (error) {
        console.error(`❌ Error processing token account ${tokenAccount.tokenSymbol}:`, error.message);
        batchStats.errors++;
        
        await this.tokenAccountsManager.updateTokenAccountStatus(
          tokenAccount.id, 
          false, 
          error.message
        );
      }
    }

    this.processingStats.totalTokenAccountsProcessed += batchStats.processed;
    this.processingStats.totalTokenTweetsSaved += batchStats.tweetsSaved;
    this.processingStats.totalTokenThreadsSaved += batchStats.threadsSaved;
    this.processingStats.tokenAccountErrors += batchStats.errors;
    this.processingStats.duplicatesFiltered += batchStats.duplicatesFiltered; 
    this.processingStats.cacheHits += batchStats.cacheHits;
    this.processingStats.lastTokenAccountProcessingTime = Date.now();

    const processingTime = Date.now() - startTime;
    const tokenAccountStats = this.tokenAccountsManager.getProcessingStats();

    console.log(`✅ Token account batch completed in ${processingTime}ms`);
    console.log(`📈 Improved token account stats: ${batchStats.processed} token accounts, ${batchStats.tweetsSaved} tweets, ${batchStats.threadsSaved} threads`);
    console.log(`🚀 Performance: ${batchStats.duplicatesFiltered} duplicates filtered, ${batchStats.cacheHits} cache hits`);
    console.log(`⚡ Current rate: ${tokenAccountStats.tokenAccountsPerMinute} token accounts per minute`);

    if (this.publicLogStreamer && (batchStats.tweetsSaved > 0 || batchStats.threadsSaved > 0)) {
      this.publicLogStreamer.emitPublicLog(`Found ${batchStats.tweetsSaved + batchStats.threadsSaved} new token mention tweets`);
    }
  }

  async processTokenAccountMentions(tokenAccount) {
    const startTime = Date.now();
    console.log(`🔍 Processing improved token account mentions for ${tokenAccount.tokenSymbol}...`);

    try {
      const result = await this.tokenAccountProcessor.searchTweetsForTokenAccount(tokenAccount);
      
      const processingTime = Date.now() - startTime;
      console.log(`✅ ${tokenAccount.tokenSymbol} processed in ${processingTime}ms - ${result.tweetsSaved} tweets, ${result.threadsSaved} threads`);

      return result;

    } catch (error) {
      console.error(`❌ Failed to process token account mentions for ${tokenAccount.tokenSymbol}:`, error.message);
      throw error;
    }
  }

  async stop() {
    console.log('🛑 Stopping Improved Twitter Processing Engine...');
    this.isRunning = false;
    this.isTokenProcessingEnabled = false;
    
    try {
      await this.twitterManager.logout();
      console.log('✅ Improved Twitter Processing Engine stopped');
    } catch (error) {
      console.error('❌ Error stopping processing engine:', error);
    }
  }

  delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  getStats() {
    const uptime = this.processingStats.startTime ? Date.now() - this.processingStats.startTime : 0;
    const accountStats = this.accountManager.getProcessingStats();
    const tokenAccountStats = this.tokenAccountsManager.getProcessingStats();
    
    return {
      version: '1.0.0',
      improvements: {
        enhancedThreadDetection: true,
        consolidatedCollections: true,
        improvedDuplicateDetection: true,
        batchProcessing: true,
        tokenAccountsManager: true
      },
      isRunning: this.isRunning,
      isTokenProcessingEnabled: this.isTokenProcessingEnabled,
      uptime: uptime,
      uptimeHours: (uptime / (1000 * 60 * 60)).toFixed(2),
      twitter: this.twitterManager.getStatus(),
      processing: this.processingStats,
      accounts: accountStats,
      tokenAccounts: tokenAccountStats,
      performance: {
        duplicatesFiltered: this.processingStats.duplicatesFiltered,
        cacheHits: this.processingStats.cacheHits,
        cacheEfficiency: this.processingStats.cacheHits > 0 ? 
          ((this.processingStats.cacheHits / (this.processingStats.cacheHits + this.processingStats.duplicatesFiltered)) * 100).toFixed(2) + '%' : 
          '0%'
      },
      collections: {
        global: 'tweet_global',
        tokens: 'tweet_tokens'
      },
      lastProcessing: this.processingStats.lastProcessingTime ? 
        new Date(this.processingStats.lastProcessingTime).toISOString() : null,
      lastTokenAccountProcessing: this.processingStats.lastTokenAccountProcessingTime ? 
        new Date(this.processingStats.lastTokenAccountProcessingTime).toISOString() : null
    };
  }
}

// ============================================================================
// EXPRESS APPLICATION INITIALIZATION
// ============================================================================

// Initialize Express application with all required components
const app = express();                                                    // Main Express application
const publicLogStreamer = new PublicLogStreamer();                       // Public log streaming for real-time updates
const processingEngine = new ImprovedTwitterProcessingEngine(publicLogStreamer); // Main processing engine
const logStreamer = new LogStreamer();                                   // Internal log streaming for debugging

// Setup log streaming event handlers
// These handlers broadcast logs to connected clients in real-time
logStreamer.on('log', (logEntry) => {
  logStreamer.broadcastLog(logEntry);
});

publicLogStreamer.on('publicLog', (logEntry) => {
  publicLogStreamer.broadcastLog(logEntry);
});

// ============================================================================
// EXPRESS MIDDLEWARE CONFIGURATION
// ============================================================================

// Security middleware - adds various HTTP headers for security
app.use(helmet());

// CORS middleware - allows cross-origin requests from web applications
app.use(cors());

// JSON parsing middleware - parses JSON request bodies up to 10MB
app.use(express.json({ limit: '10mb' }));

// URL-encoded parsing middleware - parses form data up to 10MB
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// Rate limiting middleware - prevents abuse and ensures fair usage
// Configured with very high limits for internal service usage
const limiter = rateLimit({
  windowMs: 1 * 60 * 1000,        // Time window: 1 minute
  max: 100000000,                  // Maximum requests per window (very high for internal use)
  message: {
    error: 'Too many requests from this IP, please try again later.',
    retryAfter: '15 minutes',
  },
  standardHeaders: true,           // Include rate limit info in response headers
  legacyHeaders: false,            // Disable legacy headers
});
app.use(limiter);

// Request logging middleware - logs all incoming requests for monitoring
app.use((req, res, next) => {
  const timestamp = new Date().toISOString();
  console.log(`📡 [${timestamp}] ${req.method} ${req.path} - IP: ${req.ip}`);
  next();
});

// ============================================================================
// API ROUTES AND ENDPOINTS
// ============================================================================

// CORS preflight handlers for Server-Sent Events endpoints
// These handle OPTIONS requests for cross-origin SSE connections

// CORS preflight for internal log streaming endpoint
app.options("/logs/stream", (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Accept, Cache-Control, Content-Type, X-Requested-With");
  res.setHeader("Access-Control-Max-Age", "86400");  // Cache preflight for 24 hours
  res.status(204).end();
});

// CORS preflight for public log streaming endpoint
app.options("/publicStream", (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Accept, Cache-Control, Content-Type, X-Requested-With");
  res.setHeader("Access-Control-Max-Age", "86400");  // Cache preflight for 24 hours
  res.status(204).end();
});

// ============================================================================
// REAL-TIME LOG STREAMING ENDPOINTS
// ============================================================================

// Internal log streaming endpoint - provides real-time access to all service logs
// Uses Server-Sent Events (SSE) for real-time log streaming to connected clients
app.get("/logs/stream", (req, res) => {
  try {
    // Add the client to the log streamer for real-time log updates
    logStreamer.addClient(res);
  } catch (error) {
    console.error("❌ [LOG_STREAM] Error setting up stream:", error.message);
    res.status(500).json({
      success: false,
      error: "Failed to setup log stream",
      message: error.message,
    });
  }
});

// Public log streaming endpoint - provides filtered public logs for external consumption
// Uses Server-Sent Events (SSE) for real-time public log streaming
app.get("/publicStream", (req, res) => {
  try {
    // Add the client to the public log streamer for real-time public updates
    publicLogStreamer.addClient(res);
  } catch (error) {
    console.error("❌ [PUBLIC_STREAM] Error setting up public stream:", error.message);
    res.status(500).json({
      success: false,
      error: "Failed to setup public stream",
      message: error.message,
    });
  }
});

app.get("/logs/poll", limiter, async (req, res) => {
  try {
    const { limit = 20, since } = req.query;
    const maxLimit = Math.min(parseInt(limit) || 20, 100);
    
    let logs = [...logStreamer.logBuffer];
    
    if (since) {
      const sinceTimestamp = new Date(since).getTime();
      if (!isNaN(sinceTimestamp)) {
        logs = logs.filter(log => {
          const logTimestamp = new Date(log.timestamp).getTime();
          return logTimestamp > sinceTimestamp;
        });
      }
    }
    
    logs.sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime());
    const limitedLogs = logs.slice(0, maxLimit);
    
    const formattedLogs = limitedLogs.map(log => ({
      id: `${log.timestamp}_${Math.random().toString(36).substr(2, 9)}`,
      timestamp: new Date(log.timestamp).toLocaleString(),
      level: log.level.toLowerCase(),
      message: log.message,
      source: "blackswan-twitter-scraping-service"
    }));

    res.json({
      success: true,
      logs: formattedLogs,
      count: formattedLogs.length,
      total_available: logStreamer.logBuffer.length,
      timestamp: new Date().toISOString(),
      has_more: logs.length > maxLimit
    });
  } catch (error) {
    console.error("❌ [LOGS_POLL] Error fetching logs:", error);
    res.status(500).json({ 
      success: false, 
      error: "Failed to fetch logs", 
      message: error.message 
    });
  }
});

// ============================================================================
// HEALTH CHECK AND MONITORING ENDPOINTS
// ============================================================================

// Health check endpoint - provides comprehensive service status and statistics
// Used by monitoring systems, load balancers, and health check services
app.get('/health', async (req, res) => {
  try {
    // Get comprehensive statistics from the processing engine
    const stats = processingEngine.getStats();
    
    // Return detailed health information including all service components
    res.json({
      status: 'healthy',                                    // Overall service health status
      timestamp: new Date().toISOString(),                  // Current timestamp
      service: 'BlackSwan Twitter Scraping Service',        // Service name and version
      version: '1.0.0',                                     // Service version
      logStreamer: logStreamer.getStats(),                  // Log streaming statistics
      publicLogStreamer: publicLogStreamer.getStats(),      // Public log streaming statistics
      ...stats                                              // All processing engine statistics
    });
  } catch (error) {
    // Return unhealthy status if any errors occur during health check
    res.status(500).json({
      status: 'unhealthy',
      error: error.message,
      timestamp: new Date().toISOString(),
    });
  }
});

// ============================================================================
// API DOCUMENTATION ENDPOINT
// ============================================================================

// Root endpoint - provides comprehensive API documentation and service information
// This endpoint serves as the main entry point for understanding the service capabilities
app.get('/', (req, res) => {
  res.json({
    service: 'BlackSwan Twitter Scraping Service',
    version: '1.0.0',
    description: 'Enhanced Twitter monitoring service with improved thread detection, consolidated collections, and faster duplicate filtering',
    improvements: {
      threadManagement: 'Simplified and more reliable thread detection algorithm',
      collections: 'Consolidated to tweet_global and tweet_tokens collections only',
      duplicateDetection: 'Enhanced with batch processing and intelligent caching',
      performance: 'Faster processing with reduced resource usage'
    },
    endpoints: {
      'GET /': 'API documentation',
      'GET /health': 'Service health check and statistics',
      'GET /logs/stream': 'Real-time log streaming (Server-Sent Events)',
      'GET /stats': 'Detailed processing statistics',
      'POST /start': 'Start the processing engine',
      'POST /stop': 'Stop the processing engine',
      'GET /accounts': 'List monitored Twitter accounts',
      'POST /accounts': 'Add new Twitter account to monitor',
      'GET /recent-tweets': 'Get recently collected tweets from tweet_global',
      'GET /recent-threads': 'Get recently collected threads from tweet_global',
      'GET /token-accounts': 'List monitored token accounts',
      'GET /tokens': 'List monitored token accounts (deprecated, use /token-accounts)',
      'GET /recent-token-tweets': 'Get recently collected token tweets from tweet_tokens',
      'GET /recent-token-threads': 'Get recently collected token threads from tweet_tokens',
      'POST /toggle-token-processing': 'Enable/disable token mention processing'
    },
    newFeatures: [
      'Enhanced thread detection with better pattern recognition',
      'Consolidated collections: tweet_global for accounts, tweet_tokens for token accounts',
      'Faster duplicate detection with batch processing',
      'Improved memory management and caching',
      'Better error handling and recovery',
      'Performance metrics and monitoring',
      'Token accounts management via twitter_token_accounts collection'
    ],
    collections: {
      tweet_global: 'All tweets and threads from account monitoring',
      tweet_tokens: 'All tweets and threads from token account searches',
      twitter_accounts: 'Monitored Twitter accounts configuration',
      twitter_token_accounts: 'Token-specific Twitter accounts configuration'
    },
    configuration: PROCESSING_CONFIG
  });
});

// Updated API endpoints for new collections

// Get recent tweets (from tweet_global or tweet_tokens)
app.get('/recent-tweets', async (req, res) => {
  try {
    const { hours = 1, username, tokenSymbol, limit = 50, collection = 'global' } = req.query;
    const cutoffTime = Timestamp.fromDate(new Date(Date.now() - hours * 60 * 60 * 1000));
    
    const collectionName = collection === 'tokens' ? 'tweet_tokens' : 'tweet_global';
    
    let query = db.collection(collectionName)
      .where('collectedAt', '>=', cutoffTime)
      .where('documentType', '!=', 'thread') // Exclude thread documents
      .orderBy('collectedAt', 'desc');
    
    if (username) {
      query = query.where('username', '==', username.replace('@', ''));
    }
    
    if (tokenSymbol) {
      query = query.where('tokenSymbol', '==', tokenSymbol.toUpperCase());
    }
    
    const snapshot = await query.limit(parseInt(limit)).get();
    
    const tweets = [];
    snapshot.forEach(doc => {
      tweets.push({
        id: doc.id,
        ...doc.data(),
        collectedAt: doc.data().collectedAt?.toDate()?.toISOString(),
        createdAt: doc.data().createdAt?.toDate()?.toISOString()
      });
    });
    
    res.json({
      success: true,
      tweets: tweets,
      count: tweets.length,
      collection: collectionName,
      filtered: {
        hours: parseInt(hours),
        username: username || null,
        tokenSymbol: tokenSymbol || null
      }
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: 'Failed to retrieve tweets',
      message: error.message
    });
  }
});

// Get recent threads (from tweet_global or tweet_tokens)
app.get('/recent-threads', async (req, res) => {
  try {
    const { hours = 1, username, tokenSymbol, limit = 20, collection = 'global' } = req.query;
    const cutoffTime = Timestamp.fromDate(new Date(Date.now() - hours * 60 * 60 * 1000));
    
    const collectionName = collection === 'tokens' ? 'tweet_tokens' : 'tweet_global';
    
    let query = db.collection(collectionName)
      .where('collectedAt', '>=', cutoffTime)
      .where('documentType', '==', 'thread') // Only thread documents
      .orderBy('collectedAt', 'desc');
    
    if (username) {
      query = query.where('username', '==', username.replace('@', ''));
    }
    
    if (tokenSymbol) {
      query = query.where('tokenSymbol', '==', tokenSymbol.toUpperCase());
    }
    
    const snapshot = await query.limit(parseInt(limit)).get();
    
    const threads = [];
    snapshot.forEach(doc => {
      threads.push({
        id: doc.id,
        ...doc.data(),
        collectedAt: doc.data().collectedAt?.toDate()?.toISOString(),
        createdAt: doc.data().createdAt?.toDate()?.toISOString(),
        lastUpdated: doc.data().lastUpdated?.toDate()?.toISOString()
      });
    });
    
    res.json({
      success: true,
      threads: threads,
      count: threads.length,
      collection: collectionName,
      filtered: {
        hours: parseInt(hours),
        username: username || null,
        tokenSymbol: tokenSymbol || null
      }
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: 'Failed to retrieve threads',
      message: error.message
    });
  }
});

// Keep all other original endpoints with same implementations
app.get('/stats', (req, res) => {
  try {
    const stats = processingEngine.getStats();
    res.json(stats);
  } catch (error) {
    res.status(500).json({
      success: false,
      error: 'Failed to retrieve statistics',
      message: error.message
    });
  }
});

app.post('/start', async (req, res) => {
  try {
    await processingEngine.start();
    
    res.json({
      success: true,
      message: 'Improved processing engine started successfully',
      stats: processingEngine.getStats()
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: 'Failed to start processing engine',
      message: error.message
    });
  }
});

app.post('/stop', async (req, res) => {
  try {
    await processingEngine.stop();
    
    res.json({
      success: true,
      message: 'Improved processing engine stopped successfully',
      stats: processingEngine.getStats()
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: 'Failed to stop processing engine',
      message: error.message
    });
  }
});

app.get('/accounts', async (req, res) => {
  try {
    await processingEngine.accountManager.loadAccountsFromFirestore();
    const accounts = processingEngine.accountManager.accounts;
    const stats = processingEngine.accountManager.getProcessingStats();
    
    res.json({
      success: true,
      accounts: accounts,
      stats: stats,
      total: accounts.length
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: 'Failed to retrieve accounts',
      message: error.message
    });
  }
});

app.post('/accounts', async (req, res) => {
  try {
    const { username, priority = 1, category = 'general' } = req.body;
    
    if (!username) {
      return res.status(400).json({
        success: false,
        error: 'Username is required'
      });
    }

    const cleanUsername = username.replace('@', '').trim();
    
    const existingAccount = await db.collection('twitter_accounts')
      .where('username', '==', cleanUsername)
      .limit(1)
      .get();

    if (!existingAccount.empty) {
      return res.status(409).json({
        success: false,
        error: 'Account already exists'
      });
    }

    const newAccount = {
      username: cleanUsername,
      priority: parseInt(priority),
      category: category,
      isActive: true,
      createdAt: FieldValue.serverTimestamp(),
      processCount: 0,
      errorCount: 0
    };

    const docRef = await db.collection('twitter_accounts').add(newAccount);
    
    res.json({
      success: true,
      message: `Account @${cleanUsername} added successfully`,
      accountId: docRef.id,
      account: newAccount
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: 'Failed to add account',
      message: error.message
    });
  }
});

app.get('/token-accounts', async (req, res) => {
  try {
    await processingEngine.tokenAccountsManager.loadTokenAccountsFromFirestore();
    const tokenAccounts = processingEngine.tokenAccountsManager.getAllTokenAccounts();
    const stats = processingEngine.tokenAccountsManager.getProcessingStats();
    
    res.json({
      success: true,
      tokenAccounts: tokenAccounts,
      stats: stats,
      total: tokenAccounts.length
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: 'Failed to retrieve token accounts',
      message: error.message
    });
  }
});

// Keep the old /tokens endpoint for backward compatibility but redirect to token-accounts
app.get('/tokens', async (req, res) => {
  try {
    await processingEngine.tokenAccountsManager.loadTokenAccountsFromFirestore();
    const tokenAccounts = processingEngine.tokenAccountsManager.getAllTokenAccounts();
    const stats = processingEngine.tokenAccountsManager.getProcessingStats();
    
    res.json({
      success: true,
      message: 'This endpoint now returns token accounts. Please use /token-accounts for the new structure.',
      tokenAccounts: tokenAccounts,
      stats: stats,
      total: tokenAccounts.length
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: 'Failed to retrieve token accounts',
      message: error.message
    });
  }
});

// Convenience endpoints for token-specific data
app.get('/recent-token-tweets', async (req, res) => {
  try {
    const { hours = 1, tokenSymbol, tokenAccountId, limit = 50 } = req.query;
    const cutoffTime = Timestamp.fromDate(new Date(Date.now() - hours * 60 * 60 * 1000));
    
    let query = db.collection('tweet_tokens')
      .where('collectedAt', '>=', cutoffTime)
      .where('documentType', '!=', 'thread')
      .orderBy('collectedAt', 'desc');
    
    if (tokenSymbol) {
      query = query.where('tokenSymbol', '==', tokenSymbol.toUpperCase());
    }
    
    if (tokenAccountId) {
      query = query.where('tokenAccountId', '==', tokenAccountId);
    }
    
    const snapshot = await query.limit(parseInt(limit)).get();
    
    const tweets = [];
    snapshot.forEach(doc => {
      tweets.push({
        id: doc.id,
        ...doc.data(),
        collectedAt: doc.data().collectedAt?.toDate()?.toISOString(),
        createdAt: doc.data().createdAt?.toDate()?.toISOString()
      });
    });
    
    res.json({
      success: true,
      tweets: tweets,
      count: tweets.length,
      collection: 'tweet_tokens',
      filtered: {
        hours: parseInt(hours),
        tokenSymbol: tokenSymbol || null,
        tokenAccountId: tokenAccountId || null
      }
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: 'Failed to retrieve token mention tweets',
      message: error.message
    });
  }
});

app.get('/recent-token-threads', async (req, res) => {
  try {
    const { hours = 1, tokenSymbol, tokenAccountId, limit = 20 } = req.query;
    const cutoffTime = Timestamp.fromDate(new Date(Date.now() - hours * 60 * 60 * 1000));
    
    let query = db.collection('tweet_tokens')
      .where('collectedAt', '>=', cutoffTime)
      .where('documentType', '==', 'thread')
      .orderBy('collectedAt', 'desc');
    
    if (tokenSymbol) {
      query = query.where('tokenSymbol', '==', tokenSymbol.toUpperCase());
    }
    
    if (tokenAccountId) {
      query = query.where('tokenAccountId', '==', tokenAccountId);
    }
    
    const snapshot = await query.limit(parseInt(limit)).get();
    
    const threads = [];
    snapshot.forEach(doc => {
      threads.push({
        id: doc.id,
        ...doc.data(),
        collectedAt: doc.data().collectedAt?.toDate()?.toISOString(),
        createdAt: doc.data().createdAt?.toDate()?.toISOString(),
        lastUpdated: doc.data().lastUpdated?.toDate()?.toISOString()
      });
    });
    
    res.json({
      success: true,
      threads: threads,
      count: threads.length,
      collection: 'tweet_tokens',
      filtered: {
        hours: parseInt(hours),
        tokenSymbol: tokenSymbol || null,
        tokenAccountId: tokenAccountId || null
      }
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: 'Failed to retrieve token mention threads',
      message: error.message
    });
  }
});

app.post('/toggle-token-processing', async (req, res) => {
  try {
    const { enabled } = req.body;
    
    if (typeof enabled !== 'boolean') {
      return res.status(400).json({
        success: false,
        error: 'enabled parameter must be a boolean'
      });
    }

    processingEngine.isTokenProcessingEnabled = enabled;
    
    res.json({
      success: true,
      message: `Token processing ${enabled ? 'enabled' : 'disabled'}`,
      isTokenProcessingEnabled: processingEngine.isTokenProcessingEnabled,
      stats: processingEngine.getStats()
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: 'Failed to toggle token processing',
      message: error.message
    });
  }
});

// New endpoint to get duplicate detection stats
app.get('/duplicate-stats', async (req, res) => {
  try {
    const stats = processingEngine.getStats();
    const duplicateStats = {
      duplicatesFiltered: stats.processing.duplicatesFiltered,
      cacheHits: stats.processing.cacheHits,
      cacheEfficiency: stats.performance.cacheEfficiency,
      globalCacheSize: processingEngine.duplicateDetector.globalCache.size,
      tokenCacheSize: processingEngine.duplicateDetector.tokenCache.size,
      maxCacheSize: PROCESSING_CONFIG.DUPLICATE_CACHE_SIZE,
      batchSize: PROCESSING_CONFIG.BATCH_DUPLICATE_CHECK_SIZE
    };
    
    res.json({
      success: true,
      duplicateDetection: duplicateStats,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: 'Failed to retrieve duplicate detection stats',
      message: error.message
    });
  }
});

// New endpoint to get thread detection stats
app.get('/thread-stats', async (req, res) => {
  try {
    const { collection = 'global', hours = 24 } = req.query;
    const cutoffTime = Timestamp.fromDate(new Date(Date.now() - hours * 60 * 60 * 1000));
    const collectionName = collection === 'tokens' ? 'tweet_tokens' : 'tweet_global';
    
    // Get thread stats from the collection
    const threadsSnapshot = await db.collection(collectionName)
      .where('documentType', '==', 'thread')
      .where('collectedAt', '>=', cutoffTime)
      .get();
    
    const tweetsSnapshot = await db.collection(collectionName)
      .where('documentType', '!=', 'thread')
      .where('isThread', '==', false)
      .where('collectedAt', '>=', cutoffTime)
      .get();
    
    const threadStats = {
      totalThreads: threadsSnapshot.size,
      totalStandaloneTweets: tweetsSnapshot.size,
      detectionMethods: {},
      averageThreadLength: 0,
      totalThreadTweets: 0
    };
    
    let totalParts = 0;
    
    threadsSnapshot.forEach(doc => {
      const data = doc.data();
      
      // Count detection methods
      if (data.detectionMethods && Array.isArray(data.detectionMethods)) {
        data.detectionMethods.forEach(method => {
          threadStats.detectionMethods[method] = (threadStats.detectionMethods[method] || 0) + 1;
        });
      }
      
      // Sum thread parts
      if (data.totalParts) {
        totalParts += data.totalParts;
        threadStats.totalThreadTweets += data.totalParts;
      }
    });
    
    if (threadStats.totalThreads > 0) {
      threadStats.averageThreadLength = (totalParts / threadStats.totalThreads).toFixed(2);
    }
    
    res.json({
      success: true,
      threadDetection: threadStats,
      collection: collectionName,
      hoursAnalyzed: parseInt(hours),
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: 'Failed to retrieve thread detection stats',
      message: error.message
    });
  }
});

// Error handler
app.use((error, req, res, next) => {
  console.error('💥 Unhandled error:', error);
  res.status(500).json({
    success: false,
    error: 'Internal Server Error',
    message: process.env.NODE_ENV === 'development' ? error.message : 'An unexpected error occurred',
    timestamp: new Date().toISOString(),
  });
});

// 404 handler
app.use('*', (req, res) => {
  res.status(404).json({
    success: false,
    error: 'Endpoint not found',
    message: `The endpoint ${req.method} ${req.originalUrl} does not exist`,
    version: '1.0.0'
  });
});

// ============================================================================
// GRACEFUL SHUTDOWN AND ERROR HANDLING
// ============================================================================

// Graceful shutdown handler - ensures clean shutdown of all services
// This function is called when the process receives termination signals
const gracefulShutdown = async () => {
  console.log('🛑 Shutting down Improved Twitter Service gracefully...');
  try {
    // Stop the processing engine and clean up resources
    await processingEngine.stop();
    
    // Restore original console methods to prevent memory leaks
    logStreamer.restoreOriginalConsole();
    
    console.log('✅ Improved service shutdown completed');
  } catch (error) {
    console.error('❌ Error during shutdown:', error);
  }
  process.exit(0);
};

// Register signal handlers for graceful shutdown
process.on('SIGINT', gracefulShutdown);   // Handle Ctrl+C
process.on('SIGTERM', gracefulShutdown);  // Handle termination signals

// Handle unhandled promise rejections - log and exit to prevent silent failures
process.on('unhandledRejection', (reason, promise) => {
  console.error('💥 Unhandled Rejection at:', promise, 'reason:', reason);
  process.exit(1);
});

// Handle uncaught exceptions - log and exit to prevent undefined behavior
process.on('uncaughtException', (error) => {
  console.error('💥 Uncaught Exception:', error);
  process.exit(1);
});

// ============================================================================
// SERVICE STARTUP FUNCTION
// ============================================================================

// Main service startup function - initializes and starts all service components
// This function handles the complete startup sequence including server initialization
// and processing engine startup with comprehensive logging and error handling
async function startImprovedService() {
  try {
    // Display service startup banner with version and environment information
    console.log('🚀 Starting BlackSwan Twitter Scraping Service...');
    console.log(`📊 Version: 1.0.0`);
    console.log(`🌐 Environment: ${process.env.NODE_ENV || 'development'}`);
    console.log('');
    
    // Display key improvements and features
    console.log('🆕 KEY IMPROVEMENTS:');
    console.log('  🧵 Enhanced thread detection with simplified, reliable algorithm');
    console.log('  📦 Consolidated collections: tweet_global + tweet_tokens only');
    console.log('  🚀 Faster duplicate detection with batch processing and caching');
    console.log('  💾 Improved memory management and performance monitoring');
    console.log('  🔧 Better error handling and recovery mechanisms');
    console.log('');
    
    // Display current configuration settings
    console.log('⚙️  CONFIGURATION:');
    console.log(`  🔄 Processing Mode: ALL Accounts → ALL Tokens (complete cycles)`);
    console.log(`  ⏰ Tweet lookback: ${PROCESSING_CONFIG.TWEET_LOOKBACK_MINUTES} minutes (24 hours)`);
    console.log(`  📱 Max tweets per account: ${PROCESSING_CONFIG.MAX_TWEETS_PER_ACCOUNT}`);
    console.log(`  🔍 Max tweets per token search: ${PROCESSING_CONFIG.MAX_TWEETS_PER_TOKEN_SEARCH}`);
    console.log(`  ⚡ Account rate limit delay: ${PROCESSING_CONFIG.RATE_LIMIT_DELAY}ms`);
    console.log(`  🪙 Token rate limit delay: ${PROCESSING_CONFIG.TOKEN_RATE_LIMIT_DELAY}ms`);
    console.log(`  🔄 Full cycle interval: ${PROCESSING_CONFIG.PROCESSING_INTERVAL / 1000}s`);
    console.log(`  🧠 Duplicate cache size: ${PROCESSING_CONFIG.DUPLICATE_CACHE_SIZE.toLocaleString()}`);
    console.log(`  📦 Batch duplicate check size: ${PROCESSING_CONFIG.BATCH_DUPLICATE_CHECK_SIZE} (Firestore IN limit compliant)`);
    console.log('');
    
    // Display Firestore collections information
    console.log('📊 COLLECTIONS:');
    console.log('  📚 tweet_global - All tweets/threads from account monitoring');
    console.log('  🪙 tweet_tokens - All tweets/threads from token account searches');
    console.log('  👥 twitter_accounts - Monitored Twitter accounts configuration');
    console.log('  🏷️  twitter_token_accounts - Token-specific Twitter accounts configuration');
    console.log('');

    // Start Express HTTP server
    const server = app.listen(PORT, () => {
      console.log(`✅ Improved API Server running on port ${PORT}`);
      console.log(`📚 API Documentation: http://localhost:${PORT}`);
      console.log(`💚 Health Check: http://localhost:${PORT}/health`);
      console.log(`📊 Duplicate Stats: http://localhost:${PORT}/duplicate-stats`);
      console.log(`🧵 Thread Stats: http://localhost:${PORT}/thread-stats`);
      console.log('');
    });

    // Auto-start the Twitter processing engine
    // This begins the automatic monitoring and data collection process
    console.log('🔄 Auto-starting improved processing engine...');
    try {
      await processingEngine.start();
      console.log('✅ Improved processing engine started successfully');
    } catch (error) {
      console.error('⚠️ Processing engine auto-start failed:', error.message);
      console.log('💡 You can manually start using POST /start');
    }

    // Display service ready message and comprehensive endpoint documentation
    console.log('\n🎉 BlackSwan Twitter Scraping Service is ready!');
    console.log('📖 Available endpoints:');
    console.log('   GENERAL:');
    console.log('   • GET  /              - API documentation with improvements');
    console.log('   • GET  /health        - Health check and enhanced stats');
    console.log('   • GET  /logs/stream   - Real-time log streaming (SSE)');
    console.log('   • GET  /stats         - Detailed statistics with performance metrics');
    console.log('   • POST /start         - Start improved processing engine');
    console.log('   • POST /stop          - Stop processing engine');
    console.log('');
    console.log('   ACCOUNTS (tweet_global):');
    console.log('   • GET  /accounts      - List monitored accounts');
    console.log('   • POST /accounts      - Add new account');
    console.log('   • GET  /recent-tweets?collection=global - Get recent account tweets');
    console.log('   • GET  /recent-threads?collection=global - Get recent account threads');
    console.log('');
    console.log('   TOKEN ACCOUNTS (tweet_tokens):');
    console.log('   • GET  /token-accounts     - List monitored token accounts');
    console.log('   • GET  /tokens             - List monitored token accounts (deprecated)');
    console.log('   • GET  /recent-tweets?collection=tokens - Get recent token tweets');
    console.log('   • GET  /recent-threads?collection=tokens - Get recent token threads');
    console.log('   • GET  /recent-token-tweets- Convenience endpoint for token tweets');
    console.log('   • GET  /recent-token-threads- Convenience endpoint for token threads');
    console.log('   • POST /toggle-token-processing- Enable/disable token processing');
    console.log('');
    console.log('   MONITORING (New!):');
    console.log('   • GET  /duplicate-stats    - Duplicate detection performance');
    console.log('   • GET  /thread-stats       - Thread detection analytics');
    console.log('');
    console.log('⚡ IMPROVEMENTS SUMMARY:');
    console.log('🧵 Thread Management: Simplified detection algorithm, better accuracy');
    console.log('📦 Storage: Only 2 data collections (tweet_global + tweet_tokens)');
    console.log('🏷️  Token Management: twitter_token_accounts instead of tokens-repository');
    console.log('🚀 Performance: Batch duplicate checking, intelligent caching');
    console.log('📊 Monitoring: Enhanced metrics and performance tracking');
    console.log('🔧 Reliability: Better error handling and recovery');
    console.log('\n✅ Ready to monitor Twitter accounts AND token account mentions with enhanced performance!');

  } catch (error) {
    // Handle any startup errors and exit gracefully
    console.error('💥 Failed to start improved service:', error.message);
    process.exit(1);
  }
}

// ============================================================================
// SERVICE STARTUP EXECUTION
// ============================================================================

// Start the improved service and handle any startup errors
// This is the main entry point that begins the entire service
startImprovedService().catch(console.error);

// Export the Express app for testing and external use
export default app;