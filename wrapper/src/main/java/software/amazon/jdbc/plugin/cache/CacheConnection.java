/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.amazon.jdbc.plugin.cache;

import glide.api.GlideClusterClient;
import glide.api.models.GlideString;
import glide.api.models.commands.SetOptions;
import glide.api.models.configuration.*;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCredentials;
import io.lettuce.core.RedisCredentialsProvider;
import io.lettuce.core.RedisURI;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.resource.ClientResources;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.logging.Logger;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.PooledObject;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.authentication.AwsCredentialsManager;
import software.amazon.jdbc.plugin.iam.ElastiCacheIamTokenUtility;
import software.amazon.jdbc.util.StringUtils;

// Abstraction layer on top of a connection to a remote cache server
public class CacheConnection {
  private static final Logger LOGGER = Logger.getLogger(CacheConnection.class.getName());

  private static final int DEFAULT_POOL_MIN_IDLE = 0;
  private static final int DEFAULT_MAX_POOL_SIZE = 200;
  private static final long DEFAULT_MAX_BORROW_WAIT_MS = 100;
  private static final long TOKEN_CACHE_DURATION = 15 * 60 - 30;

  private static final ReentrantLock READ_LOCK = new ReentrantLock();
  private static final ReentrantLock WRITE_LOCK = new ReentrantLock();

  private final String cacheRwServerAddr; // read-write cache server
  private final String cacheRoServerAddr; // read-only cache server
  private final String[] defaultCacheServerHostAndPort;
  private MessageDigest msgHashDigest = null;

  protected static final AwsWrapperProperty CACHE_RW_ENDPOINT_ADDR =
      new AwsWrapperProperty(
          "cacheEndpointAddrRw",
          null,
          "The cache read-write server endpoint address.");

  private static final AwsWrapperProperty CACHE_RO_ENDPOINT_ADDR =
      new AwsWrapperProperty(
          "cacheEndpointAddrRo",
          null,
          "The cache read-only server endpoint address.");

  protected static final AwsWrapperProperty CACHE_USE_SSL =
      new AwsWrapperProperty(
          "cacheUseSSL",
          "true",
          "Whether to use SSL for cache connections.");

  protected static final AwsWrapperProperty CACHE_IAM_REGION =
      new AwsWrapperProperty(
          "cacheIamRegion",
          null,
          "AWS region for ElastiCache IAM authentication.");

  protected static final AwsWrapperProperty CACHE_USERNAME =
      new AwsWrapperProperty(
          "cacheUsername",
          null,
          "Username for ElastiCache regular authentication.");

  protected static final AwsWrapperProperty CACHE_PASSWORD =
      new AwsWrapperProperty(
          "cachePassword",
          null,
          "Password for ElastiCache regular authentication.");

  protected static final AwsWrapperProperty CACHE_NAME =
      new AwsWrapperProperty(
          "cacheName",
          null,
          "Explicit cache name for ElastiCache IAM authentication. ");

  protected static final AwsWrapperProperty CACHE_CONNECTION_TIMEOUT =
      new AwsWrapperProperty(
          "cacheConnectionTimeout",
          "2000",
          "Cache connection request timeout duration in milliseconds.");

  protected static final AwsWrapperProperty CACHE_CONNECTION_POOL_SIZE =
      new AwsWrapperProperty(
          "cacheConnectionPoolSize",
          "20",
          "Cache connection pool size.");

  protected static final AwsWrapperProperty CACHE_CLIENT_TYPE =
      new AwsWrapperProperty(
          "cacheClientType",
          "LETTUCE",
          "Cache client type: LETTUCE or GLIDE");

  // Adding support for read and write connection pools to the remote cache server
  private static volatile GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> readConnectionPool;
  private static volatile GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> writeConnectionPool;
  private static final GenericObjectPoolConfig<StatefulRedisConnection<byte[], byte[]>> poolConfig = createPoolConfig();

  //Adding support for read and write connection pools using Glide
  private static volatile GenericObjectPool<GlideClusterClient> readGlideClientPool;
  private static volatile GenericObjectPool<GlideClusterClient> writeGlideClientPool;
  private static final GenericObjectPoolConfig<GlideClusterClient> glidePoolConfig = createGlidePoolConfig();

  private final boolean useSSL;
  private final boolean iamAuthEnabled;
  private final String cacheIamRegion;
  private final String cacheUsername;
  private final String cacheName;
  private final String cachePassword;
  private final Duration cacheConnectionTimeout;
  private final int cacheConnectionPoolSize;
  private final Properties awsProfileProperties;
  private final AwsCredentialsProvider credentialsProvider;
  private final boolean useGlide;

  static {
    PropertyDefinition.registerPluginProperties(CacheConnection.class);
  }

  public CacheConnection(final Properties properties) {
    this.cacheRwServerAddr = CACHE_RW_ENDPOINT_ADDR.getString(properties);
    this.cacheRoServerAddr = CACHE_RO_ENDPOINT_ADDR.getString(properties);
    this.useSSL = Boolean.parseBoolean(CACHE_USE_SSL.getString(properties));
    this.cacheName = CACHE_NAME.getString(properties);
    this.cacheIamRegion = CACHE_IAM_REGION.getString(properties);
    this.cacheUsername = CACHE_USERNAME.getString(properties);
    this.cachePassword = CACHE_PASSWORD.getString(properties);
    this.cacheConnectionTimeout = Duration.ofMillis(CACHE_CONNECTION_TIMEOUT.getInteger(properties));
    this.cacheConnectionPoolSize = CACHE_CONNECTION_POOL_SIZE.getInteger(properties);
    this.useGlide = "GLIDE".equalsIgnoreCase(CACHE_CLIENT_TYPE.getString(properties));
    if (this.cacheConnectionPoolSize <= 0 || this.cacheConnectionPoolSize > DEFAULT_MAX_POOL_SIZE) {
      throw new IllegalArgumentException(
          "Cache connection pool size must be within valid range: 1-" + DEFAULT_MAX_POOL_SIZE + ", but was: " + this.cacheConnectionPoolSize);
    }
    // Update the static poolConfig with user values
    poolConfig.setMaxTotal(this.cacheConnectionPoolSize);
    poolConfig.setMaxIdle(this.cacheConnectionPoolSize);
    // Updating the static glidePoolConfig with user values
    glidePoolConfig.setMaxTotal(this.cacheConnectionPoolSize);
    glidePoolConfig.setMaxIdle(this.cacheConnectionPoolSize);

    this.iamAuthEnabled = !StringUtils.isNullOrEmpty(this.cacheIamRegion);
    boolean hasTraditionalAuth = !StringUtils.isNullOrEmpty(this.cachePassword);
    // Validate authentication configuration
    if (this.iamAuthEnabled && hasTraditionalAuth) {
      throw new IllegalArgumentException(
          "Cannot specify both IAM authentication (cacheIamRegion) and traditional authentication (cachePassword). Choose one authentication method.");
    }
    if (this.cacheRwServerAddr == null) {
      throw new IllegalArgumentException("Cache endpoint address is required");
    }
    this.defaultCacheServerHostAndPort = getHostnameAndPort(this.cacheRwServerAddr);
    if (this.iamAuthEnabled) {
      if (this.cacheUsername == null || this.defaultCacheServerHostAndPort[0] == null || this.cacheName == null) {
        throw new IllegalArgumentException("IAM authentication requires cache name, username, region, and hostname");
      }
    }
    if (PropertyDefinition.AWS_PROFILE.getString(properties) != null) {
      this.awsProfileProperties = new Properties();
      this.awsProfileProperties.setProperty(
          PropertyDefinition.AWS_PROFILE.name,
          PropertyDefinition.AWS_PROFILE.getString(properties)
      );
    } else {
      this.awsProfileProperties = null;
    }
    if (this.iamAuthEnabled) {
      // Handle null case
      Properties propsToPass = (this.awsProfileProperties != null)
          ? this.awsProfileProperties
          : new Properties();
      this.credentialsProvider = AwsCredentialsManager.getProvider(null, propsToPass);
    } else {
      this.credentialsProvider = null;
    }
  }



  /* Here we check if we need to initialise connection pool for read or write to cache.
  With isRead we check if we need to initialise connection pool for read or write to cache.
  If isRead is true, we initialise connection pool for read.
  If isRead is false, we initialise connection pool for write.
   */
  private void initializeCacheConnectionIfNeeded(boolean isRead) {
    if (StringUtils.isNullOrEmpty(cacheRwServerAddr)) return;
    // Initialize the message digest
    if (msgHashDigest == null) {
      try {
        msgHashDigest = MessageDigest.getInstance("SHA-384");
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException("SHA-384 not supported", e);
      }
    }

    GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> cacheConnectionPool =
        isRead ? readConnectionPool : writeConnectionPool;
    if (cacheConnectionPool == null) {
      ReentrantLock connectionPoolLock = isRead ? READ_LOCK : WRITE_LOCK;
      connectionPoolLock.lock();
      try {
        if ((isRead && readConnectionPool == null) || (!isRead && writeConnectionPool == null)) {
          createConnectionPool(isRead);
        }
      } finally {
        connectionPoolLock.unlock();
      }
    }

  }

  private void createConnectionPool(boolean isRead) {
    ClientResources resources = ClientResources.builder().build();
    try {
      // cache server addr string is in the format "<server hostname>:<port>"
      String serverAddr = cacheRwServerAddr;
      // If read-only server is specified, use it for the read-only connections
      if (isRead && !StringUtils.isNullOrEmpty(cacheRoServerAddr)) {
        serverAddr = cacheRoServerAddr;
      }
      String[] hostnameAndPort = getHostnameAndPort(serverAddr);
      RedisURI redisUriCluster = buildRedisURI(hostnameAndPort[0], Integer.parseInt(hostnameAndPort[1]));
      //long startTime = System.currentTimeMillis();
      RedisClient client = RedisClient.create(resources, redisUriCluster);
      GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> pool = new GenericObjectPool<>(
          new BasePooledObjectFactory<StatefulRedisConnection<byte[], byte[]>>() {
            public StatefulRedisConnection<byte[], byte[]> create() {

              StatefulRedisConnection<byte[], byte[]> connection = client.connect(new ByteArrayCodec());
              // In cluster mode, we need to send READONLY command to the server for reading from replica.
              // Note: we gracefully ignore ERR reply to support non cluster mode.
              if (isRead) {
                try {
                  connection.sync().readOnly();
                } catch (RedisCommandExecutionException e) {
                  if (e.getMessage().contains("ERR This instance has cluster support disabled")) {
                    LOGGER.fine("------ Note: this cache cluster has cluster support disabled ------");
                  } else {
                    throw e;
                  }
                }
              }
              return connection;
            }
            public PooledObject<StatefulRedisConnection<byte[], byte[]>> wrap(StatefulRedisConnection<byte[], byte[]> connection) {
              return new DefaultPooledObject<>(connection);
            }
          }, poolConfig);
      //long duration = System.currentTimeMillis() - startTime;
      //LOGGER.info("Lettuce connection pool created in " + duration + "ms");

      if (isRead) {
        readConnectionPool = pool;
      } else {
        writeConnectionPool = pool;
      }
    } catch (Exception e) {
      String poolType = isRead ? "read" : "write";
      String errorMsg = String.format("Failed to create Cache %s connection pool", poolType);
      LOGGER.warning(errorMsg + ": " + e.getMessage());
      throw new RuntimeException(errorMsg, e);
    }
  }

  private static GenericObjectPoolConfig<StatefulRedisConnection<byte[], byte[]>> createPoolConfig() {
    GenericObjectPoolConfig<StatefulRedisConnection<byte[], byte[]>> poolConfig = new GenericObjectPoolConfig<>();
    poolConfig.setMinIdle(DEFAULT_POOL_MIN_IDLE);
    poolConfig.setMaxWait(Duration.ofMillis(DEFAULT_MAX_BORROW_WAIT_MS));
    return poolConfig;
  }

  // Get the hash digest of the given key.
  private byte[] computeHashDigest(byte[] key) {
    msgHashDigest.update(key);
    return msgHashDigest.digest();
  }

  public byte[] readFromCache(String key) {
    if (useGlide){
      return readFromCacheUsingGlide(key);
    }
    else{
      return readFromCacheUsingLettuce(key);
    }
  }

  private byte[] readFromCacheUsingLettuce(String key){
    boolean isBroken = false;
    StatefulRedisConnection<byte[], byte[]> conn = null;
    // get a connection from the read connection pool
    try {
      initializeCacheConnectionIfNeeded(true);
      //long startTime = System.currentTimeMillis();
      conn = readConnectionPool.borrowObject();
      byte[] result = conn.sync().get(computeHashDigest(key.getBytes(StandardCharsets.UTF_8)));
      //long duration = System.currentTimeMillis() - startTime;
      //LOGGER.info("Lettuce read time: " + duration + "ms");
      return result;
    } catch (Exception e) {
      if (conn != null) {
        isBroken = true;
      }
      LOGGER.warning("Failed to read result from cache. Treating it as a cache miss: " + e.getMessage());
      return null;
    } finally {
      if (conn != null && readConnectionPool != null) {
        try {
          this.returnConnectionBackToPool(conn, isBroken, true);
        } catch (Exception ex) {
          LOGGER.warning("Error closing read connection: " + ex.getMessage());
        }
      }
    }
  }

  protected void handleCompletedCacheWrite(StatefulRedisConnection<byte[], byte[]> conn, Throwable ex) {
    // Note: this callback upon completion of cache write is on a different thread
    if (ex != null) {
      LOGGER.warning("Failed to write to cache: " + ex.getMessage());
      if (writeConnectionPool != null) {
        try {
          returnConnectionBackToPool(conn, true, false);
        } catch (Exception e) {
          LOGGER.warning("Error returning broken write connection back to pool: " + e.getMessage());
        }
      }
    } else {
      if (writeConnectionPool != null) {
        try {
          returnConnectionBackToPool(conn, false, false);
        } catch (Exception e) {
          LOGGER.warning("Error returning write connection back to pool: " + e.getMessage());
        }
      }
    }
  }

  public void writeToCache(String key, byte[] value, int expiry) {
    if(useGlide){
      writeToCacheUsingGlide(key, value, expiry);
    }
    else{
      writeToCacheUsingLettuce(key, value, expiry);
    }
  }

  private void writeToCacheUsingLettuce(String key, byte[] value, int expiry){
    StatefulRedisConnection<byte[], byte[]> conn = null;
    try {
      initializeCacheConnectionIfNeeded(false);
      //long startTime = System.currentTimeMillis();
      // get a connection from the write connection pool
      conn = writeConnectionPool.borrowObject();
      // Write to the cache is async.
      RedisAsyncCommands<byte[], byte[]> asyncCommands = conn.async();
      byte[] keyHash = computeHashDigest(key.getBytes(StandardCharsets.UTF_8));
      StatefulRedisConnection<byte[], byte[]> finalConn = conn;
      asyncCommands.set(keyHash, value, SetArgs.Builder.ex(expiry))
          .whenComplete((result, exception) -> handleCompletedCacheWrite(finalConn, exception));
      //long duration = System.currentTimeMillis() - startTime;
      //.info("Lettuce write time: " + duration + "ms");
    } catch (Exception e) {
      // Failed to trigger the async write to the cache, return the cache connection to the pool as broken
      LOGGER.warning("Unable to start writing to cache: " + e.getMessage());
      if (conn != null && writeConnectionPool != null) {
        try {
          returnConnectionBackToPool(conn, true, false);
        } catch (Exception ex) {
          LOGGER.warning("Error closing write connection: " + ex.getMessage());
        }
      }
    }
  }

  private void returnConnectionBackToPool(StatefulRedisConnection <byte[], byte[]> connection, boolean isConnectionBroken, boolean isRead) {
    GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> pool = isRead ? readConnectionPool : writeConnectionPool;
    if (isConnectionBroken) {
      try {
        pool.invalidateObject(connection);
      } catch (Exception e) {
        throw new RuntimeException("Could not invalidate connection for the pool", e);
      }
    } else {
      pool.returnObject(connection);
    }
  }

  // Used for unit testing only
  protected void setConnectionPools(GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> readPool,
      GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> writePool) {
    readConnectionPool = readPool;
    writeConnectionPool = writePool;
  }

  // Used for unit testing only
  protected void triggerPoolInit(boolean isRead) {
    initializeCacheConnectionIfNeeded(isRead);
  }

  protected RedisURI buildRedisURI(String hostname, int port) {
    RedisURI.Builder uriBuilder = RedisURI.Builder.redis(hostname)
        .withPort(port)
        .withSsl(useSSL)
        .withVerifyPeer(false)
        .withLibraryName("aws-sql-jdbc-lettuce")
        .withTimeout(cacheConnectionTimeout);

    if (this.iamAuthEnabled) {
      // Create a credentials provider that Lettuce will call whenever authentication is needed
      RedisCredentialsProvider credentialsProvider = () -> {
        // Create a cached token supplier that automatically refreshes tokens every 14.5 minutes
        Supplier<String> tokenSupplier = CachedSupplier.memoizeWithExpiration(
            () -> {
              ElastiCacheIamTokenUtility tokenUtility = new ElastiCacheIamTokenUtility(this.cacheName);
              return tokenUtility.generateAuthenticationToken(
                  this.credentialsProvider,
                  Region.of(this.cacheIamRegion),
                  this.defaultCacheServerHostAndPort[0],
                  Integer.parseInt(this.defaultCacheServerHostAndPort[1]),
                  this.cacheUsername
              );
            },
            TOKEN_CACHE_DURATION,
            TimeUnit.SECONDS
        );
        // Package the username and token (from cache or freshly generated) into Redis credentials
        return Mono.just(RedisCredentials.just(this.cacheUsername, tokenSupplier.get()));
      };
      uriBuilder.withAuthentication(credentialsProvider);
    } else if (!StringUtils.isNullOrEmpty(this.cachePassword)) {
      LOGGER.finest("ACL for cache");
      uriBuilder.withAuthentication(this.cacheUsername, this.cachePassword);
    }
    return uriBuilder.build();
  }

  private String[] getHostnameAndPort(String serverAddr) {
    return serverAddr.split(":");
  }

  /*------------------------------ Glide related code -------------------------- */

  // Glide Initialization
  private void initializeGlideCacheConnectionIfNeeded(boolean isRead) {
    if (StringUtils.isNullOrEmpty(cacheRwServerAddr)) return;
    // Initialize the message digest
    if (msgHashDigest == null) {
      try {
        msgHashDigest = MessageDigest.getInstance("SHA-384");
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException("SHA-384 not supported", e);
      }
    }

    GenericObjectPool<GlideClusterClient> cacheClientPool =
        isRead ? readGlideClientPool : writeGlideClientPool;
    if (cacheClientPool == null) {
      ReentrantLock connectionPoolLock = isRead ? READ_LOCK : WRITE_LOCK;
      connectionPoolLock.lock();
      try {
        if ((isRead && readGlideClientPool == null) || (!isRead && writeGlideClientPool == null)) {
          createGlideClientPool(isRead);
        }
      } finally {
        connectionPoolLock.unlock();
      }
    }

  }

  // creating Glide pool client
  private void createGlideClientPool(boolean isRead) {
    try {
      GlideClusterClientConfiguration config = buildGlideClusterClientConfiguration(isRead);

      //long startTime = System.currentTimeMillis();
      GenericObjectPool<GlideClusterClient> pool = new GenericObjectPool<>(
          new BasePooledObjectFactory<GlideClusterClient>() {
            //creates glide client object
            public GlideClusterClient create() throws Exception {
              return GlideClusterClient.createClient(config).get();
            }

            public PooledObject<GlideClusterClient> wrap(GlideClusterClient client) {
              return new DefaultPooledObject<>(client);
            }
          }, glidePoolConfig);

      //long duration = System.currentTimeMillis() - startTime;
      //LOGGER.info("Glide connection pool created in " + duration + "ms");

      if (isRead) {
        readGlideClientPool = pool;
      } else {
        writeGlideClientPool = pool;
      }
    } catch (Exception e) {
      String poolType = isRead ? "read" : "write";
      String errorMsg = String.format("Failed to create Cache %s Glide client pool", poolType);
      LOGGER.warning(errorMsg + ": " + e.getMessage());
      throw new RuntimeException(errorMsg, e);
    }
  }

  // build glide cluster client configuration
  private GlideClusterClientConfiguration buildGlideClusterClientConfiguration(boolean isRead) {

    String serverAddr = cacheRwServerAddr;
    if (isRead && !StringUtils.isNullOrEmpty(cacheRoServerAddr)) {
      serverAddr = cacheRoServerAddr;
    }

    String[] hostnameAndPort = getHostnameAndPort(serverAddr);
    NodeAddress address = NodeAddress.builder()
        .host(hostnameAndPort[0])
        .port(Integer.parseInt(hostnameAndPort[1]))
        .build();

    GlideClusterClientConfiguration.GlideClusterClientConfigurationBuilder config = GlideClusterClientConfiguration.builder()
        .address(address)
        .useTLS(useSSL)
        .requestTimeout((int) cacheConnectionTimeout.toMillis())
        .lazyConnect(true)
        .readFrom(!StringUtils.isNullOrEmpty(cacheRoServerAddr) ? ReadFrom.PREFER_REPLICA : ReadFrom.PRIMARY);

    //If TLS enabled
    if(useSSL){
      TlsAdvancedConfiguration tlsConfig = TlsAdvancedConfiguration.builder()
          .useInsecureTLS(true)
          .build();

      AdvancedGlideClusterClientConfiguration advancedConfig = AdvancedGlideClusterClientConfiguration.builder()
          .tlsAdvancedConfiguration(tlsConfig)
          .connectionTimeout((int) cacheConnectionTimeout.toMillis())
          .build();

      config.advancedConfiguration(advancedConfig);
    }

    //If IAM authentication or ACL authentication enabled
    if(this.iamAuthEnabled){
      //To do: - Configure IAM Authentication
      boolean isServerless = hostnameAndPort[0].contains(ElastiCacheIamTokenUtility.SERVERLESS_CACHE_IDENTIFIER);
      Supplier<String> tokenSupplier = CachedSupplier.memoizeWithExpiration(
          () -> {
            ElastiCacheIamTokenUtility tokenUtility = new ElastiCacheIamTokenUtility(this.cacheName);
            return tokenUtility.generateAuthenticationToken(
                this.credentialsProvider,
                Region.of(this.cacheIamRegion),
                this.defaultCacheServerHostAndPort[0],
                Integer.parseInt(this.defaultCacheServerHostAndPort[1]),
                this.cacheUsername
            );
          },
          TOKEN_CACHE_DURATION,
          TimeUnit.SECONDS
      );

      ServerCredentials credentials = ServerCredentials.builder()
          .username(this.cacheUsername)
          .password(tokenSupplier.get())
          .build();

      config.credentials(credentials);

    } else if(!StringUtils.isNullOrEmpty(this.cachePassword)){
      config.credentials(ServerCredentials.builder()
          .username(this.cacheUsername)
          .password(this.cachePassword)
          .build());
    }

    return config.build();
  }

  //Glide pool config
  private static GenericObjectPoolConfig<GlideClusterClient> createGlidePoolConfig(){
    GenericObjectPoolConfig<GlideClusterClient> poolConfig = new GenericObjectPoolConfig<>();
    poolConfig.setMinIdle(DEFAULT_POOL_MIN_IDLE);
    poolConfig.setMaxWait(Duration.ofMillis(DEFAULT_MAX_BORROW_WAIT_MS));
    return poolConfig;
  }

  //read using glide client
  private byte[] readFromCacheUsingGlide(String key) {
    boolean isBroken = false;
    GlideClusterClient client = null;
    try {
      initializeGlideCacheConnectionIfNeeded(true);
      //long startTime = System.currentTimeMillis();

      client = readGlideClientPool.borrowObject();
      byte[] keyHash = computeHashDigest(key.getBytes(StandardCharsets.UTF_8));
      GlideString keyGs = GlideString.of(keyHash);
      GlideString gs = client.get(keyGs).get();

      //long duration = System.currentTimeMillis() - startTime;
      //LOGGER.info("Glide read time: " + duration + "ms");

      return (gs != null) ? gs.getBytes() : null;
    } catch (Exception e) {
      if (client != null) {
        isBroken = true;
      }
      LOGGER.warning("Failed to read result from cache. Treating it as a cache miss: " + e.getMessage());
      return null;
    } finally {
      if (client != null && readGlideClientPool != null) {
        try {  // ADD: Wrap in try-catch
          this.returnGlideClientBackToPool(client, isBroken, true);
        } catch (Exception ex) {
          LOGGER.warning("Error returning Glide read client: " + ex.getMessage());
        }
      }
    }
  }

  // write using glide client
  private void writeToCacheUsingGlide(String key, byte[] value, int expiry) {
    GlideClusterClient client = null;
    try {
      initializeGlideCacheConnectionIfNeeded(false);
      //long startTime = System.currentTimeMillis();
      // get a client from client pool
      client = writeGlideClientPool.borrowObject();

      byte[] keyHash = computeHashDigest(key.getBytes(StandardCharsets.UTF_8));
      GlideString keyGs = GlideString.of(keyHash);
      GlideString valueGs = GlideString.of(value);
      SetOptions options = SetOptions.builder().expiry(SetOptions.Expiry.Seconds((long) expiry)).build();

      GlideClusterClient finalClient = client;
      client.set(keyGs, valueGs, options).whenComplete((result, throwable) -> {
        handleCompletedGlideCacheWrite(finalClient, throwable);  // FIX: Remove finally block here
      });
      //long duration = System.currentTimeMillis() - startTime;
      //LOGGER.info("Glide write time: " + duration + "ms");
      // LOGGER.finest("Successfully wrote to cache using Glide");
    } catch (Exception e) {
      LOGGER.warning("Unable to start writing to cache using Glide: " + e.getMessage());
      if (client != null && writeGlideClientPool != null) {
        try {
          returnGlideClientBackToPool(client, true, false);
        } catch (Exception ex) {
          LOGGER.warning("Error closing write connection: " + ex.getMessage());
        }
      }
    }
  }


  private void returnGlideClientBackToPool(GlideClusterClient client, boolean isBroken, boolean isRead) {
    GenericObjectPool<GlideClusterClient> pool = isRead ? readGlideClientPool : writeGlideClientPool;
    if (isBroken) {
      try {
        pool.invalidateObject(client);
      } catch (Exception e) {
        throw new RuntimeException("Could not invalidate Glide client for the pool", e);  // FIX: Throw exception
      }
    } else {
      pool.returnObject(client);
    }
  }

  private void handleCompletedGlideCacheWrite(GlideClusterClient client, Throwable ex) {
    if (ex != null) {
      LOGGER.warning("Failed to write to cache using Glide: " + ex.getMessage());
      if (writeGlideClientPool != null) {
        try {
          returnGlideClientBackToPool(client, true, false);
        } catch (Exception e) {
          LOGGER.warning("Error returning broken Glide write client back to pool: " + e.getMessage());
        }
      }
    } else {
      if (writeGlideClientPool != null) {
        try {
          returnGlideClientBackToPool(client, false, false);
        } catch (Exception e) {
          LOGGER.warning("Error returning Glide write client back to pool: " + e.getMessage());
        }
      }
    }
  }
}
