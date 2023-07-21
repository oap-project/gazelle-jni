#include <memory>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/IO/ReadBufferFromAzureBlobStorage.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/ObjectStorages/AzureBlobStorage/AzureBlobStorageAuth.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/S3/getObjectInfo.h>
#include <IO/S3Common.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/BoundedReadBuffer.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <Storages/HDFS/ReadBufferFromHDFS.h>
#include <Storages/StorageS3Settings.h>
#include <Storages/SubstraitSource/ReadBufferBuilder.h>
#include <Storages/SubstraitSource/SubstraitFileSource.h>
#include <aws/core/client/DefaultRetryStrategy.h>

#include <sys/stat.h>
#include <Poco/URI.h>
#include "IO/ReadSettings.h"

#include <hdfs/hdfs.h>
#include <Poco/Logger.h>
#include <Common/Throttler.h>
#include <Common/logger_useful.h>
#include <Common/safe_cast.h>

#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Interpreters/Cache/FileCacheSettings.h>

#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <Common/CHUtil.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_OPEN_FILE;
    extern const int UNKNOWN_FILE_SIZE;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
}
}

namespace local_engine
{

std::pair<size_t, size_t>
adjustFileReadPosition(DB::SeekableReadBuffer & buffer, size_t read_start_pos, size_t read_end_pos)
{
    auto get_next_line_pos = [&](DB::SeekableReadBuffer & buf) -> size_t
    {
        while (!buf.eof())
        {
            if (*buf.position() == '\r')
            {
                ++buf.position();

                if (!buf.eof() && *buf.position() == '\n')
                {
                    ++buf.position();
                }

                return buf.getPosition();
            }
            else if (*buf.position() == '\n')
            {
                ++buf.position();
                return buf.getPosition();
            }

            ++buf.position();
        }

        return buf.getPosition();
    };

    std::pair<size_t, size_t> result;

    if (read_start_pos == 0)
        result.first = read_start_pos;
    else
    {
        buffer.seek(read_start_pos, SEEK_SET);
        result.first = get_next_line_pos(buffer);
    }

    if (read_end_pos == 0)
        result.second = read_end_pos;
    else
    {
        buffer.seek(read_end_pos, SEEK_SET);
        result.second = get_next_line_pos(buffer);
    }

    return result;
}

class LocalFileReadBufferBuilder : public ReadBufferBuilder
{
public:
    explicit LocalFileReadBufferBuilder(DB::ContextPtr context_) : ReadBufferBuilder(context_) { }
    ~LocalFileReadBufferBuilder() override = default;

    std::unique_ptr<DB::ReadBuffer> build(const substrait::ReadRel::LocalFiles::FileOrFiles & file_info, bool set_read_util_position) override
    {
        Poco::URI file_uri(file_info.uri_file());
        std::unique_ptr<DB::SeekableReadBuffer> read_buffer;
        const String & file_path = file_uri.getPath();
        struct stat file_stat;
        if (stat(file_path.c_str(), &file_stat))
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "file stat failed for {}", file_path);

        if (S_ISREG(file_stat.st_mode))
            read_buffer = std::make_unique<DB::ReadBufferFromFilePRead>(file_path);
        else
            read_buffer = std::make_unique<DB::ReadBufferFromFilePRead>(file_path);


        if (set_read_util_position)
        {
            read_buffer = std::make_unique<DB::BoundedReadBuffer>(std::move(read_buffer));
            auto start_end_pos
                = adjustFileReadPosition(*read_buffer, file_info.start(), file_info.start() + file_info.length());
            LOG_DEBUG(
                &Poco::Logger::get("ReadBufferBuilder"),
                "File read start and end position adjusted from {},{} to {},{}",
                file_info.start(),
                file_info.start() + file_info.length(),
                start_end_pos.first,
                start_end_pos.second);

            read_buffer->seek(start_end_pos.first, SEEK_SET);
            read_buffer->setReadUntilPosition(start_end_pos.second);
        }

        return read_buffer;
    }
};

#if USE_HDFS
class HDFSFileReadBufferBuilder : public ReadBufferBuilder
{
public:
    explicit HDFSFileReadBufferBuilder(DB::ContextPtr context_) : ReadBufferBuilder(context_) { }
    ~HDFSFileReadBufferBuilder() override = default;

    std::unique_ptr<DB::ReadBuffer>
    build(const substrait::ReadRel::LocalFiles::FileOrFiles & file_info, bool set_read_util_position) override
    {
        Poco::URI file_uri(file_info.uri_file());
        std::string uri_path = "hdfs://" + file_uri.getHost();
        if (file_uri.getPort())
            uri_path += ":" + std::to_string(file_uri.getPort());

        DB::ReadSettings read_settings;
        std::unique_ptr<DB::ReadBuffer> read_buffer;
        if (set_read_util_position)
        {
            std::pair<size_t, size_t> start_end_pos
                = adjustFileReadStartAndEndPos(file_info.start(), file_info.start() + file_info.length(), uri_path, file_uri.getPath());
            LOG_DEBUG(
                &Poco::Logger::get("ReadBufferBuilder"),
                "File read start and end position adjusted from {},{} to {},{}",
                file_info.start(),
                file_info.start() + file_info.length(),
                start_end_pos.first,
                start_end_pos.second);
            read_buffer = std::make_unique<DB::ReadBufferFromHDFS>(
                uri_path, file_uri.getPath(), context->getConfigRef(), read_settings, start_end_pos.second);

            if (auto * seekable_in = dynamic_cast<DB::SeekableReadBuffer *>(read_buffer.get()))
                if (start_end_pos.first)
                    seekable_in->seek(start_end_pos.first, SEEK_SET);
        }
        else
        {
            read_buffer = std::make_unique<DB::ReadBufferFromHDFS>(uri_path, file_uri.getPath(), context->getConfigRef(), read_settings);
        }
        return read_buffer;
    }

    std::pair<size_t, size_t>
    adjustFileReadStartAndEndPos(size_t read_start_pos, size_t read_end_pos, std::string uri_path, std::string file_path)
    {
        std::string hdfs_file_path = uri_path + file_path;
        auto builder = DB::createHDFSBuilder(hdfs_file_path, context->getConfigRef());
        auto fs = DB::createHDFSFS(builder.get());
        hdfsFile fin = hdfsOpenFile(fs.get(), file_path.c_str(), O_RDONLY, 0, 0, 0);
        if (!fin)
            throw DB::Exception(
                DB::ErrorCodes::CANNOT_OPEN_FILE, "Cannot open hdfs file:{}, error: {}", hdfs_file_path, std::string(hdfsGetLastError()));

        /// Always close hdfs file before exit function.
        SCOPE_EXIT({ hdfsCloseFile(fs.get(), fin); });

        auto hdfs_file_info = hdfsGetPathInfo(fs.get(), file_path.c_str());
        if (!hdfs_file_info)
            throw DB::Exception(
                DB::ErrorCodes::UNKNOWN_FILE_SIZE,
                "Cannot find out file size for :{}, error: {}",
                hdfs_file_path,
                std::string(hdfsGetLastError()));
        size_t hdfs_file_size = hdfs_file_info->mSize;

        /// initial_pos maybe in the middle of a row, so we need to find the next row start position.
        auto get_next_line_pos = [&](hdfsFS hdfsFs, hdfsFile file, size_t initial_pos, size_t file_size) -> size_t
        {
            if (initial_pos == 0 || initial_pos == file_size)
                return initial_pos;

            int seek_ret = hdfsSeek(hdfsFs, file, initial_pos);
            if (seek_ret < 0)
                throw DB::Exception(
                    DB::ErrorCodes::CANNOT_SEEK_THROUGH_FILE,
                    "Fail to seek HDFS file: {}, error: {}",
                    file_path,
                    std::string(hdfsGetLastError()));

            static constexpr size_t buf_size = 1024;
            char buf[buf_size];

            auto do_read = [&]() -> int
            {
                auto n = hdfsRead(hdfsFs, file, buf, buf_size);
                if (n < 0)
                    throw DB::Exception(
                        DB::ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR,
                        "Fail to read HDFS file: {}, error: {}",
                        file_path,
                        std::string(hdfsGetLastError()));

                return n;
            };

            auto pos = initial_pos;
            while (true)
            {
                auto n = do_read();

                /// If read to the end of file, return directly.
                if (n == 0)
                    return pos;

                /// Search for \n or \r\n or \n\r in buffer.
                int i = 0;
                while (i < n)
                {
                    if (buf[i] == '\n')
                    {
                        if (i + 1 < n)
                            return buf[i + 1] == '\r' ? pos + i + 2 : pos + i + 1;

                        /// read again if buffer is not enough.
                        auto m = do_read();
                        if (m == 0)
                            return pos + i + 1;

                        return buf[0] == '\r' ? pos + i + 2 : pos + i + 1;
                    }
                    else if (buf[i] == '\r')
                    {
                        if (i + 1 < n)
                            return buf[i + 1] == '\n' ? pos + i + 2 : pos + i + 1;

                        /// read again if buffer is not enough.
                        auto m = do_read();
                        if (m == 0)
                            return pos + i + 1;

                        return buf[0] == '\n' ? pos + i + 2 : pos + i + 1;
                    }
                    else
                        ++i;
                }

                /// Can't find \n or \r\n or \n\r in current buffer, read again.
                pos += n;
            }
        };

        std::pair<size_t, size_t> result;
        result.first = get_next_line_pos(fs.get(), fin, read_start_pos, hdfs_file_size);
        result.second = get_next_line_pos(fs.get(), fin, read_end_pos, hdfs_file_size);
        return result;
    }
};
#endif

#if USE_AWS_S3
class S3FileReadBufferBuilder : public ReadBufferBuilder
{
public:
    explicit S3FileReadBufferBuilder(DB::ContextPtr context_) : ReadBufferBuilder(context_)
    {
        DB::FileCacheSettings file_cache_settings;
        file_cache_settings.max_size = static_cast<size_t>(context->getConfigRef().getUInt64("s3.local_cache.max_size", 100L << 30));
        auto cache_base_path = context->getConfigRef().getString("s3.local_cache.cache_path", "/tmp/gluten/local_cache");
        if (!fs::exists(cache_base_path))
            fs::create_directories(cache_base_path);
        file_cache_settings.base_path = cache_base_path;
        new_settings = DB::ReadSettings();
        new_settings.enable_filesystem_cache = context->getConfigRef().getBool("s3.local_cache.enabled", false);
        if (new_settings.enable_filesystem_cache)
        {
            auto cache = DB::FileCacheFactory::instance().getOrCreate("s3_local_cache", file_cache_settings);
            cache->initialize();

            new_settings.remote_fs_cache = cache;
        }
    }

    ~S3FileReadBufferBuilder() override = default;

    std::unique_ptr<DB::ReadBuffer>
    build(const substrait::ReadRel::LocalFiles::FileOrFiles & file_info, bool set_read_util_position) override
    {
        Poco::URI file_uri(file_info.uri_file());
        // file uri looks like: s3a://my-dev-bucket/tpch100/part/0001.parquet
        std::string bucket = file_uri.getHost();
        const auto client = getClient(bucket);
        std::string key = file_uri.getPath().substr(1);
        size_t object_size = DB::S3::getObjectSize(*client, bucket, key, "");

        auto read_buffer_creator
            = [bucket, client, this](const std::string & path, size_t read_until_position) -> std::unique_ptr<DB::ReadBufferFromFileBase>
        {
            return std::make_unique<DB::ReadBufferFromS3>(
                client,
                bucket,
                path,
                "",
                DB::S3Settings::RequestSettings(),
                new_settings,
                /* use_external_buffer */ true,
                /* offset */ 0,
                read_until_position,
                /* restricted_seek */ true);
        };

        DB::StoredObjects stored_objects{DB::StoredObject{key, object_size}};
        auto s3_impl = std::make_unique<DB::ReadBufferFromRemoteFSGather>(
            std::move(read_buffer_creator), stored_objects, new_settings, /* cache_log */ nullptr, /* use_external_buffer */ false);

        auto & pool_reader = context->getThreadPoolReader(DB::FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER);
        auto async_reader
            = std::make_unique<DB::AsynchronousBoundedReadBuffer>(std::move(s3_impl), pool_reader, new_settings, nullptr, nullptr);

        if (set_read_util_position)
        {
            auto start_end_pos
                = adjustFileReadPosition(*async_reader, file_info.start(), file_info.start() + file_info.length());
            LOG_DEBUG(
                &Poco::Logger::get("ReadBufferBuilder"),
                "File read start and end position adjusted from {},{} to {},{}",
                file_info.start(),
                file_info.start() + file_info.length(),
                start_end_pos.first,
                start_end_pos.second);

            async_reader->seek(start_end_pos.first, SEEK_SET);
            async_reader->setReadUntilPosition(start_end_pos.second);
        }
        else
        {
            async_reader->setReadUntilEnd();
        }

        if (new_settings.remote_fs_prefetch)
            async_reader->prefetch(Priority{});

        return async_reader;
    }

private:
    // TODO: currently every SubstraitFileSource will create its own ReadBufferBuilder,
    // so the cached clients are not actually shared among different tasks
    std::map<std::string, std::shared_ptr<DB::S3::Client>> per_bucket_clients;
    std::shared_ptr<DB::S3::Client> shared_client;
    DB::ReadSettings new_settings;

    std::string & stripQuote(std::string & s)
    {
        s.erase(remove(s.begin(), s.end(), '\''), s.end());
        return s;
    }

    std::string getSetting(
        const DB::Settings & settings,
        const std::string & bucket_name,
        const std::string & config_name,
        const std::string & default_value = "",
        const bool require_per_bucket = false)
    {
        std::string ret;
        if (!require_per_bucket)
        {
            // if there's a bucket specific config, prefer it to non per bucket config
            if (settings.tryGetString(bucket_name + "." + config_name, ret))
            {
                return stripQuote(ret);
            }
            else
            {
                if (settings.tryGetString(config_name, ret))
                {
                    return stripQuote(ret);
                }
                else
                {
                    return default_value;
                }
            }
        }
        else
        {
            if (settings.tryGetString(bucket_name + "." + config_name, ret))
            {
                return stripQuote(ret);
            }
            else
            {
                return default_value;
            }
        }
    }

    void cacheClient(const std::string & bucket_name, const bool is_per_bucket, std::shared_ptr<DB::S3::Client> client)
    {
        if (is_per_bucket)
        {
            per_bucket_clients[bucket_name] = client;
            if (per_bucket_clients.size() > 200)
            {
                //TODO: auto clean unused client when there're too many cached client
                LOG_WARNING(&Poco::Logger::get("ReadBufferBuilder"), "Too many per_bucket_clients, {}", per_bucket_clients.size());
            }
        }
        else
        {
            shared_client = client;
        }
    }

    std::shared_ptr<DB::S3::Client> getClient(std::string bucket_name)
    {
        const auto & config = context->getConfigRef();
        const auto & settings = context->getSettingsRef();
        bool use_assumed_role = false;
        bool is_per_bucket = false;

        if (!getSetting(settings, bucket_name, BackendInitializerUtil::HADOOP_S3_ASSUMED_ROLE).empty())
            use_assumed_role = true;

        if (!getSetting(settings, bucket_name, BackendInitializerUtil::HADOOP_S3_ASSUMED_ROLE, "", true).empty())
            is_per_bucket = true;

        if (is_per_bucket && per_bucket_clients.find(bucket_name) != per_bucket_clients.end()
            && "true" != getSetting(settings, bucket_name, BackendInitializerUtil::HADOOP_S3_CLIENT_CACHE_IGNORE))
        {
            return per_bucket_clients[bucket_name];
        }

        if (!is_per_bucket && shared_client
            && "true" != getSetting(settings, bucket_name, BackendInitializerUtil::HADOOP_S3_CLIENT_CACHE_IGNORE))
        {
            return shared_client;
        }

        String config_prefix = "s3";
        auto endpoint = getSetting(settings, bucket_name, BackendInitializerUtil::HADOOP_S3_ENDPOINT, "https://s3.us-west-2.amazonaws.com");
        if (!endpoint.starts_with("https://"))
        {
            if (endpoint.starts_with("s3"))
                // as https://docs.cloudera.com/HDPDocuments/HDP3/HDP-3.0.1/bk_cloud-data-access/content/s3-config-parameters.html
                // the fs.s3a.endpoint does not contain https:// prefix
                endpoint = "https://" + endpoint;
            else
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "S3 Endpoint format not right: {}", endpoint);
        }
        String region_name;
        const char * amazon_suffix = ".amazonaws.com";
        const char * amazon_prefix = "https://s3.";
        auto pos = endpoint.find(amazon_suffix);
        if (pos != std::string::npos)
        {
            assert(endpoint.starts_with(amazon_prefix));
            region_name = endpoint.substr(strlen(amazon_prefix), pos - strlen(amazon_prefix));
            assert(region_name.find('.') == std::string::npos);
        }
        // for AWS CN, the endpoint is like: https://s3.cn-north-1.amazonaws.com.cn, can still work

        DB::S3::PocoHTTPClientConfiguration client_configuration = DB::S3::ClientFactory::instance().createClientConfiguration(
            region_name,
            context->getRemoteHostFilter(),
            static_cast<unsigned>(context->getSettingsRef().s3_max_redirects),
            false,
            false,
            nullptr,
            nullptr);

        client_configuration.connectTimeoutMs = config.getUInt(config_prefix + ".connect_timeout_ms", 10000);
        client_configuration.requestTimeoutMs = config.getUInt(config_prefix + ".request_timeout_ms", 5000);
        client_configuration.maxConnections = config.getUInt(config_prefix + ".max_connections", 100);
        client_configuration.endpointOverride = endpoint;

        client_configuration.retryStrategy
            = std::make_shared<Aws::Client::DefaultRetryStrategy>(config.getUInt(config_prefix + ".retry_attempts", 10));

        std::string ak;
        std::string sk;
        settings.tryGetString(BackendInitializerUtil::HADOOP_S3_ACCESS_KEY, ak);
        settings.tryGetString(BackendInitializerUtil::HADOOP_S3_SECRET_KEY, sk);
        stripQuote(ak);
        stripQuote(sk);
        if (use_assumed_role)
        {
            auto new_client = DB::S3::ClientFactory::instance().create(
                client_configuration,
                false,
                ak,
                sk,
                "",
                {},
                {},
                {.use_environment_credentials = true,
                 .use_insecure_imds_request = false,
                 .role_arn = getSetting(settings, bucket_name, BackendInitializerUtil::HADOOP_S3_ASSUMED_ROLE),
                 .session_name = getSetting(settings, bucket_name, BackendInitializerUtil::HADOOP_S3_ASSUMED_SESSION_NAME),
                 .external_id = getSetting(settings, bucket_name, BackendInitializerUtil::HADOOP_S3_ASSUMED_EXTERNAL_ID)});

            //TODO: support online change config for cached per_bucket_clients
            std::shared_ptr<DB::S3::Client> ret = std::move(new_client);
            cacheClient(bucket_name, is_per_bucket, ret);
            return ret;
        }
        else
        {
            auto new_client = DB::S3::ClientFactory::instance().create(
                client_configuration, false, ak, sk, "", {}, {}, {.use_environment_credentials = true, .use_insecure_imds_request = false});

            std::shared_ptr<DB::S3::Client> ret = std::move(new_client);
            cacheClient(bucket_name, is_per_bucket, ret);
            return ret;
        }
    }
};
#endif

#if USE_AZURE_BLOB_STORAGE
class AzureBlobReadBuffer : public ReadBufferBuilder
{
public:
    explicit AzureBlobReadBuffer(DB::ContextPtr context_) : ReadBufferBuilder(context_) { }
    ~AzureBlobReadBuffer() override = default;

    std::unique_ptr<DB::ReadBuffer> build(const substrait::ReadRel::LocalFiles::FileOrFiles & file_info, bool) override
    {
        Poco::URI file_uri(file_info.uri_file());
        std::unique_ptr<DB::ReadBuffer> read_buffer;
        read_buffer = std::make_unique<DB::ReadBufferFromAzureBlobStorage>(getClient(), file_uri.getPath(), DB::ReadSettings(), 5, 5);
        return read_buffer;
    }

private:
    std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> shared_client;

    std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> getClient()
    {
        if (shared_client)
            return shared_client;
        shared_client = DB::getAzureBlobContainerClient(context->getConfigRef(), "blob");
        return shared_client;
    }
};
#endif

void registerReadBufferBuilders()
{
    auto & factory = ReadBufferBuilderFactory::instance();
    factory.registerBuilder("file", [](DB::ContextPtr context_) { return std::make_shared<LocalFileReadBufferBuilder>(context_); });

#if USE_HDFS
    factory.registerBuilder("hdfs", [](DB::ContextPtr context_) { return std::make_shared<HDFSFileReadBufferBuilder>(context_); });
#endif

#if USE_AWS_S3
    factory.registerBuilder("s3", [](DB::ContextPtr context_) { return std::make_shared<S3FileReadBufferBuilder>(context_); });
    factory.registerBuilder("s3a", [](DB::ContextPtr context_) { return std::make_shared<S3FileReadBufferBuilder>(context_); });
#endif

#if USE_AZURE_BLOB_STORAGE
    factory.registerBuilder("wasb", [](DB::ContextPtr context_) { return std::make_shared<AzureBlobReadBuffer>(context_); });
    factory.registerBuilder("wasbs", [](DB::ContextPtr context_) { return std::make_shared<AzureBlobReadBuffer>(context_); });
#endif
}

ReadBufferBuilderFactory & ReadBufferBuilderFactory::instance()
{
    static ReadBufferBuilderFactory instance;
    return instance;
}

ReadBufferBuilderPtr ReadBufferBuilderFactory::createBuilder(const String & schema, DB::ContextPtr context)
{
    auto it = builders.find(schema);
    if (it == builders.end())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Not found read buffer builder for {}", schema);
    return it->second(context);
}

void ReadBufferBuilderFactory::registerBuilder(const String & schema, NewBuilder newer)
{
    auto it = builders.find(schema);
    if (it != builders.end())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "readbuffer builder for {} has been registered", schema);
    builders[schema] = newer;
}

}
