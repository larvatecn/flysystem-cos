<?php

declare(strict_types=1);

namespace Larva\Flysystem\Tencent;

use League\Flysystem\Config;
use League\Flysystem\DirectoryAttributes;
use League\Flysystem\FileAttributes;
use League\Flysystem\FilesystemAdapter;
use League\Flysystem\FilesystemException;
use League\Flysystem\FilesystemOperationFailed;
use League\Flysystem\InvalidVisibilityProvided;
use League\Flysystem\PathPrefixer;
use League\Flysystem\StorageAttributes;
use League\Flysystem\UnableToCheckDirectoryExistence;
use League\Flysystem\UnableToCheckExistence;
use League\Flysystem\UnableToCheckFileExistence;
use League\Flysystem\UnableToCopyFile;
use League\Flysystem\UnableToCreateDirectory;
use League\Flysystem\UnableToDeleteDirectory;
use League\Flysystem\UnableToDeleteFile;
use League\Flysystem\UnableToMoveFile;
use League\Flysystem\UnableToReadFile;
use League\Flysystem\UnableToRetrieveMetadata;
use League\Flysystem\UnableToSetVisibility;
use League\Flysystem\UnableToWriteFile;
use League\Flysystem\UnixVisibility\VisibilityConverter;
use League\MimeTypeDetection\FinfoMimeTypeDetector;
use League\MimeTypeDetection\MimeTypeDetector;
use Qcloud\Cos\Client;
use Throwable;

/**
 * 腾讯云COS适配器
 */
class CosAdapter implements FilesystemAdapter
{
    /**
     * @var string[]
     */
    public const AVAILABLE_OPTIONS = [
        'ACL',
        'CacheControl',
        'ContentDisposition',
        'ContentEncoding',
        'ContentLength',
        'ContentType',
        'Expires',
        'Metadata',//用户自定义元信息
        'MetadataDirective',
        'ServerSideEncryption',//服务端加密方法
        'StorageClass',//文件的存储类型，例如 STANDARD、STANDARD_IA、ARCHIVE，默认值：STANDARD。
    ];

    /**
     * @var string[]
     */
    public const MUP_AVAILABLE_OPTIONS = [
        'Concurrency',//并发度
        'PartSize',//最小分块文件大小，默认为5M
    ];

    /**
     * @var string[]
     */
    private const EXTRA_METADATA_FIELDS = [
        'Metadata',
        'StorageClass',
        'ETag',
        'VersionId',
    ];

    /**
     * @var Client
     */
    protected Client $client;

    /**
     * @var PathPrefixer
     */
    private $prefixer;

    /**
     * @var string
     */
    private $bucket;

    /**
     * @var VisibilityConverter
     */
    private $visibility;

    /**
     * @var MimeTypeDetector
     */
    private $mimeTypeDetector;

    /**
     * @var array
     */
    private $options;

    /**
     * @var bool
     */
    private $streamReads;

    /**
     * Adapter constructor.
     *
     * @param Client $client
     * @param string $bucket
     * @param string $prefix
     * @param VisibilityConverter|null $visibility
     * @param MimeTypeDetector|null $mimeTypeDetector
     * @param array $options
     * @param bool $streamReads
     */
    public function __construct(Client $client, string $bucket, string $prefix = '', VisibilityConverter $visibility = null, MimeTypeDetector $mimeTypeDetector = null, array $options = [], bool $streamReads = true)
    {
        $this->client = $client;
        $this->prefixer = new PathPrefixer($prefix);
        $this->bucket = $bucket;
        $this->visibility = $visibility ?: new PortableVisibilityConverter();
        $this->mimeTypeDetector = $mimeTypeDetector ?: new FinfoMimeTypeDetector();
        $this->options = $options;
        $this->streamReads = $streamReads;
    }

    /**
     * 判断文件是否存在
     * @param string $path
     * @return bool
     */
    public function fileExists(string $path): bool
    {
        try {
            return $this->client->doesObjectExist($this->bucket, $this->prefixer->prefixPath($path), $this->options);
        } catch (Throwable $exception) {
            throw UnableToCheckFileExistence::forLocation($path, $exception);
        }
    }

    /**
     * 判断文件夹是否存在
     * @param string $path
     * @return bool
     */
    public function directoryExists(string $path): bool
    {
        try {
            $prefix = $this->prefixer->prefixDirectoryPath($path);
            $result = $this->client->listObjects([
                'Bucket' => $this->bucket, 'Prefix' => $prefix, 'Delimiter' => '/', 'MaxKeys' => 1
            ]);
            return isset($result['Contents']);
        } catch (Throwable $exception) {
            throw UnableToCheckDirectoryExistence::forLocation($path, $exception);
        }
    }

    /**
     * 写入文件
     * @param string $path
     * @param string $contents
     * @param Config $config
     * @return void
     */
    public function write(string $path, string $contents, Config $config): void
    {
        $this->upload($path, $contents, $config);
    }

    /**
     * 写入流
     * @param string $path
     * @param $contents
     * @param Config $config
     * @return void
     */
    public function writeStream(string $path, $contents, Config $config): void
    {
        $this->upload($path, $contents, $config);
    }

    /**
     * 读取文件内容
     * @param string $path
     * @return string
     */
    public function read(string $path): string
    {
        try {
            $response = $this->client->getObject([
                'Bucket' => $this->bucket,
                'Key' => $this->prefixer->prefixPath($path)
            ]);
            return (string)$response['Body'];
        } catch (Throwable $exception) {
            throw UnableToReadFile::fromLocation($path, '', $exception);
        }
    }

    public function readStream(string $path)
    {
        // TODO: Implement readStream() method.
    }

    /**
     * 删除对象
     * @param string $path
     * @return void
     */
    public function delete(string $path): void
    {
        try {
            $this->client->DeleteObject([
                'Bucket' => $this->bucket,
                'Key' => $this->prefixer->prefixPath($path),
            ]);
        } catch (Throwable $exception) {
            throw UnableToDeleteFile::atLocation($path, '', $exception);
        }
    }

    /**
     * 删除文件夹
     * @param string $path
     * @return void
     */
    public function deleteDirectory(string $path): void
    {
        $prefix = $this->prefixer->prefixPath($path);
        $prefix = ltrim(rtrim($prefix, '/') . '/', '/');
        try {
            $this->client->deleteObject([
                'Bucket' => $this->bucket,
                'Key' => $prefix,
            ]);
        } catch (Throwable $exception) {
            throw UnableToDeleteDirectory::atLocation($path, '', $exception);
        }
    }

    /**
     * 创建文件夹
     * @param string $path
     * @param Config $config
     * @return void
     */
    public function createDirectory(string $path, Config $config): void
    {
        $prefix = $this->prefixer->prefixPath($path);
        $prefix = ltrim(rtrim($prefix, '/') . '/', '/');
        $config = $config->withDefaults(['visibility' => $this->visibility->defaultForDirectories()]);
        try {
            $this->upload($prefix, '', $config);
        } catch (Throwable $exception) {
            throw UnableToCreateDirectory::atLocation($path, '', $exception);
        }
    }

    /**
     * 设置文件可见性
     * @param string $path
     * @param string $visibility
     * @return void
     */
    public function setVisibility(string $path, string $visibility): void
    {
        try {
            $this->client->putObjectAcl([
                'Bucket' => $this->bucket,
                'Key' => $this->prefixer->prefixPath($path),
                'ACL' => $this->visibility->visibilityToAcl($visibility),
            ]);
        } catch (Throwable $exception) {
            throw UnableToSetVisibility::atLocation($path, '', $exception);
        }
    }

    /**
     * 获取文件可见性
     * @param string $path
     * @return FileAttributes
     */
    public function visibility(string $path): FileAttributes
    {
        try {
            $result = $this->client->getObjectAcl([
                'Bucket' => $this->bucket,
                'Key' => $this->prefixer->prefixPath($path),
            ]);
        } catch (Throwable $exception) {
            throw UnableToRetrieveMetadata::visibility($path, '', $exception);
        }
        $visibility = $this->visibility->aclToVisibility((array)$result['Grants']);

        return new FileAttributes($path, null, $visibility);
    }

    public function mimeType(string $path): FileAttributes
    {
        $attributes = $this->fetchFileMetadata($path, FileAttributes::ATTRIBUTE_MIME_TYPE);
        if ($attributes->mimeType() === null) {
            throw UnableToRetrieveMetadata::mimeType($path);
        }
        return $attributes;
    }

    public function lastModified(string $path): FileAttributes
    {
        $attributes = $this->fetchFileMetadata($path, FileAttributes::ATTRIBUTE_LAST_MODIFIED);
        if ($attributes->lastModified() === null) {
            throw UnableToRetrieveMetadata::lastModified($path);
        }
        return $attributes;
    }

    public function fileSize(string $path): FileAttributes
    {
        $attributes = $this->fetchFileMetadata($path, FileAttributes::ATTRIBUTE_FILE_SIZE);
        if ($attributes->fileSize() === null) {
            throw UnableToRetrieveMetadata::fileSize($path);
        }
        return $attributes;
    }

    public function listContents(string $path, bool $deep): iterable
    {
        $prefix = trim($this->prefixer->prefixPath($path), '/');
        $prefix = empty($prefix) ? '' : $prefix . '/';
        $options = ['Bucket' => $this->bucket, 'Prefix' => $prefix];
        if ($deep === false) {
            $options['Delimiter'] = '/';
        }
        $listing = $this->retrievePaginatedListing($options);

        foreach ($listing as $item) {
            yield $this->mapObjectMetadata($item);
        }
    }

    /**
     * 移动文件到新的位置
     * @param string $source
     * @param string $destination
     * @param Config $config
     * @return void
     * @throws FilesystemException
     */
    public function move(string $source, string $destination, Config $config): void
    {
        try {
            $this->copy($source, $destination, $config);
            $this->delete($source);
        } catch (FilesystemOperationFailed $exception) {
            throw UnableToMoveFile::fromLocationTo($source, $destination, $exception);
        }
    }

    /**
     * 复制文件对象
     * @param string $source
     * @param string $destination
     * @param Config $config
     * @return void
     */
    public function copy(string $source, string $destination, Config $config): void
    {
        try {
            $result = $this->client->headObject([
                'Bucket' => $this->bucket,
                'Key' => $this->prefixer->prefixPath($source),
            ]);
            $this->client->copyObject([
                'Bucket' => $this->bucket,
                'Key' => $this->prefixer->prefixPath($destination),
                'CopySource' => $result['Location'],
            ]);
        } catch (Throwable  $exception) {
            throw UnableToCopyFile::fromLocationTo($source, $destination, $exception);
        }
    }

    /**
     * 直接获取 COS 客户端
     * @return Client
     */
    public function client(): Client
    {
        return $this->client;
    }

    private function retrievePaginatedListing(array $options): Generator
    {
        $resultPaginator = $this->client->ListObjects($options + $this->options);

        foreach ($resultPaginator as $result) {
            yield from ($result->get('CommonPrefixes') ?: []);
            yield from ($result->get('Contents') ?: []);
        }
    }

    private function createOptionsFromConfig(Config $config): array
    {
        $options = ['params' => []];

        foreach (static::AVAILABLE_OPTIONS as $option) {
            $value = $config->get($option, '__NOT_SET__');

            if ($value !== '__NOT_SET__') {
                $options['params'][$option] = $value;
            }
        }

        foreach (static::MUP_AVAILABLE_OPTIONS as $option) {
            $value = $config->get($option, '__NOT_SET__');

            if ($value !== '__NOT_SET__') {
                $options[$option] = $value;
            }
        }

        return $options + $this->options;
    }

    private function upload(string $path, $body, Config $config): void
    {
        $key = $this->prefixer->prefixPath($path);
        $options = $this->createOptionsFromConfig($config);
        $shouldDetermineMimetype = $body !== '' && !array_key_exists('ContentType', $options['params']);
        if ($shouldDetermineMimetype && $mimeType = $this->mimeTypeDetector->detectMimeType($key, $body)) {
            $options['params']['ContentType'] = $mimeType;
        }
        try {
            $this->client->upload($this->bucket, $key, $body, $options);
        } catch (Throwable $exception) {
            throw UnableToWriteFile::atLocation($path, '', $exception);
        }
    }

    /**
     * 获取 文件 Meta 数据
     * @param string $path
     * @param string $type
     * @return FileAttributes
     */
    private function fetchFileMetadata(string $path, string $type): FileAttributes
    {
        try {
            $result = $this->client->headObject([
                'Bucket' => $this->bucket,
                'Key' => $this->prefixer->prefixPath($path),
            ]);
        } catch (Throwable $exception) {
            throw UnableToRetrieveMetadata::create($path, $type, '', $exception);
        }
        $attributes = $this->mapObjectMetadata($result->toArray(), $path);
        if (!$attributes instanceof FileAttributes) {
            throw UnableToRetrieveMetadata::create($path, $type, '');
        }
        return $attributes;
    }

    /**
     * 导出扩展 Meta Data
     * @param array $metadata
     * @return array
     */
    private function extractExtraMetadata(array $metadata): array
    {
        $extracted = [];
        foreach (static::EXTRA_METADATA_FIELDS as $field) {
            if (isset($metadata[$field]) && $metadata[$field] !== '') {
                $extracted[$field] = $metadata[$field];
            }
        }
        return $extracted;
    }

    /**
     * 映射Meta
     * @param array $metadata
     * @param string|null $path
     * @return StorageAttributes
     */
    private function mapObjectMetadata(array $metadata, string $path = null): StorageAttributes
    {
        if ($path === null) {
            $path = $this->prefixer->stripPrefix($metadata['Key'] ?? $metadata['Prefix']);
        }
        if (str_ends_with($path, '/')) {
            return new DirectoryAttributes(rtrim($path, '/'));
        }
        $mimetype = $metadata['ContentType'] ?? null;
        $fileSize = $metadata['ContentLength'] ?? $metadata['Size'] ?? null;
        $fileSize = $fileSize === null ? null : (int)$fileSize;
        $dateTime = $metadata['LastModified'] ?? null;
        $lastModified = $dateTime ? strtotime($dateTime) : null;
        return new FileAttributes($path, $fileSize, null, $lastModified, $mimetype, $this->extractExtraMetadata($metadata));
    }
}