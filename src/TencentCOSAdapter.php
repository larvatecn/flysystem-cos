<?php

namespace Larva\Flysystem\Tencent;

use GuzzleHttp\Psr7\Utils;
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
use League\Flysystem\UnableToCopyFile;
use League\Flysystem\UnableToCreateDirectory;
use League\Flysystem\UnableToDeleteDirectory;
use League\Flysystem\UnableToDeleteFile;
use League\Flysystem\UnableToMoveFile;
use League\Flysystem\UnableToReadFile;
use League\Flysystem\UnableToRetrieveMetadata;
use League\Flysystem\UnableToSetVisibility;
use League\Flysystem\UnableToWriteFile;
use League\Flysystem\Visibility;
use League\MimeTypeDetection\FinfoMimeTypeDetector;
use League\MimeTypeDetection\MimeTypeDetector;
use Qcloud\Cos\Client;
use Qcloud\Cos\Exception\ServiceResponseException;
use Throwable;
use Generator;

/**
 * 腾讯云COS适配器
 */
class TencentCOSAdapter implements FilesystemAdapter
{
    /**
     * @var string[]
     */
    public const AVAILABLE_OPTIONS = [
        'Cache-Control', 'Content-Disposition', 'Content-Encoding', 'Content-Type', 'Expires', 'x-cos-acl',
        'x-cos-grant-read', 'x-cos-grant-read-acp', 'x-cos-grant-write-acp', 'x-cos-grant-full-control',
        'x-cos-traffic-limit', 'x-cos-tagging', 'x-cos-storage-class',
    ];

    /**
     * @var string[]
     */
    public const MUP_AVAILABLE_OPTIONS = [
        'Retry', 'PartSize'
    ];

    /**
     * 扩展 MetaData 字段
     * @var string[]
     */
    private const EXTRA_METADATA_FIELDS = [
        'StorageClass',
        'ETag',
        'x-cos-storage-class', 'x-cos-storage-tier', 'x-cos-restore', 'x-cos-restore-status', 'x-cos-version-id'
    ];

    /**
     * @var Client
     */
    private Client $client;

    /**
     * @var PathPrefixer
     */
    private PathPrefixer $prefixer;

    /**
     * @var string
     */
    private string $bucket;

    /**
     * @var VisibilityConverter
     */
    private VisibilityConverter $visibility;

    /**
     * @var MimeTypeDetector
     */
    private MimeTypeDetector $mimeTypeDetector;

    /**
     * @var array
     */
    private array $options;

    /**
     * Adapter constructor.
     *
     * @param Client $client
     * @param string $bucket
     * @param string $prefix
     * @param VisibilityConverter|null $visibility
     * @param MimeTypeDetector|null $mimeTypeDetector
     * @param array $options
     */
    public function __construct(Client $client, string $bucket, string $prefix = '', VisibilityConverter $visibility = null, MimeTypeDetector $mimeTypeDetector = null, array $options = [])
    {
        $this->client = $client;
        $this->prefixer = new PathPrefixer($prefix);
        $this->bucket = $bucket;
        $this->visibility = $visibility ?: new PortableVisibilityConverter();
        $this->mimeTypeDetector = $mimeTypeDetector ?: new FinfoMimeTypeDetector();
        $this->options = $options;
    }

    /**
     * 对象是否存在
     *
     * @throws FilesystemException
     * @throws UnableToCheckExistence
     */
    public function fileExists(string $path): bool
    {
        try {
            return $this->client->doesObjectExist($this->bucket, $this->prefixer->prefixPath($path), $this->options);
        } catch (Throwable $exception) {
            throw UnableToCheckExistence::forLocation($path, $exception);
        }
    }

    /**
     * 目录是否存在
     *
     * @throws FilesystemException
     * @throws UnableToCheckExistence
     */
    public function directoryExists(string $path): bool
    {
        try {
            $prefix = $this->prefixer->prefixDirectoryPath($path);
            $response = $this->client->ListObjects(['Bucket' => $this->bucket, 'Prefix' => $prefix, 'Delimiter' => '/']);
            return isset($response['Contents']);
        } catch (Throwable $exception) {
            throw UnableToCheckDirectoryExistence::forLocation($path, $exception);
        }
    }

    /**
     * 写入内容到对象中
     *
     * @throws UnableToWriteFile
     * @throws FilesystemException
     */
    public function write(string $path, string $contents, Config $config): void
    {
        $this->upload($path, $contents, $config);
    }

    /**
     * @param string $path
     * @param string|resource $body
     * @param Config $config
     */
    private function upload(string $path, $body, Config $config): void
    {
        $key = $this->prefixer->prefixPath($path);
        $options = $this->createOptionsFromConfig($config);
        $options['x-cos-acl'] ?? $this->determineAcl($config);
        $shouldDetermineMimetype = $body !== '' && !array_key_exists('ContentType', $options);
        if ($shouldDetermineMimetype && $mimeType = $this->mimeTypeDetector->detectMimeType($key, $body)) {
            $options['ContentType'] = $mimeType;
        }

        try {
            $this->client->upload($this->bucket, $key, $body, $options);
        } catch (Throwable $exception) {
            throw UnableToWriteFile::atLocation($path, '', $exception);
        }
    }

    /**
     * 转换ACL
     * @param Config $config
     * @return string
     */
    private function determineAcl(Config $config): string
    {
        $visibility = (string)$config->get(Config::OPTION_VISIBILITY, Visibility::PRIVATE);

        return $this->visibility->visibilityToAcl($visibility);
    }

    /**
     * 从配置创建参数
     * @param Config $config
     * @return array
     */
    private function createOptionsFromConfig(Config $config): array
    {
        $options = [];
        foreach (static::AVAILABLE_OPTIONS as $option) {
            $value = $config->get($option, '__NOT_SET__');

            if ($value !== '__NOT_SET__') {
                $options[$option] = $value;
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

    /**
     * 写入流到对象
     *
     * @param resource $contents
     *
     * @throws UnableToWriteFile
     * @throws FilesystemException
     */
    public function writeStream(string $path, $contents, Config $config): void
    {
        $this->upload($path, $contents, $config);
    }

    /**
     * 读取对象内容
     *
     * @throws UnableToReadFile
     * @throws FilesystemException
     */
    public function read(string $path): string
    {
        $prefixedPath = $this->prefixer->prefixPath($path);
        try {
            $response = $this->client->GetObject(['Bucket' => $this->bucket, 'Key' => $prefixedPath]);
            return (string)$response['Body'];
        } catch (Throwable $exception) {
            throw UnableToReadFile::fromLocation($path, $exception->getMessage());
        }
    }

    /**
     * 读取对象到流
     *
     * @return resource
     *
     * @throws UnableToReadFile
     * @throws FilesystemException
     */
    public function readStream(string $path)
    {
        try {
            $data = $this->read($path);
            $stream = fopen('php://temp', 'r+');
            fwrite($stream, (string)$data);
            rewind($stream);
            return $stream;
        } catch (Throwable $exception) {
            throw UnableToReadFile::fromLocation($path, $exception->getMessage());
        }
    }

    /**
     * 删除对象
     *
     * @throws UnableToDeleteFile
     * @throws FilesystemException
     */
    public function delete(string $path): void
    {
        try {
            $this->client->DeleteObject(['Bucket' => $this->bucket, 'Key' => $this->prefixer->prefixPath($path)]);
        } catch (Throwable $exception) {
            throw UnableToDeleteFile::atLocation($path, '', $exception);
        }
    }

    /**
     * 删除目录
     *
     * @throws UnableToDeleteDirectory
     * @throws FilesystemException
     */
    public function deleteDirectory(string $path): void
    {
        $prefix = trim($this->prefixer->prefixPath($path), '/');
        $prefix = empty($prefix) ? '' : $prefix . '/';
        $options = ['Bucket' => $this->bucket, 'Prefix' => $prefix];
        $objectListInfo = $this->listObjects($options, true);
        if (empty($objectListInfo['Contents'])) {
            return;
        }
        $objects = array_map(function ($item) {
            return ['Key' => $item['Key']];
        }, $objectListInfo['Contents']);
        try {
            $this->client->DeleteObjects(['Bucket' => $this->bucket, 'Objects' => $objects]);
        } catch (ServiceResponseException $exception) {
            throw UnableToDeleteDirectory::atLocation($path, '', $exception);
        }
    }

    /**
     * 创建目录
     *
     * @throws UnableToCreateDirectory
     * @throws FilesystemException
     */
    public function createDirectory(string $path, Config $config): void
    {
        $config = $config->withDefaults(['visibility' => $this->visibility->defaultForDirectories()]);
        $this->upload(rtrim($path, '/') . '/', '', $config);
    }

    /**
     * 设置对象可见性
     *
     * @throws InvalidVisibilityProvided
     * @throws FilesystemException
     */
    public function setVisibility(string $path, string $visibility): void
    {
        $arguments = [
            'Bucket' => $this->bucket,
            'Key' => $this->prefixer->prefixPath($path),
            'ACL' => $this->visibility->visibilityToAcl($visibility),
        ];
        try {
            $this->client->PutObjectAcl($arguments);
        } catch (Throwable $exception) {
            throw UnableToSetVisibility::atLocation($path, '', $exception);
        }
    }

    /**
     * 获取对象可见性
     *
     * @throws UnableToRetrieveMetadata
     * @throws FilesystemException
     */
    public function visibility(string $path): FileAttributes
    {
        $arguments = ['Bucket' => $this->bucket, 'Key' => $this->prefixer->prefixPath($path)];
        try {
            $result = $this->client->GetObjectAcl($arguments);
        } catch (Throwable $exception) {
            throw UnableToRetrieveMetadata::visibility($path, '', $exception);
        }
        $visibility = $this->visibility->aclToVisibility((array)$result['Grants']);
        return new FileAttributes($path, null, $visibility);
    }

    /**
     * 获取文件 MetaData
     * @param string $path
     * @param string $type
     * @return FileAttributes|null
     */
    private function fetchFileMetadata(string $path, string $type): ?FileAttributes
    {
        try {
            $meta = $this->client->HeadObject(['Bucket' => $this->bucket, 'Key' => $this->prefixer->prefixPath($path)]);
        } catch (ServiceResponseException $exception) {
            throw UnableToRetrieveMetadata::create($path, $type, '', $exception);
        }
        $attributes = $this->mapObjectMetadata($meta->toArray(), $path);
        if (!$attributes instanceof FileAttributes) {
            throw UnableToRetrieveMetadata::create($path, $type, '');
        }
        return $attributes;
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
            $path = $this->prefixer->stripPrefix($metadata['Key']);
        }
        if (str_ends_with($path, '/')) {
            return new DirectoryAttributes(rtrim($path, '/'));
        }
        $mimetype = $metadata['ContentType'] ?? null;
        $fileSize = $metadata['ContentLength'] ?? null;
        $fileSize = $fileSize === null ? null : (int)$fileSize;
        $dateTime = $metadata['LastModified'] ?? null;
        $lastModified = $dateTime ? strtotime($dateTime) : null;
        return new FileAttributes($path, $fileSize, null, $lastModified, $mimetype, $this->extractExtraMetadata($metadata));
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
     * 获取对象类型
     *
     * @throws UnableToRetrieveMetadata
     * @throws FilesystemException
     */
    public function mimeType(string $path): FileAttributes
    {
        $attributes = $this->fetchFileMetadata($path, FileAttributes::ATTRIBUTE_MIME_TYPE);
        if ($attributes->mimeType() === null) {
            throw UnableToRetrieveMetadata::mimeType($path);
        }
        return $attributes;
    }

    /**
     * 获取对象最后更改时间
     *
     * @throws UnableToRetrieveMetadata
     * @throws FilesystemException
     */
    public function lastModified(string $path): FileAttributes
    {
        $attributes = $this->fetchFileMetadata($path, FileAttributes::ATTRIBUTE_LAST_MODIFIED);
        if ($attributes->lastModified() === null) {
            throw UnableToRetrieveMetadata::lastModified($path);
        }
        return $attributes;
    }

    /**
     * 获取对象大小
     *
     * @throws UnableToRetrieveMetadata
     * @throws FilesystemException
     */
    public function fileSize(string $path): FileAttributes
    {
        $attributes = $this->fetchFileMetadata($path, FileAttributes::ATTRIBUTE_FILE_SIZE);
        if ($attributes->fileSize() === null) {
            throw UnableToRetrieveMetadata::fileSize($path);
        }
        return $attributes;
    }

    /**
     * @return iterable<StorageAttributes>
     *
     * @throws FilesystemException
     */
    public function listContents(string $path, bool $deep): iterable
    {
        $prefix = trim($this->prefixer->prefixPath($path), '/');
        $prefix = empty($prefix) ? '' : $prefix . '/';
        $options = ['Bucket' => $this->bucket, 'Prefix' => $prefix];
        if ($deep === false) {
            $options['Delimiter'] = '/';
        }
        $response = $this->listObjects($options, $deep);

        // 处理目录
        foreach ($response['CommonPrefixes'] ?? [] as $prefix) {
            yield new DirectoryAttributes($prefix['Prefix']);
        }

        foreach ($response['Contents'] ?? [] as $content) {
            yield new FileAttributes($content['Key'], \intval($content['Size']), null, \strtotime($content['LastModified']));
        }
    }

    /**
     * 列出对象
     *
     * @param array $options
     * @return object
     */
    private function listObjects(array $options)
    {
        $result = $this->client->ListObjects($options);
        foreach (['CommonPrefixes', 'Contents'] as $key) {
            $result[$key] = $result[$key] ?? [];
            // 确保是二维数组
            if (($index = \key($result[$key])) !== 0) {
                $result[$key] = \is_null($index) ? [] : [$result[$key]];
            }
        }
        return $result;
    }

    /**
     * 移动对象到新的位置
     *
     * @throws UnableToMoveFile
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
     * 复制对象到新的位置
     *
     * @throws UnableToCopyFile
     * @throws FilesystemException
     */
    public function copy(string $source, string $destination, Config $config): void
    {
        try {
            /** @var string $visibility */
            $visibility = $this->visibility($source)->visibility();
        } catch (Throwable $exception) {
            throw UnableToCopyFile::fromLocationTo($source, $destination, $exception);
        }

        try {
            $destination = $this->prefixer->prefixPath($destination);
            $result = $this->client->HeadObject([
                'Bucket' => $this->bucket,
                'Key' => $this->prefixer->prefixPath($source),
            ]);
            $this->client->CopyObject([
                'Bucket' => $this->bucket,
                'Key' => $destination,
                'CopySource' => $result['Location'],
            ]);
            $this->setVisibility($destination, $visibility);
        } catch (Throwable $exception) {
            throw UnableToCopyFile::fromLocationTo($source, $destination, $exception);
        }
    }
}
