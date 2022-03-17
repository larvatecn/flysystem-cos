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
use Qcloud\Cos\Exception\ServiceResponseException;
use Throwable;

/**
 * 腾讯云COS适配器
 */
class TencentCOSAdapter implements FilesystemAdapter
{
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
     * 判断文件是否存在
     * @param string $path
     * @return bool
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
     * 判断文件夹是否存在
     * @param string $path
     * @return bool
     */
    public function directoryExists(string $path): bool
    {
        // TODO: Implement write() method.
    }

    public function write(string $path, string $contents, Config $config): void
    {
        // TODO: Implement write() method.
    }

    public function writeStream(string $path, $contents, Config $config): void
    {
        // TODO: Implement writeStream() method.
    }

    public function read(string $path): string
    {
        // TODO: Implement read() method.
    }

    public function readStream(string $path)
    {
        // TODO: Implement readStream() method.
    }

    /**
     * 删除对象
     * @param string $path
     */
    public function delete(string $path): void
    {
        $prefixedPath = $this->prefixer->prefixPath($path);
        try {
            $this->client->deleteObject([
                'Bucket' => $this->bucket,
                'Key' => $prefixedPath,
            ]);
        } catch (ServiceResponseException $exception) {
            throw UnableToDeleteFile::atLocation($path, '', $exception);
        }
    }

    /**
     * 删除目录
     * @param string $path
     */
    public function deleteDirectory(string $path): void
    {
        try {
            $prefix = $this->prefixer->prefixDirectoryPath($path);
            $this->client->deleteObject([
                'Bucket' => $this->bucket,
                'Key' => $prefix,
            ]);
        } catch (ServiceResponseException $exception) {
            throw UnableToDeleteDirectory::atLocation($path, '', $exception);
        }
    }

    public function createDirectory(string $path, Config $config): void
    {
        // TODO: Implement createDirectory() method.
    }

    public function setVisibility(string $path, string $visibility): void
    {
        // TODO: Implement setVisibility() method.
    }

    public function visibility(string $path): FileAttributes
    {
        // TODO: Implement visibility() method.
    }

    /**
     * 获取内容类型
     * @param string $path
     * @return FileAttributes
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
     * 获取最后更改
     * @param string $path
     * @return FileAttributes
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
     * 获取文件大小
     * @param string $path
     * @return FileAttributes
     */
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
        // TODO: Implement listContents() method.
    }

    /**
     * 移动对象到新位置
     * @param string $source
     * @param string $destination
     * @param Config $config
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

    public function copy(string $source, string $destination, Config $config): void
    {
        // TODO: Implement copy() method.
    }
}