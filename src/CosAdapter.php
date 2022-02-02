<?php

declare(strict_types=1);

namespace Larva\Flysystem\TencentCos;

use League\Flysystem\Adapter\AbstractAdapter;
use League\Flysystem\Adapter\CanOverwriteFiles;
use League\Flysystem\Adapter\Polyfill\StreamedTrait;
use League\Flysystem\AdapterInterface;
use League\Flysystem\Config;
use League\Flysystem\Util;
use Qcloud\Cos\Client;
use Qcloud\Cos\Exception\ServiceResponseException;

/**
 * 腾讯云COS适配器
 */
class CosAdapter extends AbstractAdapter implements CanOverwriteFiles
{
    use StreamedTrait;

    /**
     * @var Client
     */
    protected Client $client;

    /**
     * @var string
     */
    private string $bucket;

    /**
     * @var array
     */
    protected array $config = [];

    /**
     * Adapter constructor.
     *
     * @param Client $client
     * @param string $bucket
     * @param array $config
     */
    public function __construct(Client $client, string $bucket, array $config)
    {
        $this->client = $client;
        $this->bucket = $bucket;
        $this->config = $config;
        if (isset($config['prefix'])) {
            $this->setPathPrefix($config['prefix']);
        }
    }

    public function rename($path, $newpath)
    {
        if (!$this->copy($path, $newpath)) {
            return false;
        }
        return $this->delete($path);
    }

    public function copy($path, $newpath)
    {
        try {
            $result = $this->client->headObject([
                'Bucket' => $this->bucket,
                'Key' => $this->applyPathPrefix($path),
            ]);
            $this->client->copyObject([
                'Bucket' => $this->bucket,
                'Key' => $this->applyPathPrefix($newpath),
                'CopySource' => $result['Location'],
            ]);
        } catch (ServiceResponseException $e) {
            return false;
        }
        return true;
    }

    /**
     * 删除文件
     * @param string $path
     * @return bool
     */
    public function delete($path): bool
    {
        try {
            $this->client->deleteObject([
                'Bucket' => $this->bucket,
                'Key' => $this->applyPathPrefix($path),
            ]);
        } catch (ServiceResponseException $e) {
            return false;
        }
        return true;
    }

    /**
     * 删除目录
     * @param string $dirname
     * @return bool
     */
    public function deleteDir($dirname): bool
    {
        try {
            return (bool)$this->client->deleteObject([
                'Bucket' => $this->bucket,
                'Key' => $this->applyPathPrefix($dirname) . '/',
            ]);
        } catch (ServiceResponseException $e) {
            return false;
        }
    }

    public function createDir($dirname, Config $config)
    {
        try {
            $this->client->putObject([
                'Bucket' => $this->bucket,
                'Key' => $this->applyPathPrefix($dirname) . '/',
                'Body' => '',
            ]);
        } catch (ServiceResponseException $e) {
            return false;
        }
        return ['path' => $dirname, 'type' => 'dir'];
    }

    public function setVisibility($path, $visibility)
    {
        $location = $this->applyPathPrefix($path);
        try {
            $this->client->putObjectAcl([
                'Bucket' => $this->bucket,
                'Key' => $location,
                'ACL' => $this->normalizeVisibility($visibility),
            ]);
        } catch (ServiceResponseException $e) {
            return false;
        }
        return $this->getMetadata($path);
    }

    public function has($path): bool
    {
        $object = $this->applyPathPrefix($path);
        try {
            $this->client->headObject([
                'Bucket' => $this->bucket,
                'Key' => $object,
            ]);
            return true;
        } catch (ServiceResponseException $e) {
            return false;
        }
    }

    public function listContents($directory = '', $recursive = false, $marker = '')
    {
        try {
            return $this->client->listObjects([
                'Bucket' => $this->bucket,
                'Prefix' => ($directory === '') ? '' : ($directory . '/'),
                'Delimiter' => $recursive ? '' : '/',
                'Marker' => $marker,
                'MaxKeys' => 1000,
            ]);
        } catch (ServiceResponseException $e) {
            return [
                'Contents' => [],
                'IsTruncated' => false,
                'NextMarker' => '',
            ];
        }
    }

    public function getMetadata($path)
    {
        try {
            $result = $this->client->headObject([
                'Bucket' => $this->bucket,
                'Key' => $this->applyPathPrefix($path),
            ]);
        } catch (ServiceResponseException $e) {
            return false;
        }
        return [
            'type' => 'file',
            'dirname' => Util::dirname($path),
            'path' => $path,
            'timestamp' => strtotime($result['LastModified']),
            'mimetype' => $result['ContentType'],
            'size' => $result['ContentLength'],
        ];
    }

    public function getSize($path)
    {
        $meta = $this->getMetadata($path);
        return isset($meta['size'])
            ? ['size' => $meta['size']] : null;
    }

    public function getMimetype($path)
    {
        $meta = $this->getMetadata($path);
        return isset($meta['mimetype'])
            ? ['mimetype' => $meta['mimetype']] : null;
    }

    public function getTimestamp($path)
    {
        $meta = $this->getMetadata($path);
        return isset($meta['timestamp'])
            ? ['timestamp' => strtotime($meta['timestamp'])] : null;
    }

    public function getVisibility($path)
    {
        try {
            $response = $this->client->getObjectAcl([
                'Bucket' => $this->bucket,
                'Key' => $this->applyPathPrefix($path),
            ]);
            foreach ($response['Grants'] as $grant) {
                if (isset($grant['Grantee']['URI'])
                    && $grant['Permission'] === 'READ'
                    && strpos($grant['Grantee']['URI'], 'global/AllUsers') !== false
                ) {
                    return ['visibility' => AdapterInterface::VISIBILITY_PUBLIC];
                }
            }
            return ['visibility' => AdapterInterface::VISIBILITY_PRIVATE];
        } catch (ServiceResponseException $e) {
            return false;
        }
    }

    public function read($path)
    {
        try {
            $response = $this->client->getObject([
                'Bucket' => $this->bucket,
                'Key' => $this->applyPathPrefix($path)
            ]);
            $contents = (string)$response['Body'];
        } catch (ServiceResponseException $e) {
            return false;
        }
        return compact('contents', 'path');
    }

    public function write($path, $contents, Config $config)
    {
        $options = $this->prepareUploadConfig($config);
        try {
            $this->client->upload($this->bucket, $this->applyPathPrefix($path), $contents, $options);
            if (!isset($options['length'])) {
                $options['length'] = Util::contentSize($contents);
            }
            if (!isset($options['Content-Type'])) {
                $options['Content-Type'] = Util::guessMimeType($path, $contents);
            }
            $type = 'file';
            $result = compact('type', 'path', 'contents');
            $result['mimetype'] = $options['Content-Type'];
            $result['size'] = $options['length'];
            return $result;
        } catch (ServiceResponseException $e) {
            return false;
        }
    }

    public function update($path, $contents, Config $config)
    {
        return $this->write($path, $contents, $config);
    }

    /**
     * @param $visibility
     *
     * @return string
     */
    private function normalizeVisibility($visibility): string
    {
        switch ($visibility) {
            case AdapterInterface::VISIBILITY_PUBLIC:
                $visibility = 'public-read';
                break;
        }

        return $visibility;
    }

    /**
     * @param Config $config
     *
     * @return array
     */
    private function prepareUploadConfig(Config $config): array
    {
        $options = [];

        if (isset($this->config['encrypt']) && $this->config['encrypt']) {
            $options['params']['ServerSideEncryption'] = 'AES256';
        }

        if ($config->has('params')) {
            $options['params'] = $config->get('params');
        }

        if ($config->has('visibility')) {
            $options['params']['ACL'] = $this->normalizeVisibility($config->get('visibility'));
        }

        return $options;
    }
}