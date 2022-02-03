<?php
/**
 * This is NOT a freeware, use is subject to license terms
 * @copyright Copyright (c) 2010-2099 Jinan Larva Information Technology Co., Ltd.
 * @link http://www.larva.com.cn/
 */

namespace Larva\Flysystem\Tencent\Tests;

use Larva\Flysystem\Tencent\CosAdapter;
use Larva\Flysystem\Tencent\PortableVisibilityConverter;
use League\Flysystem\Config;
use League\Flysystem\FilesystemAdapter;
use PHPUnit\Framework\TestCase;
use Qcloud\Cos\Client;

/**
 * Class AdapterTest
 *
 * @author Tongle Xu <xutongle@gmail.com>
 */
class AdapterTest extends TestCase
{
    public function Provider()
    {
        $config = [
            // 'endpoint' => getenv('COS_ENDPOINT'),
            'region' => getenv('COS_REGION'),
            'credentials' => [
                'appId' => getenv('COS_APP_ID'),
                'secretId' => getenv('COS_SECRET_ID'),
                'secretKey' => getenv('COS_SECRET_KEY'),
                'token' => getenv('COS_TOKEN'),
            ],
            'url' => '',//CDNUrl
            'bucket' => getenv('COS_BUCKET'),
            'schema' => 'https',
            'timeout' => 3600,
            'connect_timeout' => 3600,
            'ip' => null,
            'port' => null,
            'domain' => null,
            'proxy' => null,
            'prefix' => getenv('COS_PREFIX'),//前缀
        ];
        $client = new Client($config);
        $adapter = new CosAdapter($client, $config['bucket'], '');
        $options = [
            'machineId' => PHP_OS . PHP_VERSION,
            'bucket' => getenv('COS_BUCKET'),
        ];
        return [
            [$adapter, $config, $options],
        ];
    }

    /**
     * @dataProvider Provider
     */
    public function testCreateDirectory(FilesystemAdapter $adapter, $config, $options)
    {
        $path = "bar/{$options['machineId']}";
        $adapter->createDirectory(
            $path, new Config()
        );
        $this->assertTrue($adapter->directoryExists($path));
    }

    /**
     * @dataProvider Provider
     */
    public function testWrite(FilesystemAdapter $adapter, $config, $options)
    {
        $path = "foo/{$options['machineId']}/foo.md";
        $adapter->write($path, 'content', new Config());
        $this->assertTrue($adapter->fileExists($path));
    }

    /**
     * @dataProvider Provider
     */
    public function testRead(FilesystemAdapter $adapter, $config, $options)
    {
        $path = "foo/{$options['machineId']}/foo.md";
        $body = $adapter->read($path);
        $this->assertEquals('content', $body);
    }

    /**
     * @dataProvider Provider
     */
    public function testWriteStream(FilesystemAdapter $adapter, $config, $options)
    {
        $temp = tmpfile();
        fwrite($temp, 'writing to tempfile');
        $path = "foo/{$options['machineId']}/bar.md";
        $adapter->writeStream($path, $temp, new Config());
        $this->assertTrue($adapter->fileExists($path));
        fclose($temp);
    }

    /**
     * @dataProvider Provider
     */
    public function testMovingWithUpdatedMetadata(FilesystemAdapter $adapter, $config, $options)
    {
        $path = "foo/{$options['machineId']}/source.txt";
        $adapter->write($path, 'contents to be moved', new Config(['ContentType' => 'text/plain']));
        $mimeTypeSource = $adapter->mimeType($path)->mimeType();
        $this->assertSame('text/plain', $mimeTypeSource);
    }

    /**
     * @dataProvider Provider
     */
    public function testAclViaOptions(FilesystemAdapter $adapter, $config, $options)
    {
        $path = "foo/{$options['machineId']}/v.md";
        $adapter->write($path, 'contents', new Config(['ACL' => 'bucket-owner-full-control']));
        $response = $adapter->client()->GetObjectAcl(['Bucket' => $options['bucket'], 'Key' => $path]);
        $permission = $response['Grants'][0]['Grant']['Permission'];
        $this->assertEquals('FULL_CONTROL', $permission);
        $adapter->delete($path);
    }

    /**
     * @dataProvider Provider
     */
    public function testCopy(FilesystemAdapter $adapter, $config, $options)
    {
        $adapter->copy(
            "foo/{$options['machineId']}/foo.md",
            "/foo/{$options['machineId']}/copy.md",
            new Config()
        );
        $this->assertTrue($adapter->fileExists("/foo/{$options['machineId']}/copy.md"));
    }

    /**
     * @dataProvider Provider
     */
    public function testListContents(FilesystemAdapter $adapter, $config, $options)
    {
        $this->assertArrayHasKey(
            0,
            $adapter->listContents("foo/{$options['machineId']}")
        );
    }

    /**
     * @dataProvider Provider
     */
    public function testDelete(FilesystemAdapter $adapter, $config, $options)
    {
        $path = "foo/{$options['machineId']}/foo.md";
        $this->assertTrue($adapter->fileExists($path));
        $adapter->delete($path);
        $this->assertFalse($adapter->fileExists($path));
    }

    /**
     * @dataProvider Provider
     */
    public function testDeleteDirectory(FilesystemAdapter $adapter, $config, $options)
    {
        $path = "bar/{$options['machineId']}";
        $adapter->deleteDirectory($path);
        $this->assertFalse($adapter->directoryExists($path));
    }
}
