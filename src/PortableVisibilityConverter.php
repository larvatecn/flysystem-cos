<?php

namespace Larva\Flysystem\Tencent;

use League\Flysystem\Visibility;

class PortableVisibilityConverter implements VisibilityConverter
{
    private const PUBLIC_GRANTEE_URI = 'http://cam.qcloud.com/groups/global/AllUsers';
    private const PUBLIC_GRANTS_PERMISSION = 'READ';
    private const PUBLIC_ACL = 'public-read';
    private const PRIVATE_ACL = 'private';

    /**
     * @var string
     */
    private string $defaultForDirectories;

    public function __construct(string $defaultForDirectories = Visibility::PUBLIC)
    {
        $this->defaultForDirectories = $defaultForDirectories;
    }

    public function visibilityToAcl(string $visibility): string
    {
        if ($visibility === Visibility::PUBLIC) {
            return self::PUBLIC_ACL;
        }

        return self::PRIVATE_ACL;
    }

    public function aclToVisibility(array $grants): string
    {
        if (isset($grants[0]['Grant'])) {
            foreach ($grants[0]['Grant'] as $grant) {
                $granteeUri = $grant['Grantee']['URI'] ?? null;
                $permission = $grant['Permission'] ?? null;

                if ($granteeUri === self::PUBLIC_GRANTEE_URI && $permission === self::PUBLIC_GRANTS_PERMISSION) {
                    return Visibility::PUBLIC;
                }
            }
        }

        return Visibility::PRIVATE;
    }

    public function defaultForDirectories(): string
    {
        return $this->defaultForDirectories;
    }
}
