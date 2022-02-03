<?php
declare(strict_types=1);

namespace Larva\Flysystem\Tencent;

interface VisibilityConverter
{
    public function visibilityToAcl(string $visibility): string;

    public function aclToVisibility(array $grants): string;

    public function defaultForDirectories(): string;
}