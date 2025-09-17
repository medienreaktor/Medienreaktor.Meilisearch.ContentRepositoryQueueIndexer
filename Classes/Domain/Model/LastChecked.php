<?php

namespace Medienreaktor\Meilisearch\ContentRepositoryQueueIndexer\Domain\Model;

use Doctrine\ORM\Mapping as ORM;
use Neos\Flow\Annotations as Flow;

/**
 * @Flow\Entity
 * @ORM\Table(name="medienreaktor_meilisearch_domain_model_lastchecked")
 */
class LastChecked
{

    /**
     * @var \DateTime|null
     * @ORM\Column(name="lastchecked", type="datetime", nullable=true)
     */
    protected $lastChecked;

    /**
     * @return \DateTime|null
     */
    public function getLastChecked(): ?\DateTime
    {
        return $this->lastChecked;
    }

    /**
     * @param \DateTime|null $lastChecked
     */
    public function setLastChecked(?\DateTime $lastChecked): void
    {
        $this->lastChecked = $lastChecked;
    }
}
?>
