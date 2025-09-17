<?php

declare(strict_types=1);

namespace Medienreaktor\Meilisearch\ContentRepositoryQueueIndexer\Domain\Repository;

use Neos\Flow\Annotations as Flow;
use Neos\Flow\Persistence\Repository;
use Neos\Flow\Persistence\PersistenceManagerInterface;
use Doctrine\ORM\EntityManagerInterface;
use Medienreaktor\Meilisearch\ContentRepositoryQueueIndexer\Domain\Model\LastChecked;

/**
 * @Flow\Scope("singleton")
 */
class LastCheckedRepository extends Repository
{
    /**
     * @Flow\Inject
     * @var PersistenceManagerInterface
     */
    protected $persistenceManager;

    /**
     * @Flow\Inject
     * @var EntityManagerInterface
     */
    protected $entityManager;


    public function findFirst(): ?LastChecked
    {
            $query = $this->createQuery();
            return $query->execute()->getFirst();
    }
}
