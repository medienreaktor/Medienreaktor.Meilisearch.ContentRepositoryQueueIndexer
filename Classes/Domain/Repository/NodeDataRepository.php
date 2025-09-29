<?php
declare(strict_types=1);

namespace Medienreaktor\Meilisearch\ContentRepositoryQueueIndexer\Domain\Repository;

/*
 * This file is part of the Medienreaktor.Meilisearch.ContentRepositoryQueueIndexer package.
 *
 * (c) Contributors of the Neos Project - www.neos.io
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Doctrine\ORM\EntityManagerInterface;
use Doctrine\ORM\Internal\Hydration\IterableResult;
use Neos\ContentRepository\Domain\Model\NodeData;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Persistence\Repository;
use Neos\ContentRepository\Domain\Service\NodeTypeManager;

/**
 * @Flow\Scope("singleton")
 */
class NodeDataRepository extends Repository
{
    public const ENTITY_CLASSNAME = NodeData::class;

    /**
     * @Flow\Inject
     * @var EntityManagerInterface
     */
    protected $entityManager;

    /**
     * @Flow\Inject
     * @var NodeTypeManager
     */
    protected $nodeTypeManager;

    /**
     * @param string $workspaceName
     * @param string|null $lastPersistenceObjectIdentifier
     * @param int $maxResults
     * @param string|null $startNodePath Optional node path to start indexing from (includes child nodes)
     * @param string|null $dimensionsHash Optional dimension hash to filter by specific language/dimension combination
     * @return IterableResult
     * @throws \Exception
     */
    public function findAllBySiteAndWorkspace(
        string $workspaceName,
        string $lastPersistenceObjectIdentifier = null,
        int $maxResults = 1000,
        string $startNodePath = null,
        string $dimensionsHash = null
    ): IterableResult {
        $queryBuilder = $this->entityManager->createQueryBuilder();
        $queryBuilder->select('n.Persistence_Object_Identifier persistenceObjectIdentifier, n.identifier identifier, n.dimensionValues dimensions, n.nodeType nodeType, n.path path')
            ->from(NodeData::class, 'n')
            ->where('n.workspace = :workspace AND n.removed = :removed AND n.movedTo IS NULL')
            ->setMaxResults((integer)$maxResults)
            ->setParameters([
                ':workspace' => $workspaceName,
                ':removed' => false,
            ])
            ->orderBy('n.Persistence_Object_Identifier');

        if (!empty($lastPersistenceObjectIdentifier)) {
            $queryBuilder->andWhere($queryBuilder->expr()->gt('n.Persistence_Object_Identifier', $queryBuilder->expr()->literal($lastPersistenceObjectIdentifier)));
        }

        // Filter für Start-Node-Path und Child-Nodes
        if ($startNodePath !== null) {
            $queryBuilder
            ->andWhere($queryBuilder->expr()->like('n.path', ':startNodePathLike'))
            ->setParameter('startNodePathLike', $startNodePath . '%');
        }

        // Filter für Dimension Hash (spezifische Sprache/Dimension)
        if ($dimensionsHash !== null) {
            $queryBuilder->andWhere('n.dimensionsHash = :dimensionsHash')
                ->setParameter('dimensionsHash', $dimensionsHash);
        }

        // $excludedNodeTypes = array_keys(array_filter($this->nodeTypeIndexingConfiguration->getIndexableConfiguration(), static function ($value) {
        //     return !$value;
        // }));

        // if (!empty($excludedNodeTypes)) {
        //     $queryBuilder->andWhere($queryBuilder->expr()->notIn('n.nodeType', $excludedNodeTypes));
        // }

        // Only fulltext Nodes
        $fulltextRootNodeTypes = $this->getFulltextRootNodeTypes();
        if (!empty($fulltextRootNodeTypes)) {
            $queryBuilder->andWhere($queryBuilder->expr()->in('n.nodeType', ':fulltextRootNodeTypes'))
                ->setParameter('fulltextRootNodeTypes', $fulltextRootNodeTypes);
        }

        return $queryBuilder->getQuery()->iterate();
    }

    /**
     * Iterator over an IterableResult and return a Generator
     *
     * This method is useful for batch processing huge result set as it clear the object
     * manager and detach the current object on each iteration.
     *
     * @param IterableResult $iterator
     * @param callable $callback
     * @return \Generator
     */
    public function iterate(IterableResult $iterator, callable $callback = null)
    {
        $iteration = 0;
        foreach ($iterator as $object) {
            $object = current($object);
            yield $object;
            if ($callback !== null) {
                call_user_func($callback, $iteration, $object);
            }
            $iteration++;
        }
    }

    /**
     * Liefert eine Liste der NodeTypes, deren Konfiguration fulltext: true gesetzt hat.
     * @return array<int,string>
     */
    protected function getFulltextRootNodeTypes(): array
    {
        static $cache = null;
        if ($cache !== null) {
            return $cache;
        }
        $fulltext = [];
        $allNodeTypes = $this->nodeTypeManager->getNodeTypes(false);
        foreach ($allNodeTypes as $nodeTypeName => $nodeType) {
            $configuration = $nodeType->getFullConfiguration();
            if (isset($configuration['search']['fulltext']['isRoot']) && $configuration['search']['fulltext']['isRoot'] === true) {
                $fulltext[] = $nodeTypeName;
            }
        }
        $cache = $fulltext;
        return $cache;
    }


}
