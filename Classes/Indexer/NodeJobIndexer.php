<?php
declare(strict_types=1);

namespace Medienreaktor\Meilisearch\ContentRepositoryQueueIndexer\Indexer;

/*
 * This file is part of the Medienreaktor.Meilisearch.ContentRepositoryQueueIndexer package.
 *
 * (c) Contributors of the Neos Project - www.neos.io
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Medienreaktor\Meilisearch\Indexer\NodeIndexer;
use Medienreaktor\Meilisearch\ContentRepositoryQueueIndexer\Command\NodeIndexQueueCommandController;
use Medienreaktor\Meilisearch\ContentRepositoryQueueIndexer\IndexingJob;
use Medienreaktor\Meilisearch\ContentRepositoryQueueIndexer\RemovalJob;
use Flowpack\JobQueue\Common\Job\JobManager;
use Neos\ContentRepository\Domain\Model\NodeInterface;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Persistence\PersistenceManagerInterface;
use Medienreaktor\Meilisearch\ContentRepositoryQueueIndexer\Domain\Service\DimensionsService;
use Neos\Neos\Controller\CreateContentContextTrait;


/**
 * NodeIndexer for use in batch jobs
 *
 * @Flow\Scope("singleton")
 */
class NodeJobIndexer extends NodeIndexer
{
    use CreateContentContextTrait;

    /**
     * @var JobManager
     * @Flow\Inject
     */
    protected $jobManager;

    /**
     * @var PersistenceManagerInterface
     * @Flow\Inject
     */
    protected $persistenceManager;

    /**
     * @var bool
     * @Flow\InjectConfiguration(path="enableLiveAsyncIndexing")
     */
    protected $enableLiveAsyncIndexing;

    /**
     * @var DimensionsService
     * @Flow\Inject
     */
    protected $dimensionService;

    /**
     * @param NodeInterface $node
     * @param string|null $targetWorkspaceName In case indexing is triggered during publishing, a target workspace name will be passed in
     * @throws \Exception
     */
    public function indexNode(NodeInterface $node, $targetWorkspaceName = null, $indexAllDimensions = true, $indexFallbackDimensions = true): void
    {
        if( $node->isRemoved() ){
            $this->removeNode($node, $targetWorkspaceName);
            return;
        }
        if ($this->enableLiveAsyncIndexing !== true) {
            parent::indexNode($node, $targetWorkspaceName, $indexAllDimensions, $indexFallbackDimensions);

            return;
        }

        if ($this->settings['indexAllWorkspaces'] === false) {
            if ($targetWorkspaceName !== null && $targetWorkspaceName !== 'live') {
                return;
            }

            if ($targetWorkspaceName === null && $node->getContext()->getWorkspaceName() !== 'live') {
                return;
            }
        }

        $indexingJob = new IndexingJob($targetWorkspaceName, $this->nodeAsArray($node));
        $this->jobManager->queue(NodeIndexQueueCommandController::LIVE_QUEUE_NAME, $indexingJob);
    }

    /**
     * @param NodeInterface $node
     * @param string|null $targetWorkspaceName In case indexing is triggered during publishing, a target workspace name will be passed in
     * @throws ContentRepositoryAdaptor\Exception
     * @throws \Exception
     * @throws \Neos\Flow\Persistence\Exception\IllegalObjectTypeException
     * @throws \Neos\Utility\Exception\FilesException
     */
    public function removeNode(NodeInterface $node, string $targetWorkspaceName = null): void
    {
        if ($this->enableLiveAsyncIndexing !== true) {
            parent::removeNode($node, $targetWorkspaceName);

            return;
        }

        if ($this->settings['indexAllWorkspaces'] === false) {
            if ($targetWorkspaceName !== null && $targetWorkspaceName !== 'live') {
                return;
            }

            if ($targetWorkspaceName === null && $node->getContext()->getWorkspaceName() !== 'live') {
                return;
            }
        }

        $dimensionCombinations = $this->dimensionService->getDimensionCombinationsForIndexing($node);
        $targetWorkspaceName = $targetWorkspaceName ?? $node->getWorkspace()->getName();

        if (array_filter($dimensionCombinations) === []) {
            $removalJob = new RemovalJob($targetWorkspaceName, $this->nodeAsArray($node));
            $this->jobManager->queue(NodeIndexQueueCommandController::LIVE_QUEUE_NAME, $removalJob);
        } else {
            foreach ($dimensionCombinations as $combination) {

                $nodeFromContext = $this->createContentContext($targetWorkspaceName, $combination)->getNodeByIdentifier($node->getIdentifier());
                if ($nodeFromContext instanceof NodeInterface && !$nodeFromContext->isRemoved()) {
                    continue;
                }

                $fakeNodeArray = [
                    'persistenceObjectIdentifier' => 'fake',
                    'workspace' => $node->getWorkspace()->getName(),
                    'path' => $node->getPath(),
                    'identifier' => $node->getIdentifier(),
                    'nodeType' => $node->getNodeType()->getName(),
                    'dimensions' => $combination
                ];

                $removalJob = new RemovalJob($targetWorkspaceName, [$fakeNodeArray]);
                $this->jobManager->queue(NodeIndexQueueCommandController::LIVE_QUEUE_NAME, $removalJob);
            }
        }
    }

    /**
     * Returns an array of data from the node for use as job payload.
     *
     * @param NodeInterface $node
     * @return array
     */
    public function nodeAsArray(NodeInterface $node): array
    {
        return [
            [
                'persistenceObjectIdentifier' => $this->persistenceManager->getIdentifierByObject($node->getNodeData()),
                'identifier' => $node->getIdentifier(),
                'dimensions' => $node->getContext()->getDimensions(),
                'workspace' => $node->getWorkspace()->getName(),
                'nodeType' => $node->getNodeType()->getName(),
                'path' => $node->getPath()
            ]
        ];
    }

}
