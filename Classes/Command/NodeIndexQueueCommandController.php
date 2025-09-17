<?php
declare(strict_types=1);

namespace Medienreaktor\Meilisearch\ContentRepositoryQueueIndexer\Command;

/*
 * This file is part of the Medienreaktor.Meilisearch.ContentRepositoryQueueIndexer package.
 *
 * (c) Contributors of the Neos Project - www.neos.io
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Doctrine\Common\Collections\ArrayCollection;

use Medienreaktor\Meilisearch\ContentRepositoryQueueIndexer\Domain\Repository\NodeDataRepository;
use Medienreaktor\Meilisearch\ContentRepositoryQueueIndexer\IndexingJob;

use Medienreaktor\Meilisearch\ContentRepositoryQueueIndexer\Indexer\NodeJobIndexer as NodeIndexer;

use Flowpack\JobQueue\Common\Exception;
use Flowpack\JobQueue\Common\Job\JobManager;
use Flowpack\JobQueue\Common\Queue\QueueManager;
use Neos\ContentRepository\Domain\Repository\WorkspaceRepository;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Cli\CommandController;
use Neos\Flow\Cli\Exception\StopCommandException;
use Neos\Flow\Log\Utility\LogEnvironment;
use Neos\Flow\Mvc\Exception\StopActionException;
use Neos\Flow\Persistence\PersistenceManagerInterface;
use Neos\Utility\Files;
use Neos\ContentRepository\Domain\Model\NodeInterface;
use Medienreaktor\Meilisearch\ContentRepositoryQueueIndexer\Domain\Model\LastChecked;
use Medienreaktor\Meilisearch\ContentRepositoryQueueIndexer\Domain\Repository\LastCheckedRepository;
use Doctrine\DBAL\Connection;
use Neos\ContentRepository\Domain\Service\ContextFactory;
use Medienreaktor\Meilisearch\ContentRepositoryQueueIndexer\Domain\Service\DimensionsService;
use Psr\Log\LoggerInterface;

/**
 * Provides CLI features for index handling
 *
 * @Flow\Scope("singleton")
 */
class NodeIndexQueueCommandController extends CommandController
{
    protected const BATCH_QUEUE_NAME = 'Medienreaktor.Meilisearch.ContentRepositoryQueueIndexer';
    public const LIVE_QUEUE_NAME = 'Medienreaktor.Meilisearch.ContentRepositoryQueueIndexer.Live';

    /**
     * @Flow\Inject
     * @var LoggerInterface
     */
    protected $logger;

    /**
     * @Flow\Inject
     * @var LastCheckedRepository
     */
    protected $lastCheckedResporistory;

    /**
     * @Flow\Inject
     * @var Connection
     */
    protected $databaseConnection;

    /**
     * @Flow\Inject
     * @var ContextFactory
     */
    protected $contextFactory;

    /**
     * @var JobManager
     * @Flow\Inject
     */
    protected $jobManager;

    /**
     * @var QueueManager
     * @Flow\Inject
     */
    protected $queueManager;

    /**
     * @var PersistenceManagerInterface
     * @Flow\Inject
     */
    protected $persistenceManager;

    /**
     * @var NodeDataRepository
     * @Flow\Inject
     */
    protected $nodeDataRepository;

    /**
     * @var WorkspaceRepository
     * @Flow\Inject
     */
    protected $workspaceRepository;

    /**
     * @var DimensionsService
     * @Flow\Inject
     */
    protected $dimensionsService;

    /**
     * @var NodeIndexer
     * @Flow\Inject
     */
    protected $nodeIndexer;

    /**
     * @Flow\InjectConfiguration(path="batchSize")
     * @var int
     */
    protected $batchSize;

    /**
     * @Flow\Inject
     * @var \Neos\ContentRepository\Domain\Service\ContentDimensionCombinator
     */
    protected $contentDimensionCombinator;

    /**
     * Index all nodes by creating a new index and when everything was completed, switch the index alias.
     *
     * @param string $workspace
     * @param string $startNodePath Optional node path to start indexing from (includes child nodes)
     * @param string $dimensionsHash Optional dimension hash to filter by specific language/dimension combination
     * @throws Exception
     * @throws StopCommandException
     * @throws \Exception
     */
    public function buildCommand(string $workspace = 'live', string $startNodePath = null, string $dimensionsHash = null): void
    {

        $this->outputLine();

        $pendingJobs = $this->queueManager->getQueue(self::BATCH_QUEUE_NAME)->countReady();
        if ($pendingJobs !== 0) {
            $this->outputLine('<error>!! </error> The queue "%s" is not empty (%d pending jobs), please flush the queue.', [self::BATCH_QUEUE_NAME, $pendingJobs]);
            $this->quit(1);
        }

        if ($workspace === null) {
            foreach ($this->workspaceRepository->findAll() as $workspace) {
                $workspace = $workspace->getName();
                $this->outputLine();
                $this->indexWorkspace($workspace, $startNodePath, $dimensionsHash);
            }
        } else {
            $this->outputLine();
            $this->indexWorkspace($workspace, $startNodePath, $dimensionsHash);
        }

        $this->outputLine('Indexing jobs created for queue %s with success ...', [self::BATCH_QUEUE_NAME]);
        $this->outputSystemReport();
        $this->outputLine();
    }

    /**
     * @param string $queue Type of queue to process, can be "live" or "batch"
     * @param int $exitAfter If set, this command will exit after the given amount of seconds
     * @param int $limit If set, only the given amount of jobs are processed (successful or not) before the script exits
     * @param bool $verbose Output debugging information
     * @return void
     * @throws StopActionException
     * @throws StopCommandException
     */
    public function workCommand(string $queue = 'batch', int $exitAfter = null, int $limit = null, $verbose = false): void
    {
        $allowedQueues = [
            'batch' => self::BATCH_QUEUE_NAME,
            'live' => self::LIVE_QUEUE_NAME
        ];

        if (!isset($allowedQueues[$queue])) {
            $this->output('Invalid queue, should be "live" or "batch"');
        }

        $queueName = $allowedQueues[$queue];

        if ($verbose) {
            $this->output('Watching queue <b>"%s"</b>', [$queueName]);
            if ($exitAfter !== null) {
                $this->output(' for <b>%d</b> seconds', [$exitAfter]);
            }
            $this->outputLine('...');
        }

        $startTime = time();
        $timeout = null;
        $numberOfJobExecutions = 0;

        do {
            $message = null;
            if ($exitAfter !== null) {
                $timeout = max(1, $exitAfter - (time() - $startTime));
            }


            try {
                $message = $this->jobManager->waitAndExecute($queueName, $timeout);
            } catch (\Exception $exception) {
                $numberOfJobExecutions++;

                $verbose && $this->outputLine('<error>%s</error>', [$exception->getMessage()]);

                if ($exception->getPrevious() instanceof \Exception) {
                    $verbose && $this->outputLine('  Reason: %s', [$exception->getPrevious()->getMessage()]);
                    $this->logger->error(sprintf('Indexing job failed: %s. Detailed reason %s', $exception->getMessage(), $exception->getPrevious()->getMessage()), LogEnvironment::fromMethodName(__METHOD__));
                } else {
                    $this->logger->error('Indexing job failed: ' . $exception->getMessage(), LogEnvironment::fromMethodName(__METHOD__));
                }
            }

            if ($message !== null) {
                $numberOfJobExecutions++;
                if ($verbose) {
                    $messagePayload = strlen($message->getPayload()) <= 50 ? $message->getPayload() : substr($message->getPayload(), 0, 50) . '...';
                    $this->outputLine('<success>Successfully executed job "%s" (%s)</success>', [$message->getIdentifier(), $messagePayload]);
                }
            }

            if ($exitAfter !== null && (time() - $startTime) >= $exitAfter) {
                if ($verbose) {
                    $this->outputLine('Quitting after %d seconds due to <i>--exit-after</i> flag', [time() - $startTime]);
                }
                $this->quit();
            }

            if ($limit !== null && $numberOfJobExecutions >= $limit) {
                if ($verbose) {
                    $this->outputLine('Quitting after %d executed job%s due to <i>--limit</i> flag', [$numberOfJobExecutions, $numberOfJobExecutions > 1 ? 's' : '']);
                }
                $this->quit();
            }

        } while (true);
    }

    /**
     * Flush the index queue
     */
    public function flushCommand(): void
    {
        try {
            $this->queueManager->getQueue(self::BATCH_QUEUE_NAME)->flush();
            $this->outputSystemReport();
        } catch (Exception $exception) {
            $this->outputLine('An error occurred: %s', [$exception->getMessage()]);
        }
        $this->outputLine();
    }

    /**
     * Output system report for CLI commands
     */
    protected function outputSystemReport(): void
    {
        $this->outputLine();
        $this->outputLine('Memory Usage   : %s', [Files::bytesToSizeString(memory_get_peak_usage(true))]);
        $time = microtime(true) - $_SERVER["REQUEST_TIME_FLOAT"];
        $this->outputLine('Execution time : %s seconds', [$time]);
        $this->outputLine('Indexing Queue : %s', [self::BATCH_QUEUE_NAME]);
        try {
            $queue = $this->queueManager->getQueue(self::BATCH_QUEUE_NAME);
            $this->outputLine('Pending Jobs   : %s', [$queue->countReady()]);
            $this->outputLine('Reserved Jobs  : %s', [$queue->countReserved()]);
            $this->outputLine('Failed Jobs    : %s', [$queue->countFailed()]);
        } catch (Exception $exception) {
            $this->outputLine('Pending Jobs   : Error, queue %s not found, %s', [self::BATCH_QUEUE_NAME, $exception->getMessage()]);
        }
    }

    /**
     * @param string $workspaceName
     * @param string $startNodePath Optional node path to start indexing from
     * @param string $dimensionHash Optional dimension hash to filter by
     * @throws \Exception
     */
    protected function indexWorkspace(string $workspaceName, string $startNodePath = null, string $dimensionHash = null): void
    {
        $filterInfo = [];
        if ($startNodePath !== null) {
            $filterInfo[] = sprintf('starting from path "%s"', $startNodePath);
        }
        if ($dimensionHash !== null) {
            $filterInfo[] = sprintf('dimension hash "%s"', $dimensionHash);
        }

        $filterText = empty($filterInfo) ? '' : ' (' . implode(', ', $filterInfo) . ')';
        $this->outputLine('<info>++</info> Indexing %s workspace%s', [$workspaceName, $filterText]);

        $nodeCounter = 0;
        $offset = 0;
        $lastPersistenceObjectIdentifier = null;

        while (true) {
            $iterator = $this->nodeDataRepository->findAllBySiteAndWorkspace(
                $workspaceName,
                $lastPersistenceObjectIdentifier,
                $this->batchSize,
                $startNodePath,
                $dimensionHash
            );

            $jobData = [];

            foreach ($this->nodeDataRepository->iterate($iterator) as $data) {
                $lastPersistenceObjectIdentifier = $data['persistenceObjectIdentifier'];

                $jobData[] = [
                    'persistenceObjectIdentifier' => $data['persistenceObjectIdentifier'],
                    'identifier' => $data['identifier'],
                    'dimensions' => $data['dimensions'],
                    'workspace' => $workspaceName,
                    'nodeType' => $data['nodeType'],
                    'path' => $data['path'],
                ];
                $nodeCounter++;
            }

            if ($jobData === []) {
                break;
            }

            $indexingJob = new IndexingJob($workspaceName, $jobData);
            $this->jobManager->queue(self::BATCH_QUEUE_NAME, $indexingJob);
            $this->output('.');
            $offset += $this->batchSize;
            $this->persistenceManager->clearState();
        }
        $this->outputLine();
        $this->outputLine("\nNumber of Nodes to be indexed in workspace '%s': %d", [$workspaceName, $nodeCounter]);
        $this->outputLine();
    }

    /**
     * Übersetzt Nodes, die seit dem letzten Datum geändert wurden.
     */
    public function indexChangedNodesCommand(string $workspace = 'live', int $exitAfter = null): void
    {
        $startTime = time();
        // Datum aus dem Modell abrufen
        $lastChecked = $this->getLastCheckedDate();

        if (!$lastChecked) {
            $this->outputLine('Keine vorherigen Prüfungen gefunden. Verarbeite alle Nodes.');
        } else {
            $this->outputLine(sprintf('Prüfe Nodes, die seit %s geändert wurden.', $lastChecked->format('Y-m-d H:i:s')));
        }

        $currentDateTime = new \DateTime('now', new \DateTimeZone('Europe/Berlin'));

        // Beispiel: Nodes abrufen, die seit dem Datum geändert wurden
        $changedNodes = $this->getChangedNodesSince($workspace, $lastChecked);

        if ($changedNodes === null) {
            $this->outputLine('Fehler beim Abrufen der geänderten Nodes.');
            $this->quit(1);
        }

        $this->outputLine(sprintf('Es wurden %d geänderte Nodes gefunden.', count($changedNodes)));

        $alreadyAddedNodes = [];

        // Index found nodes
        foreach ($changedNodes as $key => $node) {

            $fullTextRoot = $this->nodeIndexer->findFulltextRoot($node);

            if ($fullTextRoot === null) {
                $this->outputLine('Fehler beim Abrufen des Fulltext-Node.');
                continue;
            }

            if (in_array($fullTextRoot->getIdentifier().'_'.$this->dimensionsService->hashByNode($fullTextRoot), $alreadyAddedNodes, true)) {
                $this->outputLine('Node %s mit Dimension Hash %s wurde bereits zum Index hinzugefügt.', [$fullTextRoot->getIdentifier(), $this->dimensionsService->hashByNode($fullTextRoot)]);
                // Node alreaedy added
                continue;
            }

            $indexingJob = new IndexingJob($workspace, $this->nodeIndexer->nodeAsArray($fullTextRoot));
            $this->jobManager->queue(self::LIVE_QUEUE_NAME, $indexingJob);
            $alreadyAddedNodes[] = $fullTextRoot->getIdentifier().'_'.$this->dimensionsService->hashByNode($fullTextRoot);
            $this->persistenceManager->persistAll();

            // Check if the next node has a higher lastModificationDateTime
            $nextNode = $changedNodes[$key + 1] ?? null;
            if ($nextNode) {
                if ($nextNode->getLastModificationDateTime() > $node->getLastModificationDateTime()) {
                    $this->updateLastCheckedDate($node->getLastModificationDateTime());
                    $this->outputLine('Alle Nodes bis zum Datum %s wurden verarbeitet. Das Datum wird aktualisiert.', [$node->getLastModificationDateTime()->format('Y-m-d H:i:s')]);
                }
            }
            if ($exitAfter !== null && (time() - $startTime) >= $exitAfter) {
                $this->outputLine('Quitting after %d seconds due to <i>--exit-after</i> flag', [time() - $startTime]);
                $this->quit();
            }
        }

        // Aktualisiere das Tracking-Datum im Modell
        $this->updateLastCheckedDate($currentDateTime);

        $this->outputLine('Indexing Jobs generiert.');
    }

    public function getChangedNodesSince(string $workspace = 'live', ?\DateTime $lastChecked = null): ?array
    {
        // Pseudo-Code für das Abrufen von Nodes, die seit `lastChecked` geändert wurden
        if ($lastChecked === null) {
            // Hole alle Nodes, wenn kein Datum gesetzt ist
            $this->outputLine('Keine vorherigen Prüfungen gefunden. Erstes Cheked-Datum wird initialisiert.');
            $this->updateLastCheckedDate(new \DateTime());
            return null;
        }

       // Verbindung zur Datenbank herstellen
        $connection = $this->databaseConnection;

        // Basis-Query erstellen
        $query = $connection->createQueryBuilder()
            ->select('n.*') // Wähle alle Spalten oder spezifische, die benötigt werden
            ->from('neos_contentrepository_domain_model_nodedata', 'n')
            ->where('n.lastmodificationdatetime IS NOT NULL');

        $query->andWhere('n.workspace = :workspace')->setParameter(':workspace', $workspace);
        $query->orderBy('n.lastmodificationdatetime', 'ASC');
        // Falls ein Datum übergeben wurde, füge die Bedingung hinzu
        if ($lastChecked !== null) {
            $query->andWhere('n.lastmodificationdatetime > :lastChecked')
                ->setParameter(':lastChecked', $lastChecked->format('Y-m-d H:i:s'));
        }

        // Query ausführen
        $result = $query->execute()->fetchAllAssociative();

        // Nodes zurückgeben (Mapping von DB-Daten zu Node-Objekten)
        $nodes = [];
        foreach ($result as $row) {
            $node = $this->mapDatabaseRowToNode($row);
            if ($node !== null) {
                $nodes[] = $node;
            }
        }

        return $nodes;

    }

    protected function mapDatabaseRowToNode(array $row): ?NodeInterface
    {
        try {
            $combinatons = ($this->contentDimensionCombinator->getAllAllowedCombinations());
            $dimensionvalues = json_decode($row['dimensionvalues'], true);
            $combinations = array_filter($combinatons, function($combination) use ($dimensionvalues){
                return $combination["language"][0] == $dimensionvalues['language'][0];
            });
            $dimensions = array_values($combinations)[0];
            $context = $this->contextFactory->create([
                'workspaceName' => $row['workspace'],
                'dimensions' => $dimensions,
                'targetDimensions' => array(
                    'language' => $dimensions['language'][0],
                ),
                'invisibleContentShown' => true,
                'removedContentShown' => true,
                'inaccessibleContentShown' => true
            ]);

            return $context->getNodeByIdentifier($row['identifier']);
        } catch (\Exception $e) {
            // Fehler beim Mapping behandeln
            $this->logger->error(sprintf(
                'Fehler beim Mapping der Node-Daten für ID "%s": %s',
                $row['identifier'],
                $e->getMessage()
            ));
            return null;
        }
    }

    public function getLastCheckedDate(): ?\DateTime
    {
        $lastCheckedEntry = $this->lastCheckedResporistory->findFirst();
        return $lastCheckedEntry ? $lastCheckedEntry->getLastChecked() : null;
    }

    protected function updateLastCheckedDate(\DateTime $dateTime): void
    {
        $lastCheckedEntry = $this->lastCheckedResporistory->findFirst();
        if (!$lastCheckedEntry) {
            $lastCheckedEntry = new LastChecked();
            $lastCheckedEntry->setLastChecked($dateTime);
            $this->persistenceManager->add($lastCheckedEntry);
        } else {
            $lastCheckedEntry->setLastChecked($dateTime);
            $this->persistenceManager->update($lastCheckedEntry);
        }
        $this->persistenceManager->persistAll();
    }
}
