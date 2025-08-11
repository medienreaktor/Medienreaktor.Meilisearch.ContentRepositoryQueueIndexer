# Neos CMS Meilisearch indexer based on a job queue

This package can be used to index a huge amount of nodes in Meilisearch indexes. This
package use the Flowpack JobQueue packages to handle the indexing asynchronously.

**Topics**

* [Installation](#installation-and-configuration)
* [Indexing](#indexing)
* [SupervisorD configuration](#supervisord-configuration)
* [Update Instructions](#update-instructions)


# Installation and Configuration

You need to install the correct Queue package based on your needs.

Available packages:

  - [sqlite](https://packagist.org/packages/flownative/jobqueue-sqlite)
  - [beanstalkd](https://packagist.org/packages/flowpack/jobqueue-beanstalkd)
  - [doctrine](https://packagist.org/packages/flowpack/jobqueue-doctrine)
  - [redis](https://packagist.org/packages/flowpack/jobqueue-redis)

Please check the package documentation for specific configurations.

The default configuration uses the FakeQueue, which is provided by the JobQueue.Common package. Note that with that package jobs are executed synchronous with the `flow nodeindexqueue:build` command.

Check the ```Settings.yaml``` to adapt based on the Queue package, you need to adapt the ```className```:

    Flowpack:
      JobQueue:
        Common:
          presets:
            'Medienreaktor.Meilisearch.ContentRepositoryQueueIndexer':
              className: 'Flowpack\JobQueue\Common\Queue\FakeQueue'

If you use the [doctrine](https://packagist.org/packages/flownative/jobqueue-doctrine) package you have to set the ```tableName``` manually:

    Flowpack:
      JobQueue:
        Common:
          presets:
            'Medienreaktor.Meilisearch.ContentRepositoryQueueIndexer':
              className: 'Flowpack\JobQueue\Doctrine\Queue\DoctrineQueue'
          queues:
            'Medienreaktor.Meilisearch.ContentRepositoryQueueIndexer':
              options:
                tableName: 'flowpack_jobqueue_QueueIndexer'
            'Medienreaktor.Meilisearch.ContentRepositoryQueueIndexer.Live':
              options:
                tableName: 'flowpack_jobqueue_QueueIndexerLive'

# Indexing

## Batch Indexing

### How to build indexing jobs

    flow nodeindexqueue:build --workspace live

Optional parameters:

--start-node-path="/sites/yoursitename"
Limit job creation to the given node path (the subtree starting at that node will be indexed).

--dimensions-hash="e781f29c8dd927c09735547a848e3459"
Restrict indexing to a single DimensionSpacePoint (e.g. a specific language / market combination). The hash must match an existing configured dimension combination.

Combined example:

flow nodeindexqueue:build --workspace live --start-node-path="/sites/yoursitename" --dimensions-hash="e781f29c8dd927c09735547a848e3459"


You can use this CLI command to process indexing job:

    flow nodeindexqueue:work --queue batch

## Live Indexing

You can disable async live indexing by editing ```Settings.yaml```:

    Medienreaktor:
      Meilisearch:
        ContentRepositoryQueueIndexer:
          enableLiveAsyncIndexing: false

You can use this CLI command to process indexing job:

    flow nodeindexqueue:work --queue live

License
-------

Licensed under MIT, see [LICENSE](LICENSE)
