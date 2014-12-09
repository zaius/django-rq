from rq.registry import (DeferredJobRegistry, FinishedJobRegistry,
                         StartedJobRegistry)

from .queues import get_connection, get_queue_by_index
from .settings import QUEUES_LIST
from .templatetags.django_rq import to_localtime
from .workers import collect_workers_by_connection, get_all_workers_by_configuration


def get_statistics():
    queues = []
    workers = []
    workers_collections = collect_workers_by_connection(QUEUES_LIST)
    for index, config in enumerate(QUEUES_LIST):

        queue = get_queue_by_index(index)
        connection = queue.connection
        connection_kwargs = connection.connection_pool.connection_kwargs

        # Raw access to the first item from left of the redis list.
        # This might not be accurate since new job can be added from the left
        # with `at_front` parameters.
        # Ideally rq should supports Queue.oldest_job
        last_job_id = connection.lindex(queue.key, 0)
        last_job = queue.fetch_job(last_job_id.decode('utf-8')) if last_job_id else None
        if last_job:
            oldest_job_timestamp = to_localtime(last_job.enqueued_at)\
                .strftime('%Y-%m-%d, %H:%M:%S')
        else:
            oldest_job_timestamp = "-"

        # parse_class is not needed and not JSON serializable
        try:
            del(connection_kwargs['parser_class'])
        except KeyError:
            pass

        queue_data = {
            'name': queue.name,
            'jobs': queue.count,
            'oldest_job_timestamp': oldest_job_timestamp,
            'index': index,
            'connection_kwargs': connection_kwargs
        }

        if queue.name == 'failed':
            queue_data['workers'] = '-'
            queue_data['finished_jobs'] = '-'
            queue_data['started_jobs'] = '-'
            queue_data['deferred_jobs'] = '-'

        else:
            connection = get_connection(queue.name)
            all_workers = get_all_workers_by_configuration(
                config['connection_config'],
                workers_collections
            )

            seen_workers = [w.name for w in workers]
            workers += [w for w in all_workers if w.name not in seen_workers]

            queue_workers = [worker for worker in all_workers if queue in worker.queues]
            queue_data['workers'] = len(queue_workers)

            finished_job_registry = FinishedJobRegistry(queue.name, connection)
            started_job_registry = StartedJobRegistry(queue.name, connection)
            deferred_job_registry = DeferredJobRegistry(queue.name, connection)
            queue_data['finished_jobs'] = len(finished_job_registry)
            queue_data['started_jobs'] = len(started_job_registry)
            queue_data['deferred_jobs'] = len(deferred_job_registry)

        queues.append(queue_data)

    # TODO: Right now the scheduler can run on multiple queues, but multiple
    # queues can use the same connection. Either need to dedupe connections or
    # split scheduled into its own queue, like failed.
    #
    # TODO: the real solution here is ditch allowing queues to have separate
    # connections - make a single global connection and multiple queues are
    # only separated by name. This will solve the multiple failed queue issue
    # too. But was there a reason to allow multiple connections? Also, this
    # will require some massive doc updates.
    scheduled_jobs = []
    scheduler_running = False
    scheduler_installed = False
    try:
        from rq_scheduler import Scheduler
        scheduler_installed = True
    except ImportError:
        pass
    else:
        connection = get_connection('default')
        scheduler = Scheduler(connection=connection)
        # get_jobs with_times returns a list of tuples: (job, datetime)

        # TODO: job.origin is the scheduler queue originally used to schedule
        # the job. Need to check if this is how the scheduler actually picks
        # which queue to put the job into.
        for job in scheduler.get_jobs(with_times=True):
            scheduled_jobs.append({
                'job': job[0],
                'runtime': job[1],
                'queue': job[0].origin,
            })

        # TODO: should expose this from rq-scheduler.
        # TODO: this is really per-queue.
        scheduler_running = connection.exists(scheduler.scheduler_key) and \
            not connection.hexists(scheduler.scheduler_key, 'death')

    return {
        'queues': queues,
        'workers': list(set(workers)),
        'scheduler_installed': scheduler_installed,
        'scheduler_running': 'running' if scheduler_running else 'stopped',
        'scheduled_jobs': scheduled_jobs,
    }
