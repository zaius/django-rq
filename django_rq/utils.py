from rq.registry import (DeferredJobRegistry, FailedJobRegistry,
                         FinishedJobRegistry, StartedJobRegistry, clean_registries)
from rq.worker import Worker
from rq.worker_registration import clean_worker_registry

from .queues import get_connection, get_queue_by_index
from .settings import QUEUES_LIST
from .templatetags.django_rq import to_localtime
from .workers import get_worker_class


def get_statistics(run_maintenance_tasks=False):
    queues = []
    workers = []
    worker_class = get_worker_class()
    for index, config in enumerate(QUEUES_LIST):

        queue = get_queue_by_index(index)
        connection = queue.connection
        connection_kwargs = connection.connection_pool.connection_kwargs

        if run_maintenance_tasks:
            clean_registries(queue)
            clean_worker_registry(queue)

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

        # parse_class and connection_pool are not needed and not JSON serializable
        connection_kwargs.pop('parser_class', None)
        connection_kwargs.pop('connection_pool', None)

        queue_data = {
            'name': queue.name,
            'jobs': queue.count,
            'oldest_job_timestamp': oldest_job_timestamp,
            'index': index,
            'connection_kwargs': connection_kwargs
        }

        connection = get_connection(queue.name)
        queue_data['workers'] = Worker.count(queue=queue)

        finished_job_registry = FinishedJobRegistry(queue.name, connection)
        started_job_registry = StartedJobRegistry(queue.name, connection)
        deferred_job_registry = DeferredJobRegistry(queue.name, connection)
        failed_job_registry = FailedJobRegistry(queue.name, connection)
        queue_data['finished_jobs'] = len(finished_job_registry)
        queue_data['started_jobs'] = len(started_job_registry)
        queue_data['deferred_jobs'] = len(deferred_job_registry)
        queue_data['failed_jobs'] = len(failed_job_registry)

        workers += worker_class.all(connection=connection)

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
