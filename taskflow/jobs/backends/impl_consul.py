# -*- coding: utf-8 -*-

#    Copyright (C) 2015 Yahoo! Inc. All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import contextlib
import datetime
import functools
from http.client import CONFLICT
from urllib.error import HTTPError

import six
import time
import consul_kv

import msgpack
from os.path import join
from oslo_serialization import msgpackutils
from oslo_utils import excutils
from oslo_utils import timeutils
from oslo_utils import uuidutils

from taskflow import exceptions as exc
from taskflow.jobs import base
from taskflow import logging
from taskflow import states
from taskflow.utils import misc
from taskflow.utils import consul_utils as cu


LOG = logging.getLogger(__name__)

#: Key **prefix** that job entries have.
JOB_PREFIX = 'job'

#: Postfix that lock entries have.
LOCK_POSTFIX = ".lock"



@contextlib.contextmanager
def _translate_failures():
    """Translates common consul exceptions into taskflow exceptions."""
    yield


@functools.total_ordering
class ConsulJob(base.Job):
    """A consul job."""

    def __init__(self, board, name, key, path,
                 uuid=None, details=None,
                 created_on=None, backend=None,
                 book=None, book_data=None,
                 priority=base.JobPriority.NORMAL):
        super(ConsulJob, self).__init__(board, name,
                                       uuid=uuid, details=details,
                                       backend=backend,
                                       book=book, book_data=book_data)
        self._created_on = created_on
        self._path = path
        self._client = board._client
        self._consul_version = board._consul_version
        self._key = key
        self._priority = priority

    @property
    def key(self):
        """Key (in board listings/trash hash) the job data is stored under."""
        return self._key

    @property
    def priority(self):
        return self._priority

    @property
    def path(self):
        """Path the job key value data is stored."""
        return self._path

    @property
    def last_modified_key(self):
        """Key the job last modified data is stored under."""
        return self._last_modified_key

    @property
    def owner_key(self):
        """Key the job claim + data of the owner is stored under."""
        return self._owner_key

    @property
    def sequence(self):
        """Sequence number of the current job."""
        return self._sequence

    def expires_in(self):
        """How many seconds until the claim expires.

        Returns the number of seconds until the ownership entry expires or
        :attr:`~taskflow.utils.consul_utils.UnknownExpire.DOES_NOT_EXPIRE` or
        :attr:`~taskflow.utils.consul_utils.UnknownExpire.KEY_NOT_FOUND` if it
        does not expire or if the expiry can not be determined (perhaps the
        :attr:`.owner_key` expired at/before time of inquiry?).
        """
        with _translate_failures():
            return cu.get_expiry(self._client, self._owner_key,
                                 prior_version=self._consul_version)

    def extend_expiry(self, expiry):
        """Extends the owner key (aka the claim) expiry for this job.

        NOTE(harlowja): if the claim for this job did **not** previously
        have an expiry associated with it, calling this method will create
        one (and after that time elapses the claim on this job will cease
        to exist).

        Returns ``True`` if the expiry request was performed
        otherwise ``False``.
        """
        with _translate_failures():
            return cu.apply_expiry(self._client, self._owner_key, expiry,
                                   prior_version=self._consul_version)

    def __lt__(self, other):
        if not isinstance(other, ConsulJob):
            return NotImplemented
        else:
            ordered = base.JobPriority.reorder(
                (self.priority, self), (other.priority, other))
            if ordered[0] is self:
                return False
            return True

    def __eq__(self, other):
        if not isinstance(other, ConsulJob):
            return NotImplemented
        return ((self.board.listings_key, self.priority, self.sequence) ==
                (other.board.listings_key, other.priority, other.sequence))

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash((self.board.listings_key, self.priority, self.sequence))

    @property
    def created_on(self):
        return self._created_on

    @property
    def last_modified(self):
        with _translate_failures():
            raw_last_modified = self._client.get(self._last_modified_key)
        last_modified = None
        if raw_last_modified:
            last_modified = self._board._loads(
                raw_last_modified, root_types=(datetime.datetime,))
            # NOTE(harlowja): just incase this is somehow busted (due to time
            # sync issues/other), give back the most recent one (since consul
            # does not maintain clock information; we could have this happen
            # due to now clients who mutate jobs also send the time in).
            last_modified = max(last_modified, self._created_on)
        return last_modified

    @property
    def state(self):
        with _translate_failures():
            try:
                job_data = self._client.get_dict(self.path)
                try:
                    lock_data = self._client.get_dict(join(self.path, LOCK_POSTFIX))
                    return states.CLAIMED
                except:
                    return states.UNCLAIMED
            except:
                return states.COMPLETE


class ConsulJobBoard(base.JobBoard):
    """A jobboard backed by `consul`_.

    Powered by the `consul-kv <github.com/vdloo/consul-kv/>`_ library.

    This jobboard creates job entries by listing jobs from a dict that is
    translated to consul kv items. This dict contains jobs that can be actively
    worked on by (and examined/claimed by) some set of eligible consumers.
    Job posting is typically performed using the :meth:`.post` method (this
    creates a dict entry with job contents/details encoded in `msgpack`_).
    The users of these jobboard(s) (potentially on disjoint sets of machines)
    can then iterate over the available jobs and decide if they want to attempt
    to claim one of the jobs they have iterated over. If so they will then
    attempt to contact consul and they will attempt to create a key in
    the key value store to claim a desired job. If the entity trying to use
    the jobboard to :meth:`.claim` the job is able to create that lock/owner
    key then it will be allowed (and expected) to perform whatever *work* the
    contents of that job described. Once the claiming entity is finished the
    lock/owner key and the `hash`_ entry will be deleted (if successfully
    completed) in a single request using TXN. If the claiming entity is not
    successful (or the entity that claimed the job dies) the lock/owner key
    can be released automatically (by usage of a claim expiry) or by using
    :meth:`.abandon` to manually abandon the job so that it can be consumed/worked
    on by others.

    .. _msgpack: http://msgpack.org/
    .. _consul: http://consul.io/
    """

    CLIENT_CONF_TRANSFERS = tuple([
        # Host config...
        ('endpoint', str),
        ('timeout', int)
    ])
    """
    Keys (and value type converters) that we allow to proxy from the jobboard
    configuration into the consul-kv client (used to configure the consul client
    internals if no explicit client is provided via the ``client`` keyword
    argument).

    See: https://github.com/vdloo/consul-kv/blob/master/consul_kv/__init__.py#L22
    """

    #: Postfix (combined with job key) used to make a jobs owner key.
    OWNED_POSTFIX = ".owned"

    #: Postfix (combined with job key) used to make a jobs last modified key.
    LAST_MODIFIED_POSTFIX = ".last_modified"

    #: Default namespace for keys when none is provided.
    DEFAULT_NAMESPACE = 'taskflow'

    #: Transaction support was added in 0.7.0 so we need at least that version.
    MIN_CONSUL_VERSION = (0, 7, 0)

    @classmethod
    def _make_client(cls, conf):
        client_conf = {}
        for key, value_type_converter in cls.CLIENT_CONF_TRANSFERS:
            if key in conf:
                if value_type_converter is not None:
                    client_conf[key] = value_type_converter(conf[key])
                else:
                    client_conf[key] = conf[key]
        return consul_kv.Connection(**client_conf)

    def __init__(self, name, conf,
                 client=None, persistence=None):
        super(ConsulJobBoard, self).__init__(name, conf)
        self._closed = True
        if client is not None:
            self._client = client
            self._owns_client = False
        else:
            self._client = self._make_client(self._conf)
            self._owns_client = True
        self._namespace = self._conf.get('namespace', self.DEFAULT_NAMESPACE)
        # Consul server version connected to
        self._consul_version = None
        # The backend to load the full logbooks from, since what is sent over
        # the data connection is only the logbook uuid and name, and not the
        # full logbook.
        self._persistence = persistence
        # Misc. internal details
        self._known_jobs = {}

    @property
    def namespace(self):
        """The namespace all keys will be prefixed with (or none)."""
        return self._namespace

    @property
    def job_count(self):
        return len(self._known_jobs)

    @property
    def connected(self):
        return not self._closed

    def connect(self):
        self.close()
        if self._owns_client:
            self._client = self._make_client(self._conf)
        with _translate_failures():
            # The client connects to a HTTP API, so do a GET and
            # if that works then assume the connection works, which may or
            # may not be continuously maintained (if the server dies
            # at a later time, we will become aware of that when the next
            # op occurs).
            self._client.get_meta('agent/self')
            is_new_enough, consul_version = cu.is_server_new_enough(
                self._client, self.MIN_CONSUL_VERSION)
            if not is_new_enough:
                wanted_version = ".".join([str(p)
                                           for p in self.MIN_CONSUL_VERSION])
                if consul_version:
                    raise exc.JobFailure("Consul version %s or greater is"
                                         " required (version %s is to"
                                         " old)" % (wanted_version,
                                                    consul_version))
                else:
                    raise exc.JobFailure("Consul version %s or greater is"
                                         " required" % wanted_version)
            else:
                self._consul_version = consul_version
                self._closed = False

    def close(self):
        self._consul_version = None
        self._closed = True

    @staticmethod
    def _dumps(obj):
        try:
            return msgpackutils.dumps(obj)
        except (msgpack.PackException, ValueError):
            # TODO(harlowja): remove direct msgpack exception access when
            # oslo.utils provides easy access to the underlying msgpack
            # pack/unpack exceptions..
            exc.raise_with_cause(exc.JobFailure,
                                 "Failed to serialize object to"
                                 " msgpack blob")

    @staticmethod
    def _loads(blob, root_types=(dict,)):
        try:
            return misc.decode_msgpack(blob, root_types=root_types)
        except (msgpack.UnpackException, ValueError):
            # TODO(harlowja): remove direct msgpack exception access when
            # oslo.utils provides easy access to the underlying msgpack
            # pack/unpack exceptions..
            exc.raise_with_cause(exc.JobFailure,
                                 "Failed to deserialize object from"
                                 " msgpack blob (of length %s)" % len(blob))

    def find_owner(self, job):
        owner_key = self.join(job.key + self.OWNED_POSTFIX)
        with _translate_failures():
            raw_owner = self._client.get(owner_key)
            return raw_owner

    def _put_key_value(self, key, value):
        """
        Raises HTTPError if any of the keys already exist
        """
        self._client.put_dict(
            {self.DEFAULT_NAMESPACE: {key: value}},
            verb='cas'
        )

    def _delete_key(self, key):
        self._client.delete(
            join(self.DEFAULT_NAMESPACE, key),
            recurse=True
        )

    def post(self, name, book=None, details=None,
             priority=base.JobPriority.NORMAL):
        # NOTE(vdloo): Jobs are not ephemeral, they will persist until they
        # are consumed (this may change later, but seems safer to do this until
        # further notice).
        job_priority = base.JobPriority.convert(priority)
        job_uuid = uuidutils.generate_uuid()
        posting = base.format_posting(job_uuid, name,
                                      created_on=timeutils.utcnow(),
                                      book=book, details=details,
                                      priority=job_priority)

        path = join(JOB_PREFIX, job_uuid)
        with _translate_failures():
            self._put_key_value(path, posting)

            return ConsulJob(self, name, job_uuid, path=path,
                             uuid=job_uuid, details=details,
                             created_on=posting['created_on'],
                             book=book, book_data=posting.get('book'),
                             backend=self._persistence,
                             priority=job_priority)

    def wait(self, timeout=None, initial_delay=0.005,
             max_delay=1.0, sleep_func=time.sleep):
        if initial_delay > max_delay:
            raise ValueError("Initial delay %s must be less than or equal"
                             " to the provided max delay %s"
                             % (initial_delay, max_delay))
        # This does a spin-loop that backs off by doubling the delay
        # up to the provided max-delay.
        w = timeutils.StopWatch(duration=timeout)
        w.start()
        delay = initial_delay
        while True:
            jc = self.job_count
            if jc > 0:
                curr_jobs = self._fetch_jobs()
                if curr_jobs:
                    return base.JobBoardIterator(
                        self, LOG,
                        board_fetch_func=lambda ensure_fresh: curr_jobs)
            if w.expired():
                raise exc.NotFound("Expired waiting for jobs to"
                                   " arrive; waited %s seconds"
                                   % w.elapsed())
            else:
                remaining = w.leftover(return_none=True)
                if remaining is not None:
                    delay = min(delay * 2, remaining, max_delay)
                else:
                    delay = min(delay * 2, max_delay)
                sleep_func(delay)

    def _fetch_jobs(self):
        with _translate_failures():
            base_path = "{}/{}".format(self.DEFAULT_NAMESPACE, JOB_PREFIX)
            serialized_postings = self._client.get_dict(
                base_path
            )[self.DEFAULT_NAMESPACE][JOB_PREFIX]
        postings = []
        for job_key, serialized_posting in six.iteritems(serialized_postings):
            try:
                try:
                    job_priority = serialized_posting['priority']
                    job_priority = base.JobPriority.convert(job_priority)
                except KeyError:
                    job_priority = base.JobPriority.NORMAL
                job_created_on = serialized_posting['created_on']
                job_uuid = serialized_posting['uuid']
                job_name = serialized_posting['name']
                job_details = serialized_posting.get('details', {})
            except (ValueError, TypeError, KeyError):
                with excutils.save_and_reraise_exception():
                    LOG.warning("Incorrectly formatted job data found at"
                                " key: %s[%s]", JOB_PREFIX,
                                job_key, exc_info=True)
            else:
                postings.append(ConsulJob(self, job_name,
                                          job_key, path=join(base_path, job_key),
                                          uuid=job_uuid,
                                          details=job_details,
                                          created_on=job_created_on,
                                          book_data=serialized_posting.get('book'),
                                          backend=self._persistence,
                                          priority=job_priority))
        return sorted(postings, reverse=True)

    def iterjobs(self, only_unclaimed=False, ensure_fresh=False):
        return base.JobBoardIterator(
            self, LOG, only_unclaimed=only_unclaimed,
            ensure_fresh=ensure_fresh,
            board_fetch_func=lambda ensure_fresh: self._fetch_jobs())

    def register_entity(self, entity):
        # Not implemented
        pass

    @base.check_who
    def consume(self, job, who):
        try:
            with _translate_failures():
                self._delete_key(join(job.path + LOCK_POSTFIX))
                self._delete_key(join(job.path))
        except:
            raise exc.JobFailure("Failure to consume job %s,"
                                 " unknown internal error (reason=%s)"
                                 % (job.uuid, 'uncaught'))

    @base.check_who
    def claim(self, job, who, expiry=None):
        if expiry is None:
            ms_expiry = None
        else:
            ms_expiry = int(expiry * 1000.0)
            if ms_expiry <= 0:
                raise ValueError("Provided expiry (when converted to"
                                 " milliseconds) must be greater"
                                 " than zero instead of %s" % (expiry))
        value = {'owner': who, 'pexpire': ms_expiry}
        try:
            with _translate_failures():
                self._put_key_value(join(job.path + LOCK_POSTFIX), value)
        except HTTPError as e:
            if e.code == CONFLICT:
                raise exc.UnclaimableJob("Job %s already"
                                         " claimed" % (job.uuid))
            else:
                raise

    @base.check_who
    def abandon(self, job, who):
        try:
            with _translate_failures():
                self._delete_key(join(job.path + LOCK_POSTFIX))
                self._delete_key(join(job.path))
        except:
            raise exc.JobFailure("Failure to abandon job %s,"
                                 " unknown internal"
                                 " error (status=%s, reason=%s)"
                                 % (job.uuid, 'unknown', 'uncaught'))

    def _get_script(self, name):
        try:
            return self._scripts[name]
        except KeyError:
            exc.raise_with_cause(exc.NotFound,
                                 "Can not access %s script (has this"
                                 " board been connected?)" % name)

    @base.check_who
    def trash(self, job, who):
        try:
            with _translate_failures():
                self._delete_key(join(job.path))
        except:
            raise exc.JobFailure("Failure to trash job %s,"
                                 " unknown internal error (reason=%s)"
                                 % (job.uuid, 'unknown'))
