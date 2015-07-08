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
import logging
import os
import sys
import time

logging.basicConfig(level=logging.ERROR)

top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir,
                                       os.pardir))
sys.path.insert(0, top_dir)

from taskflow.conductors import backends as conductor_backends
from taskflow import engines
from taskflow.jobs import backends as job_backends
from taskflow.patterns import linear_flow as lf
from taskflow.persistence import backends as persistence_backends
from taskflow.persistence import logbook
from taskflow import task
from taskflow.types import timing

from oslo_utils import uuidutils

# Instructions!
#
# 1. Install zookeeper (or change host listed below)
# 2. Download this example, place in file '99_bottles.py'
# 3. Run `python 99_bottles.py p` to place a song request onto the jobboard
# 4. Run `python 99_bottles.py c` a few times (in different shells)
# 5. On demand kill previously listed processes created in (4) and watch
#    the work resume on another process (and repeat)
# 6. Keep enough workers alive to eventually finish the song (if desired).

ME = os.getpid()
ZK_HOST = "localhost:2181"
JB_CONF = {
    'hosts': ZK_HOST,
    'board': 'zookeeper',
    'path': '/taskflow/99-bottles-demo',
}
PERSISTENCE_URI = r"sqlite:////tmp/bottles.db"
TAKE_DOWN_DELAY = 1.0
PASS_AROUND_DELAY = 3.0
HOW_MANY_BOTTLES = 99


class TakeABottleDown(task.Task):
    def execute(self, bottles_left):
        sys.stdout.write('Take one down, ')
        sys.stdout.flush()
        time.sleep(TAKE_DOWN_DELAY)
        return bottles_left - 1


class PassItAround(task.Task):
    def execute(self):
        sys.stdout.write('pass it around, ')
        sys.stdout.flush()
        time.sleep(PASS_AROUND_DELAY)


class Conclusion(task.Task):
    def execute(self, bottles_left):
        sys.stdout.write('%s bottles of beer on the wall...\n' % bottles_left)
        sys.stdout.flush()


def make_bottles(count):
    s = lf.Flow("bottle-song")

    take_bottle = TakeABottleDown("take-bottle-%s" % count,
                                  inject={'bottles_left': count},
                                  provides='bottles_left')
    pass_it = PassItAround("pass-%s-around" % count)
    next_bottles = Conclusion("next-bottles-%s" % (count - 1))
    s.add(take_bottle, pass_it, next_bottles)

    for bottle in reversed(list(range(1, count))):
        take_bottle = TakeABottleDown("take-bottle-%s" % bottle,
                                      provides='bottles_left')
        pass_it = PassItAround("pass-%s-around" % bottle)
        next_bottles = Conclusion("next-bottles-%s" % (bottle - 1))
        s.add(take_bottle, pass_it, next_bottles)

    return s


def run_conductor():
    event_watches = {}

    # This will be triggered by the conductor doing various activities
    # with engines, and is quite nice to be able to see the various timing
    # segments (which is useful for debugging, or watching, or figuring out
    # where to optimize).
    def on_conductor_event(event, details):
        print("Event '%s' has been received..." % event)
        print("Details = %s" % details)
        if event.endswith("_start"):
            w = timing.StopWatch()
            w.start()
            base_event = event[0:-len("_start")]
            event_watches[base_event] = w
        if event.endswith("_end"):
            base_event = event[0:-len("_end")]
            try:
                w = event_watches.pop(base_event)
                w.stop()
                print("It took %0.3f seconds for event '%s' to finish"
                      % (w.elapsed(), base_event))
            except KeyError:
                pass

    print("Starting conductor with pid: %s" % ME)
    my_name = "conductor-%s" % ME
    persist_backend = persistence_backends.fetch(PERSISTENCE_URI)
    with contextlib.closing(persist_backend):
        with contextlib.closing(persist_backend.get_connection()) as conn:
            conn.upgrade()
        job_backend = job_backends.fetch(my_name, JB_CONF,
                                         persistence=persist_backend)
        job_backend.connect()
        with contextlib.closing(job_backend):
            cond = conductor_backends.fetch('blocking', my_name, job_backend,
                                            persistence=persist_backend)
            cond.notifier.register(cond.notifier.ANY, on_conductor_event)
            # Run forever, and kill -9 or ctrl-c me...
            try:
                cond.run()
            finally:
                cond.stop()
                cond.wait()


def run_poster():
    print("Starting poster with pid: %s" % ME)
    my_name = "poster-%s" % ME
    persist_backend = persistence_backends.fetch(PERSISTENCE_URI)
    with contextlib.closing(persist_backend):
        with contextlib.closing(persist_backend.get_connection()) as conn:
            conn.upgrade()
        job_backend = job_backends.fetch(my_name, JB_CONF,
                                         persistence=persist_backend)
        job_backend.connect()
        with contextlib.closing(job_backend):
            # Create information in the persistence backend about the
            # unit of work we want to complete and the factory that
            # can be called to create the tasks that the work unit needs
            # to be done.
            lb = logbook.LogBook("post-from-%s" % my_name)
            fd = logbook.FlowDetail("song-from-%s" % my_name,
                                    uuidutils.generate_uuid())
            lb.add(fd)
            with contextlib.closing(persist_backend.get_connection()) as conn:
                conn.save_logbook(lb)
            engines.save_factory_details(fd, make_bottles,
                                         [HOW_MANY_BOTTLES], {},
                                         backend=persist_backend)
            # Post, and be done with it!
            job_backend.post("song-from-%s" % my_name, book=lb)


def main():
    if len(sys.argv) == 1:
        sys.stderr.write("%s p|c\n" % os.path.basename(sys.argv[0]))
    elif sys.argv[1] == 'p':
        run_poster()
    elif sys.argv[1] == 'c':
        run_conductor()
    else:
        sys.stderr.write("%s p|c\n" % os.path.basename(sys.argv[0]))


if __name__ == '__main__':
    main()
