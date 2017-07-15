# -*- coding: utf-8 -*-

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

import time

from oslo_utils import uuidutils
import six
import testtools
import consul_kv

from taskflow import exceptions as excp
from taskflow.jobs.backends import impl_consul
from taskflow import states
from taskflow import test
from taskflow.tests.unit.jobs import base
from taskflow.tests import utils as test_utils
from taskflow.tests.utils import CONSUL_TEST_ENDPOINT
from taskflow.utils import persistence_utils as p_utils


CONSUL_AVAILABLE = test_utils.consul_available(
    impl_consul.ConsulJobBoard.MIN_CONSUL_VERSION)


@testtools.skipIf(not CONSUL_AVAILABLE, 'consul is not available')
class ConsulJobboardTest(test.TestCase, base.BoardTestMixin):
    def close_client(self, client):
        client.close()

    def create_board(self, persistence=None):
        namespace = uuidutils.generate_uuid()
        client = consul_kv.Connection(endpoint=CONSUL_TEST_ENDPOINT)
        config = {
            'namespace': six.b("taskflow-{}" % namespace),
        }
        kwargs = {
            'client': client,
            'persistence': persistence,
        }
        board = impl_consul.ConsulJobBoard('test-board', config, **kwargs)
        self.addCleanup(board.close)
        self.addCleanup(self.close_client, client)
        return client, board

    def test_posting_claim_expiry(self):

        with base.connect_close(self.board):
            with self.flush(self.client):
                self.board.post('test', p_utils.temporary_log_book())

            self.assertEqual(1, self.board.job_count)
            possible_jobs = list(self.board.iterjobs(only_unclaimed=True))
            self.assertEqual(1, len(possible_jobs))
            j = possible_jobs[0]
            self.assertEqual(states.UNCLAIMED, j.state)

            with self.flush(self.client):
                self.board.claim(j, self.board.name, expiry=0.5)

            self.assertEqual(self.board.name, self.board.find_owner(j))
            self.assertEqual(states.CLAIMED, j.state)

            time.sleep(0.6)
            self.assertEqual(states.UNCLAIMED, j.state)
            possible_jobs = list(self.board.iterjobs(only_unclaimed=True))
            self.assertEqual(1, len(possible_jobs))

    def test_posting_claim_same_owner(self):
        with base.connect_close(self.board):
            with self.flush(self.client):
                self.board.post('test', p_utils.temporary_log_book())

            self.assertEqual(1, self.board.job_count)
            possible_jobs = list(self.board.iterjobs(only_unclaimed=True))
            self.assertEqual(1, len(possible_jobs))
            j = possible_jobs[0]
            self.assertEqual(states.UNCLAIMED, j.state)

            with self.flush(self.client):
                self.board.claim(j, self.board.name)

            possible_jobs = list(self.board.iterjobs())
            self.assertEqual(1, len(possible_jobs))
            with self.flush(self.client):
                self.assertRaises(excp.UnclaimableJob, self.board.claim,
                                  possible_jobs[0], self.board.name)
            possible_jobs = list(self.board.iterjobs(only_unclaimed=True))
            self.assertEqual(0, len(possible_jobs))

    def setUp(self):
        super(ConsulJobboardTest, self).setUp()
        self.client, self.board = self.create_board()
