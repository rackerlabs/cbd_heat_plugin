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

import uuid
import mock
from mox import MockObject

from heat.common import template_format
from heat.engine import environment
from heat.engine import resource
from heat.engine import scheduler
from heat.engine import stack as parser
from heat.engine import template
from heat.tests import common
from heat.tests import utils

from ..resources import cloud_big_data as cbd
from lavaclient import client

TEMPLATE = """ {
    "heat_template_version": "2014-10-16",
    "resources": {
        "cbd_cluster": {
            "type": "Rackspace::Cloud::BigData",
            "properties": {
                "clusterLogin": "arpi7023",
                "stackId": "HADOOP_HDP2_2",
                "publicKey": "test",
                "publicKeyName": "test",
                "flavor": "Small Hadoop Instance",
                "numSlaveNodes": 3,
                "clusterName": "test"
            }
        }
    }
} """

FLAVOR_ID = {'Small Hadoop Instance':  'hadoop1-7',
             'Medium Hadoop Instance': 'hadoop1-15',
             'Large Hadoop Instance':  'hadoop1-30',
             'XLarge Hadoop Instance': 'hadoop1-60'}

RETURN_CLUSTER_1 = {'_id': 4, 'name': 'cluster_1', 'status': 'ACTIVE',
                    'stack_id': 'HADOOP_HDP2_2', 'cbd_version': 2}

CREATE_CLUSTER_ARG_1 = {'name': 'test',
                        'connectors': [],
                        'node_groups':
                        [{'count': 3,
                          'flavor_id': FLAVOR_ID['Small Hadoop Instance'],
                          'id': 'slave'}],
                        'ssh_keys': [u'test'], 'stack_id': u'HADOOP_HDP2_2',
                        'user_scripts': [], 'username': u'arpi7023'}


class FakeCluster(object):
    """Fake cluster class for testing."""
    def __init__(self, _id=None, name=None, status=None, stack_id=None,
                 cbd_version=None):
        """Fake cluster response."""
        self.id = _id
        self.name = name
        self.status = status
        self.stack_id = stack_id
        self.cbd_version = cbd_version


class BigdataTest(common.HeatTestCase):
    """Cloud Big Data test class."""

    def setUp(self):
        """Initialization."""
        super(BigdataTest, self).setUp()
        resource._register_class('Rackspace::Cloud::BigData',
                                 cbd.CloudBigData)

    def _setup_test_stack(self, stack_name, test_templ):
        """Helper method to parse template and stack."""
        temp = template_format.parse(test_templ)
        templ = template.Template(temp,
                                  env=environment.Environment(
                                      {'key_name': 'test'}))
        stack = parser.Stack(utils.dummy_context(), stack_name, templ,
                             stack_id=str(uuid.uuid4()),
                             stack_user_project_id='8888')
        return (templ, stack)

    def _stubout_create(self, instance, fake_cbdinstance, create_args):
        """Mock cluster creation."""
        mock_client = MockObject(client.Lava)
        self.m.StubOutWithMock(instance, 'cloud_big_data')
        instance.cloud_big_data().AndReturn(mock_client)
        credentials = self.m.CreateMockAnything()
        mock_client.credentials = credentials
        clusters = self.m.CreateMockAnything()
        mock_client.clusters = clusters
        self.m.StubOutWithMock(mock_client.credentials, 'create_ssh_key', True)
        self.m.StubOutWithMock(mock_client.clusters, 'create', True)
        self.m.StubOutWithMock(instance, 'get_flavor_id')
        flavor_name = self._get_flavor_name(
            create_args['node_groups'][0]['flavor_id'])
        instance.get_flavor_id(flavor_name).AndReturn(FLAVOR_ID[flavor_name])
        mock_client.credentials.create_ssh_key("test", "test").AndReturn('')
        mock_client.clusters.create(**create_args).AndReturn(fake_cbdinstance)

    def _setup_test_cluster(self, return_cluster, name, create_args):
        """Helper method to create test cluster."""
        stack_name = '{0}_stack'.format(name)
        templ, self.stack = self._setup_test_stack(stack_name, TEMPLATE)
        cluster_instance = cbd.CloudBigData('%s_name' % name,
                                            templ.resource_definitions(
                                                self.stack)['cbd_cluster'],
                                            self.stack)
        self._stubout_create(cluster_instance, return_cluster, create_args)
        return cluster_instance

    def _create_test_cluster(self, return_cluster, name, create_args):
        """Cluster for testing."""
        cluster = self._setup_test_cluster(return_cluster, name, create_args)
        cluster.check_create_complete = mock.Mock(return_value=True)
        self.m.ReplayAll()
        return cluster

    def _get_flavor_name(self, flavor_id):
        """Get flavor name from id."""
        for name, f_id in FLAVOR_ID.items():
            if f_id == flavor_id:
                return name

    def setup_cluster_delete(self, cluster, fake_cbdinstance):
        """Mock cluster delete."""
        mock_client = MockObject(client.Lava)
        self.m.StubOutWithMock(cluster, 'cloud_big_data')
        cluster.cloud_big_data().AndReturn(mock_client)
        clusters = self.m.CreateMockAnything()
        mock_client.clusters = clusters
        self.m.StubOutWithMock(mock_client.clusters, 'delete', True)
        mock_client.clusters.delete(str(fake_cbdinstance.id)).AndReturn('')
        cluster.check_delete_complete = mock.Mock(return_value=True)
        self.m.ReplayAll()

    def test_cluster_create(self):
        """Test basic cluster creation."""
        fake_cluster = FakeCluster(**RETURN_CLUSTER_1)
        cluster = self._create_test_cluster(
            fake_cluster, 'stack_delete', CREATE_CLUSTER_ARG_1)
        scheduler.TaskRunner(cluster.create)()
        self.assertEqual((cluster.CREATE, cluster.COMPLETE), cluster.state)
        self.m.VerifyAll()

    def test_cluster_delete(self):
        """Test basic cluster deletion."""
        fake_cluster = FakeCluster(**RETURN_CLUSTER_1)
        cluster = self._create_test_cluster(
            fake_cluster, 'stack_delete', CREATE_CLUSTER_ARG_1)
        scheduler.TaskRunner(cluster.create)()
        self.m.UnsetStubs()
        self.setup_cluster_delete(cluster, fake_cbdinstance=fake_cluster)
        scheduler.TaskRunner(cluster.delete)()
        self.assertEqual((cluster.DELETE, cluster.COMPLETE), cluster.state)
        self.m.VerifyAll()
        self.m.UnsetStubs()
