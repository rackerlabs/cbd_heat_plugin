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
from ..clients import StackConstraint, FlavorConstraint, \
    RackspaceCBDClientPlugin, cfg

from heat.common import template_format
from heat.engine import environment
from heat.engine import resource
from heat.engine import scheduler
from heat.engine import stack as parser
from heat.engine import template
from heat.tests import common
from heat.tests import utils

from ..resources import cloud_big_data as cbd
from mock import MagicMock
from heat.engine import resource as res


TEMPLATE = """ {
    "heat_template_version": "2014-10-16",
    "resources": {
        "cbd_cluster": {
            "type": "Rackspace::Cloud::BigData",
            "properties": {
                "clusterLogin": "test_user",
                "stackId": "HADOOP_HDP2_2",
                "publicKey": "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC0UGHHrNc
                              EekIsAeoXQxb1Ed8F3or3Zl402bCWTcSeZC9uTOKmi0WJK
                              s7zFJf78ueM5J",
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
                        'user_scripts': [], 'username': u'test_user'}


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

# pylint: disable=no-init
class BigdataTest(common.HeatTestCase):

    """Cloud Big Data test class."""

    def setUp(self):
        """Initialization."""
        super(BigdataTest, self).setUp()
        RackspaceCBDClientPlugin._client = mock.MagicMock()

        self.mck_cbd_client = mock.MagicMock()
        self.patchobject(RackspaceCBDClientPlugin,
                         '_create').return_value = self.mck_cbd_client
        self.client_plugin = RackspaceCBDClientPlugin(context=mock.MagicMock())
        cfg.CONF.set_override('region_name_for_services', 'RegionOne')
        resource._register_class('Rackspace::Cloud::BigData',
                                 cbd.CloudBigData)

    def stub_StackConstraint_validate(self):
        validate = self.patchobject(StackConstraint, 'validate')
        validate.return_value = True

    def stub_FlavorConstraint_validate(self):
        validate = self.patchobject(FlavorConstraint, 'validate')
        validate.return_value = True

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

    def setup_cluster_delete(self, cluster):
        self.mck_cbd_client.cluster.delete.return_value = True
        cluster.check_delete_complete = mock.Mock(return_value=True)
        self.m.ReplayAll()

    def _stubout_create(self, fake_cbdinstance):
        """Mock cluster creation."""
        self.stub_StackConstraint_validate()
        self.stub_FlavorConstraint_validate()
        self.mck_cbd_client.credentials.create_ssh_key.\
            return_value = MagicMock()
        self.mck_cbd_client.clusters.create.return_value = MagicMock(
            fake_cbdinstance)
        self.patchobject(RackspaceCBDClientPlugin,
                         'get_flavor_id').return_value = 'hadoop1-7'

    def _setup_test_cluster(self, return_cluster, name, create_args):
        """Helper method to create test cluster."""
        stack_name = '{0}_stack'.format(name)
        templ, self.stack = self._setup_test_stack(stack_name, TEMPLATE)
        cluster_instance = cbd.CloudBigData('%s_name' % name,
                                            templ.resource_definitions(
                                                self.stack)['cbd_cluster'],
                                            self.stack)
        self._stubout_create(return_cluster)
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

    @mock.patch.object(res.Resource, 'is_service_available')
    def test_cluster_create(self, mock_is_service_available):
        """Test basic cluster creation."""

        mock_is_service_available.return_value = True
        fake_cluster = FakeCluster(**RETURN_CLUSTER_1)
        cluster = self._create_test_cluster(
            fake_cluster, 'stack_delete', CREATE_CLUSTER_ARG_1)
        scheduler.TaskRunner(cluster.create)()
        self.assertEqual((cluster.CREATE, cluster.COMPLETE), cluster.state)
        self.m.VerifyAll()

    @mock.patch.object(res.Resource, 'is_service_available')
    def test_cluster_delete(self, mock_is_service_available):
        """Test basic cluster deletion."""

        mock_is_service_available.return_value = True
        fake_cluster = FakeCluster(**RETURN_CLUSTER_1)
        cluster = self._create_test_cluster(
            fake_cluster, 'stack_delete', CREATE_CLUSTER_ARG_1)
        scheduler.TaskRunner(cluster.create)()
        self.m.UnsetStubs()
        self.setup_cluster_delete(cluster)
        scheduler.TaskRunner(cluster.delete)()
        self.assertEqual((cluster.DELETE, cluster.COMPLETE), cluster.state)
        self.m.VerifyAll()
        self.m.UnsetStubs()
