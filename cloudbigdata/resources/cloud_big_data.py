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

"""Resources for Rackspace Cloud Big Data."""

from oslo_log import log as logging

from heat.common.i18n import _
from heat.engine import attributes
from heat.engine import constraints
from heat.engine import properties
from heat.engine import resource
from heat.engine import support

from lavaclient.error import LavaError, RequestError


LOG = logging.getLogger(__name__)


class CloudBigData(resource.Resource):
    """Represents a Cloud Big Data resource."""
    support_status = support.SupportStatus(version='2015.8')

    PROPERTIES = (
        CLUSTER_NAME, STACK_ID, FLAVOR, NUM_SLAVES, CLUSTER_LOGIN,
        PUB_KEY_NAME, PUB_KEY,
    ) = (
        'clusterName', 'stackId', 'flavor', 'numSlaveNodes', 'clusterLogin',
        'publicKeyName', 'publicKey', )

    properties_schema = {
        CLUSTER_NAME: properties.Schema(
            properties.Schema.STRING,
            _('Rackspace Cloud Big Data Cluster Name.'),
            constraints=[
                constraints.Length(max=50,
                                   description="Cluster name is to long.")
            ],
            required=True
        ),
        STACK_ID: properties.Schema(
            properties.Schema.STRING,
            _('Rackspace Cloud Big Data Stack ID.'),
            constraints=[
                constraints.CustomConstraint('cbd.stack')
            ],
            required=True
        ),
        FLAVOR: properties.Schema(
            properties.Schema.STRING,
            _('Rackspace Cloud Big Data Flavor ID to be used for cluster slave'
              'nodes.'),
            constraints=[
                constraints.CustomConstraint('cbd.flavor')
            ],
            required=True
        ),
        CLUSTER_LOGIN: properties.Schema(
            properties.Schema.STRING,
            _('Cluster SSH login.'),
            constraints=[
                constraints.Length(max=50,
                                   description="Cluster SSH login is to long.")
            ],
            required=True
        ),
        NUM_SLAVES: properties.Schema(
            properties.Schema.INTEGER,
            _('How many slave nodes to create in the cluster.'),
            default=3,
            constraints=[
                constraints.Range(1, 10, "Number of slave nodes must be "
                                  "1-10."),
            ]
        ),
        PUB_KEY_NAME: properties.Schema(
            properties.Schema.STRING,
            _('Cluster public key name. This key name will be used along with '
              'the publicKey by the Cloud Big Data system to install SSH keys '
              'on to CBD clusters for user access. If the key name already '
              'exists, it will not be overwritten and the existing key will '
              'be used instead.'),
            constraints=[
                constraints.Length(max=50,
                                   description="Public key name is to long.")
            ],
            required=True
        ),
        PUB_KEY: properties.Schema(
            properties.Schema.STRING,
            _('Cluster public key used to SSH into cluster nodes.'),
            constraints=[
                constraints.Length(max=1000,
                                   description="Public key is to long.")
            ],
            required=True
        )
    }

    ATTRIBUTES = (
        CBD_VERSION
    ) = (
        'cbdVersion'
    )

    attributes_schema = {
        CBD_VERSION: attributes.Schema(
            _("Rackspace Cloud Big Data version"),
            type=attributes.Schema.STRING
        )
    }

    default_client_name = "cloud_big_data"

    def handle_create(self):
        """Create a Rackspace Cloud Big Data Instance."""
        LOG.debug("Cloud Big Data handle_create called.")
        args = dict((key, val) for key, val in self.properties.items())
        # Create the cluster SSH key
        try:
            self.client().credentials.create_ssh_key(
                args[self.PUB_KEY_NAME],
                args[self.PUB_KEY])
        except LavaError:
            pass  # A key may already exist

        # Create the cluster
        flavor_id = self.client_plugin().get_flavor_id(args[self.FLAVOR])
        num_slave_nodes = args[self.NUM_SLAVES]

        node_group_list = [{'flavor_id': flavor_id,
                            'count': num_slave_nodes,
                            'id': 'slave'}]
        try:
            cluster = self.client().clusters.create(
                name=args[self.CLUSTER_NAME],
                stack_id=args[self.STACK_ID],
                username=args[self.CLUSTER_LOGIN],
                ssh_keys=[args[self.PUB_KEY_NAME]],
                user_scripts=[],
                node_groups=node_group_list,
                connectors=[])
        except LavaError as exc:
            LOG.warning("Unable to create CBD cluster", exc_info=exc)
            raise
        self.resource_id_set(str(cluster.id))

    def _show_resource(self):
        """ Show cluster resource details"""
        return self.client().clusters.get(self.resource_id)

    def check_create_complete(self, ignored):
        """Check the cluster creation status."""
        try:
            cluster = self._show_resource()
        except RequestError as exc:
            # RequestError is the only exception that should be retried and
            # only a 503 HTTP status code should be retried. Only 4xx-5xx
            # codes are returned by this exception.
            if exc.code == 503:
                return False
            raise
        # If any other LavaError-based exception is raised, it is a failed
        # cluster create. Let the exception bubble up to Heat.

        if cluster.status == 'ACTIVE':
            return True
        if cluster.status == 'ERROR':
            raise LavaError("Cluster {} entered an error state".format(
                self.resource_id))
        return False

    def handle_delete(self):
        """Delete a Rackspace Cloud Big Data Instance."""
        LOG.debug("Cloud Big Data handle_delete called.")
        if self.resource_id:
            try:
                self.client().clusters.delete(self.resource_id)
            except LavaError as exc:
                self.client_plugin().ignore_not_found(exc)

    def check_delete_complete(self, ignored):
        """
        Return deletion status.
        :param result: None
        if handle_delete returns any result, we can use
        it here.
        """
        if self.resource_id is None:
            return True
        try:
            self.client().clusters.get(self.resource_id)
        except LavaError as exc:
            self.client_plugin().ignore_not_found(exc)
            return True
        return False

    def _resolve_attribute(self, name):
        """Enable returning of Cloud Big Data cluster ID."""
        try:
            cluster = self.client().clusters.get(self.resource_id)
        except LavaError as exc:
            LOG.error("Unable to find CBD cluster due to: %s", exc)
            return None

        if name == self.CBD_VERSION:
            return cluster.cbd_version


def resource_mapping():
    """Return the Rackspace Cloud Big Data identifier."""
    return {'Rackspace::Cloud::BigData': CloudBigData}


def available_resource_mapping():
    """Return the available resource map."""
    return resource_mapping()
