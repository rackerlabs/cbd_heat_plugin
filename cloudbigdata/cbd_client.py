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

"""Client Libraries for Rackspace Resources."""

from lavaclient.client import Lava
from lavaclient.error import LavaError
from lavaclient.error import RequestError
from oslo_config import cfg
from oslo_log import log as logging

from heat.common import exception
from heat.common.i18n import _LI
from heat.common.i18n import _LW
from heat.engine.clients import client_plugin
from heat.engine import constraints


LOG = logging.getLogger(__name__)


class StackConstraint(constraints.BaseCustomConstraint):
    """Validate CBD stack IDs."""
    expected_exceptions = (RequestError,)

    def validate_with_client(self, client, stack_id):
        """Check stack ID with CBD client."""
        try:
            client.client("cloud_big_data").stacks.get(stack_id)
        except RequestError as exc:
            if exc.code == 404:  # Resource not found
                raise
            raise LavaError(exc)


class FlavorConstraint(constraints.BaseCustomConstraint):
    """Validate CBD flavors."""
    expected_exceptions = (exception.FlavorMissing,)

    def validate_with_client(self, client, flavor):
        """Check flavor with CBD client."""
        client.client_plugin("cloud_big_data").get_flavor_id(flavor)


class RackspaceCBDClientPlugin(client_plugin.ClientPlugin):
    """Cloud Big Data client plugin.

    Creating a new class instead of complicating the original class
    since CBD is not Pyrax-based.
    """
    def get_flavor_id(self, flavor):
        """Get the id for the specified flavor name.

        If the specified value is flavor id, just return it.
        :param flavor: the name of the flavor to find
        :returns: the id of :flavor:
        :raises: exception.EntityNotFound
        """
        flavor_id = None
        try:
            flavor_list = self.client().flavors.list()
        except LavaError as exc:
            LOG.info("Unable to read CBD flavor list", exc_info=exc)
            raise
        for bigdata_flavor in flavor_list:
            if bigdata_flavor.name == flavor or bigdata_flavor.id == flavor:
                flavor_id = bigdata_flavor.id
                break
        if flavor_id is None:
            LOG.info("Unable to find CBD flavor %s", flavor)
            raise exception.EntityNotFound(entity='Flavor', name=flavor)
        return flavor_id

    def _create(self):
        """Create an authenticated CBD client."""
        region = cfg.CONF.region_name_for_services.lower()
        if self.context.region_name:
            region = self.context.region_name.lower()
        LOG.info(_LI("CBD client authenticating username %s in region %s"),
                 self.context.username, region)
        tenant = self.context.tenant_id
        username = self.context.username
        endpoint_uri = ("https://{region}.bigdata.api.rackspacecloud.com:443/"
                        "v2/{tenant}".format(region=region, tenant=tenant))
        try:
            return Lava(username=username,
                        tenant_id=self.context.tenant_id,
                        auth_url=self.context.auth_url,
                        api_key=None,
                        token=self.context.auth_token,
                        region=region,
                        endpoint=endpoint_uri,
                        verify_ssl=False)
        except LavaError as exc:
            LOG.warn(_LW("CBD client authentication failed: %s."), exc)
            raise exception.AuthorizationFailure()
        LOG.info(_LI("CBD user %s authenticated successfully."), username)

    def is_not_found(self, exc):
        """Determine if a CBD cluster exists."""
        return (isinstance(exc, RequestError) and
                exc.code == 404)
