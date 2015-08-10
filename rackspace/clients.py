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
from oslo_config import cfg
from oslo_log import log as logging

from heat.common import exception
from heat.common.i18n import _LI
from heat.common.i18n import _LW
from heat.engine.clients import client_plugin


LOG = logging.getLogger(__name__)


class RackspaceCBDClientPlugin(client_plugin.ClientPlugin):
    """Cloud Big Data client plugin.

    Creating a new class instead of complicating the original class
    since CBD is not Pyrax-based.
    """
    lava_client = None

    def _get_client(self, _):
        """Return the CBD Lava client."""
        return self.lava_client

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
            lava_client = Lava(username=username,
                               tenant_id=self.context.tenant_id,
                               auth_url=self.context.auth_url,
                               api_key=None,
                               token=self.context.auth_token,
                               region=region,
                               endpoint=endpoint_uri,
                               verify_ssl=False)
            return lava_client
        except LavaError as exc:
            LOG.warn(_LW("CBD client authentication failed: %s."), exc)
            raise exception.AuthorizationFailure()
        LOG.info(_LI("User %s authenticated successfully."), username)
