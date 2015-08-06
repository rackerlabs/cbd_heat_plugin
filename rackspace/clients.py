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

import hashlib
import itertools
import random
import time

from glanceclient import client as gc
from lavaclient.client import Lava
from lavaclient.error import LavaError
from oslo_config import cfg
from oslo_log import log as logging
from six.moves.urllib import parse
from swiftclient import utils as swiftclient_utils
from troveclient import client as tc

from heat.common import exception
from heat.common.i18n import _LI
from heat.common.i18n import _LW
from heat.engine import constraints
from heat.engine.clients import client_plugin
from heat.engine.clients.os import cinder
from heat.engine.clients.os import glance
from heat.engine.clients.os import nova
from heat.engine.clients.os import swift
from heat.engine.clients.os import trove


LOG = logging.getLogger(__name__)

try:
    import pyrax
except ImportError:
    pyrax = None


class RackspaceClientPlugin(client_plugin.ClientPlugin):

    if pyrax is not None:
        exceptions_module = pyrax.exceptions

    rax_auth = None

    def _get_client(self, name):
        if self.rax_auth is None:
            self._authenticate()
        return self.rax_auth.get_client(
            name, cfg.CONF.region_name_for_services)

    def _authenticate(self):
        """Create an authenticated client context."""
        self.rax_auth = pyrax.create_context("rackspace")
        self.rax_auth.auth_endpoint = self.context.auth_url
        LOG.info(_LI("Authenticating username: %s") %
                 self.context.username)
        tenant = self.context.tenant_id
        tenant_name = self.context.tenant
        self.rax_auth.auth_with_token(self.context.auth_token,
                                      tenant_id=tenant,
                                      tenant_name=tenant_name)
        if not self.rax_auth.authenticated:
            LOG.warn(_LW("Pyrax Authentication Failed."))
            raise exception.AuthorizationFailure()
        LOG.info(_LI("User %s authenticated successfully."),
                 self.context.username)

    def is_not_found(self, ex):
        return isinstance(ex, self.exceptions_module.NotFound)

    def is_over_limit(self, ex):
        return isinstance(ex, self.exceptions_module.OverLimit)


class RackspaceAutoScaleClient(RackspaceClientPlugin):

    def _create(self):
        """Rackspace Auto Scale client."""
        return self._get_client("autoscale")


class RackspaceCloudLBClient(RackspaceClientPlugin):

    def _create(self):
        """Rackspace cloud loadbalancer client."""
        return self._get_client("load_balancer")


class RackspaceCloudDNSClient(RackspaceClientPlugin):

    def _create(self):
        """Rackspace cloud dns client."""
        return self._get_client("dns")


class RackspaceNovaClient(nova.NovaClientPlugin,
                          RackspaceClientPlugin):

    def _create(self):
        """Rackspace cloudservers client."""
        client = self._get_client("compute")
        if not client:
            client = super(RackspaceNovaClient, self)._create()
        return client


class RackspaceCloudNetworksClient(RackspaceClientPlugin):

    def _create(self):
        """
        Rackspace cloud networks client.

        Though pyrax "fixed" the network client bugs that were introduced
        in 1.8, it still doesn't work for contexts because of caching of the
        nova client.
        """
        if self.rax_auth is None:
            self._authenticate()
        # need special handling now since the contextual
        # pyrax doesn't handle "networks" not being in
        # the catalog
        ep = pyrax._get_service_endpoint(
            self.rax_auth, "compute", region=cfg.CONF.region_name_for_services)
        cls = pyrax._client_classes['compute:network']
        client = cls(self.rax_auth,
                     region_name=cfg.CONF.region_name_for_services,
                     management_url=ep)
        return client


class RackspaceTroveClient(trove.TroveClientPlugin):
    """
    Rackspace trove client.

    Since the pyrax module uses its own client implementation for Cloud
    Databases, we have to skip pyrax on this one and override the super
    implementation to account for custom service type and regionalized
    management url.
    """

    def _create(self):
        service_type = "rax:database"
        con = self.context
        endpoint_type = self._get_client_option('trove', 'endpoint_type')
        args = {
            'service_type': service_type,
            'auth_url': con.auth_url,
            'proxy_token': con.auth_token,
            'username': None,
            'password': None,
            'cacert': self._get_client_option('trove', 'ca_file'),
            'insecure': self._get_client_option('trove', 'insecure'),
            'endpoint_type': endpoint_type
        }

        client = tc.Client('1.0', **args)
        region = cfg.CONF.region_name_for_services
        management_url = self.url_for(service_type=service_type,
                                      endpoint_type=endpoint_type,
                                      region_name=region)
        client.client.auth_token = con.auth_token
        client.client.management_url = management_url

        return client


class RackspaceCinderClient(cinder.CinderClientPlugin):

    def _create(self):
        """Override the region for the cinder client."""
        client = super(RackspaceCinderClient, self)._create()
        management_url = self.url_for(
            service_type='volume',
            region_name=cfg.CONF.region_name_for_services)
        client.client.management_url = management_url
        return client


class RackspaceSwiftClient(swift.SwiftClientPlugin):

    def is_valid_temp_url_path(self, path):
        '''Return True if path is a valid Swift TempURL path, False otherwise.

        A Swift TempURL path must:
        - Be five parts, ['', 'v1', 'account', 'container', 'object']
        - Be a v1 request
        - Have account, container, and object values
        - Have an object value with more than just '/'s

        :param path: The TempURL path
        :type path: string
        '''
        parts = path.split('/', 4)
        return bool(len(parts) == 5 and
                    not parts[0] and
                    parts[1] == 'v1' and
                    parts[2] and
                    parts[3] and
                    parts[4].strip('/'))

    def get_temp_url(self, container_name, obj_name, timeout=None,
                     method="PUT"):
        '''
        Return a Swift TempURL.
        '''

        sw_url = parse.urlparse(self.client().url)
        tenant_uuid = sw_url.path.split("/")[-1]

        key_header = 'x-account-meta-temp-url-key'
        if key_header in self.client().head_account():
            key = self.client().head_account()[key_header]
        else:
            key = hashlib.sha224(str(random.getrandbits(256))).hexdigest()[:32]
            self.client().post_account({key_header: key})

        path = '/v1/%s/%s/%s' % (tenant_uuid, container_name, obj_name)
        if timeout is None:
            timeout = swift.MAX_EPOCH - 60 - time.time()
        tempurl = swiftclient_utils.generate_temp_url(path, timeout, key,
                                                      method)
        return '%s://%s%s' % (sw_url.scheme, sw_url.netloc, tempurl)


class RackspaceGlanceClient(glance.GlanceClientPlugin):

    def _create(self):
        con = self.context
        endpoint_type = self._get_client_option('glance', 'endpoint_type')
        endpoint = self.url_for(
            service_type='image',
            endpoint_type=endpoint_type,
            region_name=cfg.CONF.region_name_for_services)
        # Rackspace service catalog includes a tenant scoped glance
        # endpoint so we have to munge the url a bit
        glance_url = parse.urlparse(endpoint)
        # remove the tenant and following from the url
        endpoint = "%s://%s" % (glance_url.scheme, glance_url.hostname)
        args = {
            'auth_url': con.auth_url,
            'service_type': 'image',
            'project_id': con.tenant,
            'token': self.auth_token,
            'endpoint_type': endpoint_type,
            'ca_file': self._get_client_option('glance', 'ca_file'),
            'cert_file': self._get_client_option('glance', 'cert_file'),
            'key_file': self._get_client_option('glance', 'key_file'),
            'insecure': self._get_client_option('glance', 'insecure')
        }
        return gc.Client('2', endpoint, **args)


class RackspaceMonitoringClient(RackspaceClientPlugin):

    def _create(self):
        return self._get_client("cloud_monitoring")


class CheckTypeConstraint(constraints.BaseCustomConstraint):

    if pyrax is not None:
        expected_exceptions = (pyrax.exceptions.NotFound,)

    def validate_with_client(self, clients, value):
        types = [t.id for t
                 in clients.client("cloud_monitoring").list_check_types()]
        if value not in types:
            raise pyrax.exceptions.NotFound(404, message="Invalid check type; "
                                            "must be one of %s" % types)


class NotificationTypeConstraint(constraints.BaseCustomConstraint):

    if pyrax is not None:
        expected_exceptions = (pyrax.exceptions.NotFound,)

    def validate_with_client(self, clients, value):
        types = [t.id for t in
                 clients.client("cloud_monitoring").list_notification_types()]
        if value not in types:
            raise pyrax.exceptions.NotFound(404, message="Invalid notification"
                                            " type; must be one of %s" % types)


class MonitoringZoneConstraint(constraints.BaseCustomConstraint):

    if pyrax is not None:
        expected_exceptions = (pyrax.exceptions.NotFound,)

    def validate_with_client(self, clients, value):
        m_zones = clients.client("cloud_monitoring").list_monitoring_zones()
        zones = itertools.chain.from_iterable([(z.id, z.label)
                                               for z in m_zones])
        if value not in list(zones):
            raise pyrax.exceptions.NotFound(404, message="%s is not a valid "
                                            "monitoring zone." % value)


class RackspaceRackConnectClient(RackspaceClientPlugin):

    def _create(self):
        """Rackspace RackConnect client."""
        return self._get_client("rackconnect")


class RackconnectNetworkConstraint(constraints.BaseCustomConstraint):

    if pyrax:
        expected_exceptions = (pyrax.exceptions.NotFound,)

    def validate_with_client(self, clients, value):
        nets = clients.client("rackconnect").list_networks()
        vals = itertools.chain.from_iterable([(n.id, n.name) for n in nets])
        if value not in list(vals):
            raise pyrax.exceptions.NotFound(404, message="%s is not a valid "
                                            "Rackconnect V3 network" % value)


class RackconnectPoolConstraint(constraints.BaseCustomConstraint):

    if pyrax:
        expected_exceptions = (pyrax.exceptions.NotFound,)

    def validate_with_client(self, clients, value):
        pools = clients.client("rackconnect").list_load_balancer_pools()
        vals = itertools.chain.from_iterable([(p.id, p.name) for p in pools])
        if value not in list(vals):
            raise pyrax.exceptions.NotFound(404, message="%s is not a valid "
                                            "Rackconnect V3 load balancer pool"
                                            % value)


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
