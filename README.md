Rackspace Cloud Big Data Heat Resource Plugin
=============================================

### Overview
This project defines the [Rackspace Cloud Big Data](http://www.rackspace.com/en-us/cloud/big-data) (CBD) team's [Cloud Orchestration](http://www.rackspace.com/en-us/cloud/orchestration) ([OpenStack Heat](https://wiki.openstack.org/wiki/Heat)) Resource Plugin. The CBD plugin enables accessing Rackspace cloud big data services through [Heat Orchestration Templates](http://docs.openstack.org/developer/heat/template_guide/hot_guide.html) (HOTs) by editing a YAML file or through the [Rackspace Control Panel](https://mycloud.rackspace.com/) Orchestration management interface. The Rackspace Cloud Big Data Heat resource plugin identifier is:

> Rackspace::Cloud::BigData

### License
[Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0)

### Resource Plugin Capabilities
This plugin implements the Heat create and delete functionality which enables CBD cluster creation and deletion. Updates, such as resizing clusters, may be added later. For now, it is recommeneded that one of the full featured interfaces be used for advanced operations such as updates:
* [Rackspace Control Panel](https://mycloud.rackspace.com/)
* [Rackspace Cloud Big Data CLI](https://github.com/rackerlabs/python-lavaclient/)
* [Rackspace Cloud Big Data API](http://docs.rackspace.com/cbd/api/v1.0/cbd-devguide/content/overview.html)

### Code Structure
Instead of cloning the entire [Heat repository](https://github.com/openstack/heat), this code mimics the structure of the Heat repository starting at [heat/contrib/rackspace](https://github.com/openstack/heat/tree/master/contrib/rackspace). Please note that files like [clients.py](https://github.com/rackerlabs/cbd_heat_plugin/blob/master/rackspace/clients.py) have various code snippets from other plugins.

Please note that the [hots](https://github.com/rackerlabs/cbd_heat_plugin/tree/master/hots) directory has example HOT YAML files for creating various Rackspace Cloud Big Data stacks.

### Heat CLI Examples
The following are useful Heat CLI examples used in the creation and testing of the CBD plugin. It is recommened to edit and fill in a HOT file such as [hadoop.yaml](https://github.com/rackerlabs/cbd_heat_plugin/blob/master/hots/hadoop.yaml) to use in the examples. User and environment-specific data required for the Heat CLI is identified inside a <> below.

Notes:
* region - will look like DFW, ORD, IAD, LON, etc
* heat_url - has a format like http://<heat_server_ip>/v1/<tenant_id>

##### Create a Heat Stack
```sh
heat --os-region-name <region> --os-auth-url https://identity.api.rackspacecloud.com/v2.0/ --os-tenant-id <tenant_id> --heat-url <heat_url> --os-username <user_name> --os-password <password> stack-create -f <hot_template> <my_heat_stack_name>
```

##### List All Heat Stacks
```sh
heat --os-region-name <region> --os-auth-url https://identity.api.rackspacecloud.com/v2.0/ --os-tenant-id <tenant_id> --heat-url <heat_url> --os-username <user_name> --os-password <password> stack-list
```

##### Show Heat Stack Details
```sh
heat --os-region-name <region> --os-auth-url https://identity.api.rackspacecloud.com/v2.0/ --os-tenant-id <tenant_id> --heat-url <heat_url> --os-username <user_name> --os-password <password> stack-show <my_heat_stack_name>
```

##### Delete a Heat Stack
```sh
heat --os-region-name <region> --os-auth-url https://identity.api.rackspacecloud.com/v2.0/ --os-tenant-id <tenant_id> --heat-url <heat_url> --os-username <user_name> --os-password <password> stack-delete <my_heat_stack_name>
```
