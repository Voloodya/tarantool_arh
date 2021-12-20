# Terms

Instance - Tarantool process with Tarantool Data Grid code.

Server - a machine, that can include one or more instances.

Replicaset - a group of one or more instances that replicate all data between each other.

# Preparing a TGZ image for deployment

First you should to get the TGZ archive for TDG.
This can be done in two ways:
- download built archive;
- build archive yourself from the source code.

## Download TGZ

Go to [tarantool.io](https://tarantool.io/) website in your browser
and open customer zone. Here you can find archive of different versions and types.
Download the TGZ archive of the required version.

## Build TGZ

To build archive, you first should install [Docker](https://docs.docker.com/engine/install/).
Then in the root of the project run this:

```shell
export AWS_ACCESS_KEY_ID=""
export AWS_SECRET_ACCESS_KEY=""

git submodule update --init

./make_package.sh tgz
```

This will produce `tdg-<version>.tar.gz` in the `packages` folder.

# Set up servers

## Set up virtual machines

You need [Vagrant](https://www.vagrantup.com).
Then, if you'll be running with VirtualBox, [install it as well](https://www.virtualbox.org/wiki/Downloads).

Then make sure you have `VBoxManage` in your $PATH (e.g. by `which VBoxManage`).

*NB*: the VMs require 4 GB total free RAM (2 GB each).

Go to `deploy` directory and run Vagrant machines:

```shell
cd deploy/
vagrant up
```

This will bring up 2 virtual machines that have Docker
installed and passwordless ssh access for user `vagrant`.
IP addresses of those machines: `172.19.0.2` and `172.19.0.3`.

## Set up physical machines

1. The user under which the installation will take place should have root privileges.
   You also need to disable the password prompt for it in the `/etc/sudoers` file:
   ```
   admin ALL = (ALL: ALL) NOPASSWD: ALL
   ```
2. You should enable SSH server;
3. The public administrator SSH key should be loaded to machine.

# Deploy the cluster

## Prepare

After VMs are created, you should install [Ansible 2.8+](https://docs.ansible.com/ansible/latest/installation_guide/)
and [tarantool.cartridge role 1.10+](https://github.com/tarantool/ansible-cartridge).
For example, you can use it like this:
```shell
pip install ansible~=4.1.0
ansible-galaxy install tarantool.cartridge,1.10.0
```

## Configure

After that, you should set cluster cookie and path to package
in hosts file `hosts.yml` like this:

```yaml
all:
  vars:
    cartridge_package_path: "../../packages/tdg-ENTER-VERSION-HERE.tgz" # path relative to playbook
    cartridge_cluster_cookie: "ENTER-SECRET-COOKIE-HERE"
```

Also, you can change cluster configuration in this file.
Short info about file sections:
- `all.vars` - section for common variables;
- `all.children.tdg_group.hosts` - section for instances parameters;
- `all.children.tdg_group.children` - section to specify parameters
  for a group of instances:
  - here you can group the instances by host
    (set them `ansible_host` parameter)
  - here you can group the instances by replicaset
    (set them parameters `replicaset_alias`, `roles`, `failover_priority`, etc.)

More information about parameters you can read in Tarantool Cartridge Ansible role
[documentation](https://github.com/tarantool/ansible-cartridge).

## Deploy

Now you can run the playbook to deploy only instances:

```shell
ansible-playbook -i hosts.yml playbooks/deploy_without_topology.yml
```

Or for a complete deployment with topology:

```shell
ansible-playbook -i hosts.yml playbooks/deploy.yml
```

Now you can open [http://172.19.0.2:8081](http://172.19.0.2:8081) in your web browser
to see the cluster web interface.

# Manage the cluster

## Configuring topology of cluster

If you have deployed instances with topology, then this step is not necessary.

If you have deployed instances without topology,
then on page [http://172.19.0.2:8081](http://172.19.0.2:8081)
you'll see tarantool instances already discovered by UDP broadcast.

Now you can edit topology (assign roles) as follows:

- `172.19.0.2:3001` (`core`) - `core`
- `172.19.0.2:3002` (`runner_1`) - `failover-coordinator`, `connector`, `runner`
- `172.19.0.2:3003` (`storage_1`) - `storage`
- `172.19.0.2:3004` (`storage_2`) - `storage`

- `172.19.0.3:3001` (`runner_2`) - `connector`, `runner`
- `172.19.0.3:3002` (`storage_1_replica`) - `storage`, replica of `172.19.0.2:3003` (`storage_1`)
- `172.19.0.3:3003` (`storage_2_replica`) - `storage`, replica of `172.19.0.2:3004` (`storage_2`)

After that, you can bootstrap VShard.

## Update package to a new version

To update a package, you should to specify the path to the new package in `hosts.yml`,
and then run deployment script:

```shell
ansible-playbook -i hosts.yml playbooks/deploy_without_topology.yml
```

## Start or stop instances

You can stop and disable instances by `stop.yml` playbook:

```shell
ansible-playbook -i hosts.yml playbooks/stop.yml
```

You can start and enable instances by `start.yml` playbook:

```shell
ansible-playbook -i hosts.yml playbooks/start.yml
```

# Destroy virtual machines

To destroy virtual machines, you should enter this command:

```shell
vagrant destroy -f
```
