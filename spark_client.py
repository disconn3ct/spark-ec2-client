#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import division, print_function, with_statement

import hashlib
import itertools
import logging
import os
import os.path
import pipes
import shutil
from stat import S_IRUSR
import subprocess
import sys
import tarfile
import tempfile
import textwrap
import time
from datetime import datetime
from optparse import OptionParser
from sys import stderr

if sys.version < "3":
    from urllib2 import urlopen, Request, HTTPError
else:
    from urllib.request import urlopen, Request
    from urllib.error import HTTPError
    raw_input = input
    xrange = range

SPARK_EC2_VERSION = "2.0.0"
SPARK_EC2_DIR = os.path.dirname(os.path.realpath(__file__))

VALID_SPARK_VERSIONS = set([
    "0.7.3",
    "0.8.0",
    "0.8.1",
    "0.9.0",
    "0.9.1",
    "0.9.2",
    "1.0.0",
    "1.0.1",
    "1.0.2",
    "1.1.0",
    "1.1.1",
    "1.2.0",
    "1.2.1",
    "1.3.0",
    "1.3.1",
    "1.4.0",
    "1.4.1",
    "1.5.0",
    "1.5.1",
    "1.5.2",
    "1.6.0",
    "1.6.1",
    "1.6.2",
    "1.6.3",
    "2.0.0-preview",
    "2.0.0",
    "2.0.1",
    "2.0.2",
    "2.1.0"
])

SPARK_TACHYON_MAP = {
    "1.0.0": "0.4.1",
    "1.0.1": "0.4.1",
    "1.0.2": "0.4.1",
    "1.1.0": "0.5.0",
    "1.1.1": "0.5.0",
    "1.2.0": "0.5.0",
    "1.2.1": "0.5.0",
    "1.3.0": "0.5.0",
    "1.3.1": "0.5.0",
    "1.4.0": "0.6.4",
    "1.4.1": "0.6.4",
    "1.5.0": "0.7.1",
    "1.5.1": "0.7.1",
    "1.5.2": "0.7.1",
    "1.6.0": "0.8.2",
    "2.0.0-preview": "",
}

DEFAULT_SPARK_VERSION = SPARK_EC2_VERSION
DEFAULT_SPARK_GITHUB_REPO = "https://github.com/apache/spark"

# Default location to get the spark-ec2 scripts (and ami-list) from
DEFAULT_SPARK_EC2_GITHUB_REPO = "https://github.com/disconn3ct/spark-ec2"
DEFAULT_SPARK_EC2_BRANCH = "branch-2.0"

# Other defaults
DEFAULT_USER = "root"

def setup_external_libs(libs):
    """
    Download external libraries from PyPI to SPARK_EC2_DIR/lib/ and prepend them to our PATH.
    """
    PYPI_URL_PREFIX = "https://pypi.python.org/packages/source"
    SPARK_EC2_LIB_DIR = os.path.join(SPARK_EC2_DIR, "lib")

    if not os.path.exists(SPARK_EC2_LIB_DIR):
        print("Downloading external libraries that spark-ec2 needs from PyPI to {path}...".format(
            path=SPARK_EC2_LIB_DIR
        ))
        print("This should be a one-time operation.")
        os.mkdir(SPARK_EC2_LIB_DIR)

    for lib in libs:
        versioned_lib_name = "{n}-{v}".format(n=lib["name"], v=lib["version"])
        lib_dir = os.path.join(SPARK_EC2_LIB_DIR, versioned_lib_name)

        if not os.path.isdir(lib_dir):
            tgz_file_path = os.path.join(SPARK_EC2_LIB_DIR, versioned_lib_name + ".tar.gz")
            print(" - Downloading {lib}...".format(lib=lib["name"]))
            download_stream = urlopen(
                "{prefix}/{first_letter}/{lib_name}/{lib_name}-{lib_version}.tar.gz".format(
                    prefix=PYPI_URL_PREFIX,
                    first_letter=lib["name"][:1],
                    lib_name=lib["name"],
                    lib_version=lib["version"]
                )
            )
            with open(tgz_file_path, "wb") as tgz_file:
                tgz_file.write(download_stream.read())
            with open(tgz_file_path, "rb") as tar:
                if hashlib.md5(tar.read()).hexdigest() != lib["md5"]:
                    print("ERROR: Got wrong md5sum for {lib}.".format(lib=lib["name"]), file=stderr)
                    sys.exit(1)
            tar = tarfile.open(tgz_file_path)
            tar.extractall(path=SPARK_EC2_LIB_DIR)
            tar.close()
            os.remove(tgz_file_path)
            print(" - Finished downloading {lib}.".format(lib=lib["name"]))
        sys.path.insert(1, lib_dir)


# Only PyPI libraries are supported.
external_libs = [
    {
        "name": "boto",
        "version": "2.34.0",
        "md5": "5556223d2d0cc4d06dd4829e671dcecd"
    }
]

setup_external_libs(external_libs)

import boto
from boto.ec2.blockdevicemapping import BlockDeviceMapping, BlockDeviceType, EBSBlockDeviceType
from boto import ec2


class UsageError(Exception):
    pass

# Configure and parse our command-line arguments
def parse_args():
    parser = OptionParser(
        prog="spark-ec2",
        version="%prog {v}".format(v=SPARK_EC2_VERSION),
        usage="%prog [options] <action> <cluster_name>\n\n"
        + "<action> can be: login, stop, start, reboot-subordinates, get-main, get-urls, get-nodes")

    parser.add_option(
        "-i", "--identity-file", default="spark.pem",
        help="SSH private key file matching the AWS key (default: %default)")
    parser.add_option(
        "-p", "--profile", default=None,
        help="If you have multiple profiles (AWS or boto config), you can use " +
             "a specific profile by using this option (default: %default)")
    parser.add_option(
        "-r", "--region", default="us-east-1",
        help="EC2 region used to find instances in (default: %default)")
    parser.add_option(
        "-v", "--spark-version", default=DEFAULT_SPARK_VERSION,
        help="Version of Spark to use: 'X.Y.Z' or a specific git hash (default: %default)")
    parser.add_option(
        "--deploy-root-dir",
        default=None,
        help="A directory to copy into / on the first main. " +
             "Must be absolute. Note that a trailing slash is handled as per rsync: " +
             "If you omit it, the last directory of the --deploy-root-dir path will be created " +
             "in / before copying its contents. If you append the trailing slash, " +
             "the directory is not created and its contents are copied directly into /. " +
             "(default: %default).")
    parser.add_option(
        "--hadoop-major-version", default="yarn",
        help="Major version of Hadoop. Valid options are 1 (Hadoop 1.0.4), 2 (CDH 4.2.0), yarn " +
             "(Hadoop 2.4.0) (default: %default)")
    parser.add_option(
        "--swap", metavar="SWAP", type="int", default=1024,
        help="Swap space to set up per node, in MB (default: %default)")
    parser.add_option(
        "--main-opts", type="string", default="",
        help="Extra options to give to main through SPARK_MASTER_OPTS variable " +
             "(e.g -Dspark.worker.timeout=180)")
    parser.add_option(
        "--vpc-id", default=None,
        help="VPC to search for instances in. (default: search all vpcs)")

    (opts, args) = parser.parse_args()
    if len(args) != 2:
        parser.print_help()
        sys.exit(1)
    (action, cluster_name) = args

    # Boto config check
    # http://boto.cloudhackers.com/en/latest/boto_config_tut.html
    home_dir = os.getenv('HOME')
    if home_dir is None or not os.path.isfile(home_dir + '/.boto'):
        if not os.path.isfile('/etc/boto.cfg'):
            # If there is no boto config, check aws credentials
            if not os.path.isfile(home_dir + '/.aws/credentials'):
                if os.getenv('AWS_ACCESS_KEY_ID') is None:
                    print("ERROR: The environment variable AWS_ACCESS_KEY_ID must be set",
                          file=stderr)
                    sys.exit(1)
                if os.getenv('AWS_SECRET_ACCESS_KEY') is None:
                    print("ERROR: The environment variable AWS_SECRET_ACCESS_KEY must be set",
                          file=stderr)
                    sys.exit(1)
    return (opts, action, cluster_name)

def validate_spark_hadoop_version(spark_version, hadoop_version):
    if "." in spark_version:
        parts = spark_version.split(".")
        if parts[0].isdigit():
            spark_major_version = float(parts[0])
            if spark_major_version > 1.0 and hadoop_version != "yarn":
              print("Spark version: {v}, does not support Hadoop version: {hv}".
                    format(v=spark_version, hv=hadoop_version), file=stderr)
              sys.exit(1)
        else:
            print("Invalid Spark version: {v}".format(v=spark_version), file=stderr)
            sys.exit(1)

def get_validate_spark_version(version, repo):
    if "." in version:
        # Remove leading v to handle inputs like v1.5.0
        version = version.lstrip("v")
        if version not in VALID_SPARK_VERSIONS:
            print("Don't know about Spark version: {v}".format(v=version), file=stderr)
            sys.exit(1)
        return version
    else:
        github_commit_url = "{repo}/commit/{commit_hash}".format(repo=repo, commit_hash=version)
        request = Request(github_commit_url)
        request.get_method = lambda: 'HEAD'
        try:
            response = urlopen(request)
        except HTTPError as e:
            print("Couldn't validate Spark commit: {url}".format(url=github_commit_url),
                  file=stderr)
            print("Received HTTP response code of {code}.".format(code=e.code), file=stderr)
            sys.exit(1)
        return version


# Source: http://aws.amazon.com/amazon-linux-ami/instance-type-matrix/
# Last Updated: 2015-06-19
# For easy maintainability, please keep this manually-inputted dictionary sorted by key.
EC2_INSTANCE_TYPES = {
    "c1.medium":   "pvm",
    "c1.xlarge":   "pvm",
    "c3.large":    "hvm",
    "c3.xlarge":   "hvm",
    "c3.2xlarge":  "hvm",
    "c3.4xlarge":  "hvm",
    "c3.8xlarge":  "hvm",
    "c4.large":    "hvm",
    "c4.xlarge":   "hvm",
    "c4.2xlarge":  "hvm",
    "c4.4xlarge":  "hvm",
    "c4.8xlarge":  "hvm",
    "cc1.4xlarge": "hvm",
    "cc2.8xlarge": "hvm",
    "cg1.4xlarge": "hvm",
    "cr1.8xlarge": "hvm",
    "d2.xlarge":   "hvm",
    "d2.2xlarge":  "hvm",
    "d2.4xlarge":  "hvm",
    "d2.8xlarge":  "hvm",
    "g2.2xlarge":  "hvm",
    "g2.8xlarge":  "hvm",
    "hi1.4xlarge": "pvm",
    "hs1.8xlarge": "pvm",
    "i2.xlarge":   "hvm",
    "i2.2xlarge":  "hvm",
    "i2.4xlarge":  "hvm",
    "i2.8xlarge":  "hvm",
    "m1.small":    "pvm",
    "m1.medium":   "pvm",
    "m1.large":    "pvm",
    "m1.xlarge":   "pvm",
    "m2.xlarge":   "pvm",
    "m2.2xlarge":  "pvm",
    "m2.4xlarge":  "pvm",
    "m3.medium":   "hvm",
    "m3.large":    "hvm",
    "m3.xlarge":   "hvm",
    "m3.2xlarge":  "hvm",
    "m4.large":    "hvm",
    "m4.xlarge":   "hvm",
    "m4.2xlarge":  "hvm",
    "m4.4xlarge":  "hvm",
    "m4.10xlarge": "hvm",
    "r3.large":    "hvm",
    "r3.xlarge":   "hvm",
    "r3.2xlarge":  "hvm",
    "r3.4xlarge":  "hvm",
    "r3.8xlarge":  "hvm",
    "t1.micro":    "pvm",
    "t2.micro":    "hvm",
    "t2.small":    "hvm",
    "t2.medium":   "hvm",
    "t2.large":    "hvm",
}


def get_tachyon_version(spark_version):
    return SPARK_TACHYON_MAP.get(spark_version, "")


def get_existing_cluster(conn, opts, cluster_name, vpc_id=None, die_on_error=True):
    """
    Get the EC2 instances in an existing cluster if available.
    Returns a tuple of lists of EC2 instance objects for the mains and subordinates.
    """
    print("Searching for existing cluster {c} in region {r}...".format(
          c=cluster_name, r=opts.region))

    def get_instances(group_names):
        """
        Get all non-terminated instances that belong to any of the provided security groups.

        EC2 reservation filters and instance states are documented here:
            http://docs.aws.amazon.com/cli/latest/reference/ec2/describe-instances.html#options
        """
        filters={}
        filters['instance.group-name']=group_names
        if vpc_id is not None:
            filters["vpc_id"] = vpc_id
        reservations = conn.get_all_reservations(filters=filters)
        instances = itertools.chain.from_iterable(r.instances for r in reservations)
        return [i for i in instances if i.state not in ["shutting-down", "terminated"]]

    main_instances = get_instances([cluster_name + "-main"])
    subordinate_instances = get_instances([cluster_name + "-subordinates"])

    if any((main_instances, subordinate_instances)):
        print("Found {m} main{plural_m}, {s} subordinate{plural_s}.".format(
              m=len(main_instances),
              plural_m=('' if len(main_instances) == 1 else 's'),
              s=len(subordinate_instances),
              plural_s=('' if len(subordinate_instances) == 1 else 's')))

    if not main_instances and die_on_error:
        print("ERROR: Could not find a main for cluster {c} in region {r}.".format(
              c=cluster_name, r=opts.region), file=sys.stderr)
        sys.exit(1)

    return (main_instances, subordinate_instances)


# Deploy configuration files and run setup scripts on a newly launched
# or started EC2 cluster.
def setup_cluster(conn, main_nodes, subordinate_nodes, opts):
    main = main_nodes[0].public_dns_name

    modules = ['spark', 'ephemeral-hdfs', 'persistent-hdfs',
               'mapreduce', 'spark-standalone', 'tachyon', 'rstudio', 'ganglia']

    if opts.hadoop_major_version == "1":
        modules = list(filter(lambda x: x != "mapreduce", modules))

    # Clear SPARK_WORKER_INSTANCES if running on YARN
    if opts.hadoop_major_version == "yarn":
        opts.worker_instances = ""

    # NOTE: We should clone the repository before running deploy_files to
    # prevent ec2-variables.sh from being overwritten
    print("Cloning spark-ec2 scripts from {r}/tree/{b} on main...".format(
        r=DEFAULT_SPARK_EC2_GITHUB_REPO, b=DEFAULT_SPARK_EC2_BRANCH))
    ssh(
        host=main,
        opts=opts,
        command="rm -rf spark-ec2"
        + " && "
        + "git clone {r} -b {b} spark-ec2".format(r=DEFAULT_SPARK_EC2_GITHUB_REPO,
                                                  b=DEFAULT_SPARK_EC2_BRANCH)
    )

    print("Deploying files to main...")
    deploy_files(
        conn=conn,
        root_dir=SPARK_EC2_DIR + "/" + "deploy.generic",
        opts=opts,
        main_nodes=main_nodes,
        subordinate_nodes=subordinate_nodes,
        modules=modules
    )

    if opts.deploy_root_dir is not None:
        print("Deploying {s} to main...".format(s=opts.deploy_root_dir))
        deploy_user_files(
            root_dir=opts.deploy_root_dir,
            opts=opts,
            main_nodes=main_nodes
        )

    print("Running setup on main...")
    setup_spark_cluster(main, opts)
    print("Done!")


def setup_spark_cluster(main, opts):
    ssh(main, opts, "chmod u+x spark-ec2/setup.sh")
    ssh(main, opts, "spark-ec2/setup.sh")
    print("Spark standalone cluster started at http://%s:8080" % main)
    print("Ganglia started at http://%s:5080/ganglia" % main)


def is_ssh_available(host, opts, print_ssh_output=True):
    """
    Check if SSH is available on a host.
    """
    s = subprocess.Popen(
        ssh_command(opts) + ['-t', '-t', '-o', 'ConnectTimeout=3',
                             '%s@%s' % (DEFAULT_USER, host), stringify_command('true')],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT  # we pipe stderr through stdout to preserve output order
    )
    cmd_output = s.communicate()[0]  # [1] is stderr, which we redirected to stdout

    if s.returncode != 0 and print_ssh_output:
        # extra leading newline is for spacing in wait_for_cluster_state()
        print(textwrap.dedent("""\n
            Warning: SSH connection error. (This could be temporary.)
            Host: {h}
            SSH return code: {r}
            SSH output: {o}
        """).format(
            h=host,
            r=s.returncode,
            o=cmd_output.strip()
        ))

    return s.returncode == 0


def is_cluster_ssh_available(cluster_instances, opts):
    """
    Check if SSH is available on all the instances in a cluster.
    """
    for i in cluster_instances:
        if not is_ssh_available(host=i.public_dns_name, opts=opts):
            return False
    else:
        return True


def wait_for_cluster_state(conn, opts, cluster_instances, cluster_state):
    """
    Wait for all the instances in the cluster to reach a designated state.

    cluster_instances: a list of boto.ec2.instance.Instance
    cluster_state: a string representing the desired state of all the instances in the cluster
           value can be 'ssh-ready' or a valid value from boto.ec2.instance.InstanceState such as
           'running', 'terminated', etc.
           (would be nice to replace this with a proper enum: http://stackoverflow.com/a/1695250)
    """
    sys.stdout.write(
        "Waiting for cluster to enter '{s}' state.".format(s=cluster_state)
    )
    sys.stdout.flush()

    start_time = datetime.now()
    num_attempts = 0

    while True:
        time.sleep(5 * num_attempts)  # seconds

        for i in cluster_instances:
            i.update()

        max_batch = 100
        statuses = []
        for j in xrange(0, len(cluster_instances), max_batch):
            batch = [i.id for i in cluster_instances[j:j + max_batch]]
            statuses.extend(conn.get_all_instance_status(instance_ids=batch))

        if cluster_state == 'ssh-ready':
            if all(i.state == 'running' for i in cluster_instances) and \
               all(s.system_status.status == 'ok' for s in statuses) and \
               all(s.instance_status.status == 'ok' for s in statuses) and \
               is_cluster_ssh_available(cluster_instances, opts):
                break
        else:
            if all(i.state == cluster_state for i in cluster_instances):
                break

        num_attempts += 1

        sys.stdout.write(".")
        sys.stdout.flush()

    sys.stdout.write("\n")

    end_time = datetime.now()
    print("Cluster is now in '{s}' state. Waited {t} seconds.".format(
        s=cluster_state,
        t=(end_time - start_time).seconds
    ))


# Get number of local disks available for a given EC2 instance type.
def get_num_disks(instance_type):
    # Source: http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/InstanceStorage.html
    # Last Updated: 2015-06-19
    # For easy maintainability, please keep this manually-inputted dictionary sorted by key.
    disks_by_instance = {
        "c1.medium":   1,
        "c1.xlarge":   4,
        "c3.large":    2,
        "c3.xlarge":   2,
        "c3.2xlarge":  2,
        "c3.4xlarge":  2,
        "c3.8xlarge":  2,
        "c4.large":    0,
        "c4.xlarge":   0,
        "c4.2xlarge":  0,
        "c4.4xlarge":  0,
        "c4.8xlarge":  0,
        "cc1.4xlarge": 2,
        "cc2.8xlarge": 4,
        "cg1.4xlarge": 2,
        "cr1.8xlarge": 2,
        "d2.xlarge":   3,
        "d2.2xlarge":  6,
        "d2.4xlarge":  12,
        "d2.8xlarge":  24,
        "g2.2xlarge":  1,
        "g2.8xlarge":  2,
        "hi1.4xlarge": 2,
        "hs1.8xlarge": 24,
        "i2.xlarge":   1,
        "i2.2xlarge":  2,
        "i2.4xlarge":  4,
        "i2.8xlarge":  8,
        "m1.small":    1,
        "m1.medium":   1,
        "m1.large":    2,
        "m1.xlarge":   4,
        "m2.xlarge":   1,
        "m2.2xlarge":  1,
        "m2.4xlarge":  2,
        "m3.medium":   1,
        "m3.large":    1,
        "m3.xlarge":   2,
        "m3.2xlarge":  2,
        "m4.large":    0,
        "m4.xlarge":   0,
        "m4.2xlarge":  0,
        "m4.4xlarge":  0,
        "m4.10xlarge": 0,
        "r3.large":    1,
        "r3.xlarge":   1,
        "r3.2xlarge":  1,
        "r3.4xlarge":  1,
        "r3.8xlarge":  2,
        "t1.micro":    0,
        "t2.micro":    0,
        "t2.small":    0,
        "t2.medium":   0,
        "t2.large":    0,
    }
    if instance_type in disks_by_instance:
        return disks_by_instance[instance_type]
    else:
        print("WARNING: Don't know number of disks on instance type %s; assuming 1"
              % instance_type, file=stderr)
        return 1


# Deploy the configuration file templates in a given local directory to
# a cluster, filling in any template parameters with information about the
# cluster (e.g. lists of mains and subordinates). Files are only deployed to
# the first main instance in the cluster, and we expect the setup
# script to be run on that instance to copy them to other nodes.
#
# root_dir should be an absolute path to the directory with the files we want to deploy.
def deploy_files(conn, root_dir, opts, main_nodes, subordinate_nodes, modules):
    active_main = main_nodes[0].public_dns_name

    num_disks = get_num_disks(opts.instance_type)
    hdfs_data_dirs = "/mnt/ephemeral-hdfs/data"
    mapred_local_dirs = "/mnt/hadoop/mrlocal"
    spark_local_dirs = "/mnt/spark"
    if num_disks > 1:
        for i in range(2, num_disks + 1):
            hdfs_data_dirs += ",/mnt%d/ephemeral-hdfs/data" % i
            mapred_local_dirs += ",/mnt%d/hadoop/mrlocal" % i
            spark_local_dirs += ",/mnt%d/spark" % i

    cluster_url = "%s:7077" % active_main

    if "." in opts.spark_version:
        # Pre-built Spark deploy
        spark_v = get_validate_spark_version(opts.spark_version, DEFAULT_SPARK_GITHUB_REPO)
        validate_spark_hadoop_version(spark_v, opts.hadoop_major_version)
        tachyon_v = get_tachyon_version(spark_v)
    else:
        # Spark-only custom deploy
        spark_v = "%s|%s" % (DEFAULT_SPARK_GITHUB_REPO, opts.spark_version)
        tachyon_v = ""

    if tachyon_v == "":
      print("No valid Tachyon version found; Tachyon won't be set up")
      modules.remove("tachyon")

    main_addresses = [i.public_dns_name for i in main_nodes]
    subordinate_addresses = [i.public_dns_name for i in subordinate_nodes]
    worker_instances_str = "%d" % opts.worker_instances if opts.worker_instances else ""
    template_vars = {
        "main_list": '\n'.join(main_addresses),
        "active_main": active_main,
        "subordinate_list": '\n'.join(subordinate_addresses),
        "cluster_url": cluster_url,
        "hdfs_data_dirs": hdfs_data_dirs,
        "mapred_local_dirs": mapred_local_dirs,
        "spark_local_dirs": spark_local_dirs,
        "swap": str(opts.swap),
        "modules": '\n'.join(modules),
        "spark_version": spark_v,
        "tachyon_version": tachyon_v,
        "hadoop_major_version": opts.hadoop_major_version,
        "spark_worker_instances": worker_instances_str,
        "spark_main_opts": opts.main_opts
    }
    try:
        cred=open("hadoop.aws")
        for line in cred:
            # quietly ignore misformatted lines
            try:
                token, value = line.partition('=')[::2]
                if token == 'AWS_ACCESS_KEY_ID':
                    template_vars["aws_access_key_id"] = value.strip()
                elif token == 'AWS_SECRET_ACCESS_KEY':
                    template_vars["aws_secret_access_key"] = value.strip()
            except:
                pass
    except Exception as e:
        print((e), file=stderr)
        sys.exit(1)

    # Create a temp directory in which we will place all the files to be
    # deployed after we substitue template parameters in them
    tmp_dir = tempfile.mkdtemp()
    for path, dirs, files in os.walk(root_dir):
        if path.find(".svn") == -1:
            dest_dir = os.path.join('/', path[len(root_dir):])
            local_dir = tmp_dir + dest_dir
            if not os.path.exists(local_dir):
                os.makedirs(local_dir)
            for filename in files:
                if filename[0] not in '#.~' and filename[-1] != '~':
                    dest_file = os.path.join(dest_dir, filename)
                    local_file = tmp_dir + dest_file
                    with open(os.path.join(path, filename)) as src:
                        with open(local_file, "w") as dest:
                            text = src.read()
                            for key in template_vars:
                                text = text.replace("{{" + key + "}}", template_vars[key])
                            dest.write(text)
                            dest.close()
    # rsync the whole directory over to the main machine
    command = [
        'rsync', '-rv',
        '-e', stringify_command(ssh_command(opts)),
        "%s/" % tmp_dir,
        "%s@%s:/" % (DEFAULT_USER, active_main)
    ]
    subprocess.check_call(command)
    # Remove the temp directory we created above
    shutil.rmtree(tmp_dir)


# Deploy a given local directory to a cluster, WITHOUT parameter substitution.
# Note that unlike deploy_files, this works for binary files.
# Also, it is up to the user to add (or not) the trailing slash in root_dir.
# Files are only deployed to the first main instance in the cluster.
#
# root_dir should be an absolute path.
def deploy_user_files(root_dir, opts, main_nodes):
    active_main = main_nodes[0].public_dns_name
    command = [
        'rsync', '-rv',
        '-e', stringify_command(ssh_command(opts)),
        "%s" % root_dir,
        "%s@%s:/" % (DEFAULT_USER, active_main)
    ]
    subprocess.check_call(command)


def stringify_command(parts):
    if isinstance(parts, str):
        return parts
    else:
        return ' '.join(map(pipes.quote, parts))


def ssh_args(opts):
    parts = ['-o', 'StrictHostKeyChecking=no']
    parts += ['-o', 'UserKnownHostsFile=/dev/null']
    if opts.identity_file is not None:
        parts += ['-i', opts.identity_file]
    return parts


def ssh_command(opts):
    return ['ssh'] + ssh_args(opts)


# Run a command on a host through ssh, retrying up to five times
# and then throwing an exception if ssh continues to fail.
def ssh(host, opts, command):
    tries = 0
    while True:
        try:
            return subprocess.check_call(
                ssh_command(opts) + ['-t', '-t', '%s@%s' % (DEFAULT_USER, host),
                                     stringify_command(command)])
        except subprocess.CalledProcessError as e:
            if tries > 5:
                # If this was an ssh failure, provide the user with hints.
                if e.returncode == 255:
                    raise UsageError(
                        "Failed to SSH to remote host {0}.\n"
                        "Please check that you have provided the correct --identity-file "
                        "parameter and try again.".format(host))
                else:
                    raise e
            print("Error executing remote command, retrying after 30 seconds: {0}".format(e),
                  file=stderr)
            time.sleep(30)
            tries = tries + 1


# Backported from Python 2.7 for compatiblity with 2.6 (See SPARK-1990)
def _check_output(*popenargs, **kwargs):
    if 'stdout' in kwargs:
        raise ValueError('stdout argument not allowed, it will be overridden.')
    process = subprocess.Popen(stdout=subprocess.PIPE, *popenargs, **kwargs)
    output, unused_err = process.communicate()
    retcode = process.poll()
    if retcode:
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = popenargs[0]
        raise subprocess.CalledProcessError(retcode, cmd, output=output)
    return output


def ssh_read(host, opts, command):
    return _check_output(
        ssh_command(opts) + ['%s@%s' % (DEFAULT_USER, host), stringify_command(command)])


def ssh_write(host, opts, command, arguments):
    tries = 0
    while True:
        proc = subprocess.Popen(
            ssh_command(opts) + ['%s@%s' % (DEFAULT_USER, host), stringify_command(command)],
            stdin=subprocess.PIPE)
        proc.stdin.write(arguments)
        proc.stdin.close()
        status = proc.wait()
        if status == 0:
            break
        elif tries > 5:
            raise RuntimeError("ssh_write failed with error %s" % proc.returncode)
        else:
            print("Error {0} while executing remote command, retrying after 30 seconds".
                  format(status), file=stderr)
            time.sleep(30)
            tries = tries + 1

# Find out id any nodes are running
def nodes_running(instances):
    for inst in instances:
        if inst.state not in ["shutting-down", "terminated", "stopped"]:
            return True
    return False

def real_main():
    (opts, action, cluster_name) = parse_args()

    # Input parameter validation
    spark_v = get_validate_spark_version(opts.spark_version, DEFAULT_SPARK_GITHUB_REPO)
    validate_spark_hadoop_version(spark_v, opts.hadoop_major_version)

    if opts.identity_file is not None:
        if not os.path.exists(opts.identity_file):
            print("ERROR: The identity file '{f}' doesn't exist.".format(f=opts.identity_file),
                  file=stderr)
            sys.exit(1)

        file_mode = os.stat(opts.identity_file).st_mode
        if not (file_mode & S_IRUSR) or not oct(file_mode)[-2:] == '00':
            print("ERROR: The identity file must be accessible only by you.", file=stderr)
            print('You can fix this with: chmod 400 "{f}"'.format(f=opts.identity_file),
                  file=stderr)
            sys.exit(1)

    # Prevent breaking ami_prefix (/, .git and startswith checks)
    # Prevent forks with non spark-ec2 names for now.
    if DEFAULT_SPARK_EC2_GITHUB_REPO.endswith("/") or \
            DEFAULT_SPARK_EC2_GITHUB_REPO.endswith(".git") or \
            not DEFAULT_SPARK_EC2_GITHUB_REPO.startswith("https://github.com") or \
            not DEFAULT_SPARK_EC2_GITHUB_REPO.endswith("spark-ec2"):
        print("spark-ec2-git-repo must be a github repo and it must not have a trailing / or .git. "
              "Furthermore, we currently only support forks named spark-ec2.", file=stderr)
        sys.exit(1)

    if not (opts.deploy_root_dir is None or
            (os.path.isabs(opts.deploy_root_dir) and
             os.path.isdir(opts.deploy_root_dir) and
             os.path.exists(opts.deploy_root_dir))):
        print("--deploy-root-dir must be an absolute path to a directory that exists "
              "on the local file system", file=stderr)
        sys.exit(1)

    try:
        if opts.profile is None:
            conn = ec2.connect_to_region(opts.region)
        else:
            conn = ec2.connect_to_region(opts.region, profile_name=opts.profile)
    except Exception as e:
        print((e), file=stderr)
        sys.exit(1)

    vpc_id=opts.vpc_id

    if action == "login":
        (main_nodes, subordinate_nodes) = get_existing_cluster(conn, opts, cluster_name, vpc_id)
        if not nodes_running(main_nodes):
            print("Cluster " + cluster_name + " does not appear to have any running main nodes.")
        else:
            if not main_nodes[0].public_dns_name:
                main = main_nodes[0].ip_address
            else:
                main = main_nodes[0].public_dns_name
            print("Logging into main " + main + "...")
            subprocess.check_call(
                ssh_command(opts) + ['-t', '-t', "%s@%s" % (DEFAULT_USER, main)])

    elif action == "reboot-subordinates":
        (main_nodes, subordinate_nodes) = get_existing_cluster(
            conn, opts, cluster_name, vpc_id, die_on_error=False)
        if not nodes_running(subordinate_nodes):
            print("Cluster " + cluster_name + " does not appear to have any running subordinate nodes.")
        else:
            response = raw_input(
                "Are you sure you want to reboot the cluster " +
                cluster_name + " subordinates?\n" +
                "Reboot cluster subordinates " + cluster_name + " (y/N): ")
            if response == "y":
                print("Rebooting subordinates...")
                for inst in subordinate_nodes:
                    if inst.state not in ["shutting-down", "terminated", "stopped"]:
                        print("Rebooting " + inst.id)
                        inst.reboot()

    elif action == "get-main":
        (main_nodes, subordinate_nodes) = get_existing_cluster(conn, opts, cluster_name, vpc_id)
        if not nodes_running(main_nodes):
            print("Cluster " + cluster_name + " does not appear to have any running main nodes.")
        else:
            print(main_nodes[0].public_dns_name)

    elif action == "get-nodes":
        (main_nodes, subordinate_nodes) = get_existing_cluster(conn, opts, cluster_name, vpc_id)
        for i in main_nodes:
            if i.public_dns_name:
                name=i.public_dns_name
            else:
                name=i.private_ip_address
            print("Main {m} is {s}".format(m=name,s=i.state))
        for i in subordinate_nodes:
            if i.public_dns_name:
                name=i.public_dns_name
            else:
                name=i.private_ip_address
            print("Subordinate {m} is {s}".format(m=name,s=i.state))

    elif action == "get-urls":
        (main_nodes, subordinate_nodes) = get_existing_cluster(conn, opts, cluster_name, vpc_id)
        if not nodes_running(main_nodes):
            print("Cluster " + cluster_name + " does not appear to have any running main nodes.")
        else:
            if not main_nodes[0].public_dns_name:
                main=main_nodes[0].ip_address
            else:
                main = main_nodes[0].public_dns_name
            print("Spark standalone cluster is at http://%s:8080" % main)
            print("HDFS console is at http://%s:50070" % main)
            print("Ganglia is at http://%s:5080/ganglia" % main)

    elif action == "stop":
        (main_nodes, subordinate_nodes) = get_existing_cluster(
            conn, opts, cluster_name, vpc_id, die_on_error=False)
        if not nodes_running(main_nodes + subordinate_nodes):
            print("Cluster " + cluster_name + " has no running nodes.")
        else:
            response = raw_input(
                "Are you sure you want to stop the cluster " +
                cluster_name + "?\nDATA ON EPHEMERAL DISKS WILL BE LOST, " +
                "BUT THE CLUSTER WILL KEEP USING SPACE ON\n" +
                "AMAZON EBS IF IT IS EBS-BACKED!!\n" +
                "All data on spot-instance subordinates will be lost.\n" +
                "Stop cluster " + cluster_name + " (y/N): ")
            if response == "y":
                print("Stopping main...")
                for inst in main_nodes:
                    if inst.state not in ["shutting-down", "terminated"]:
                        inst.stop()
                print("Stopping subordinates...")
                for inst in subordinate_nodes:
                    if inst.state not in ["shutting-down", "terminated"]:
                        if inst.spot_instance_request_id:
                            inst.terminate()
                        else:
                            inst.stop()

    elif action == "start":
        (main_nodes, subordinate_nodes) = get_existing_cluster(conn, opts, cluster_name, vpc_id)
        print("Starting subordinates...")
        for inst in subordinate_nodes:
            if inst.state not in ["shutting-down", "terminated"]:
                inst.start()
        print("Starting main...")
        for inst in main_nodes:
            if inst.state not in ["shutting-down", "terminated"]:
                inst.start()
        wait_for_cluster_state(
            conn=conn,
            opts=opts,
            cluster_instances=(main_nodes + subordinate_nodes),
            cluster_state='ssh-ready'
        )

        # Determine types of running instances
        existing_main_type = main_nodes[0].instance_type
        existing_subordinate_type = subordinate_nodes[0].instance_type
        # Setting opts.main_instance_type to the empty string indicates we
        # have the same instance type for the main and the subordinates
        if existing_main_type == existing_subordinate_type:
            existing_main_type = ""
        opts.main_instance_type = existing_main_type
        opts.instance_type = existing_subordinate_type

        setup_cluster(conn, main_nodes, subordinate_nodes, opts)

    else:
        print("Invalid action: %s" % action, file=stderr)
        sys.exit(1)


def main():
    try:
        real_main()
    except UsageError as e:
        print("\nError:\n", e, file=stderr)
        sys.exit(1)


if __name__ == "__main__":
    logging.basicConfig()
    main()
