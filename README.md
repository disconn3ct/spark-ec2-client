# spark-ec2-client
Client for managing the EC2 Spark clusters. It allows for the cluster to be stopped, started, rebooted or ssh'd into and can also provide information on a cluster such as the master IP, ganglia and spark URLs, etc.

This software is Apache licenced as it is largely drawn from https://github.com/amplab/spark-ec2

## Configuration:
### AWS Credentials
This script uses the `boto` library, so you need to have your individual credentials configured correctly. If you can run `aws ec2 describe-instances --dry-run` and receive `Request would have succeeded`, you are good to go.

If not, install the AWS commandline tools (`brew install awscli`) and run `aws configure`. It will ask for the credentials supplied by your administrator.

More documentation on the AWS configuration is available at [cloudhackers.com](http://boto.cloudhackers.com/en/latest/boto_config_tut.html)

### Hadoop Credentials
In order to allow the cluster to access S3, it uses separate credentials. These are refreshed on the nodes every time the cluster is restarted.

Create a local file with the cluster AWS credentials called `hadoop.aws`:

    AWS_ACCESS_KEY_ID=AKIAAAAAAAAAAAAAAAAA
    AWS_SECRET_ACCESS_KEY=1111111111111111+22222222222222222222222

(Replace the above examples with the cluster credentials you were given. These are **not** the credentials used above but are specific to the cluster.)
### SSH Key
When the cluster is created, an SSH key is generated. In order to log in to the master via ssh, you should put that key into `spark.pem`. Without that file, you will not be able to complete most start/stop tasks or log into the master.

### Cluster Configuration
The `templates/` directory holds configuration files that are pushed out each time the cluster is started. This is where you can make any necessary changes to the cluster configuration. Several variables are available:

    {{master_list}}
    {{active_master}}
    {{slave_list}}
    {{zoo_list}}
    {{cluster_url}}
    {{hdfs_data_dirs}}
    {{mapred_local_dirs}}
    {{spark_local_dirs}}
    {{spark_worker_mem}}
    {{spark_worker_instances}}
    {{spark_worker_cores}}
    {{spark_master_opts}}
    {{aws_access_key_id}}
    {{aws_secret_access_key}}

These strings will be replaced prior to the files being pushed to the nodes.

## Usage

The first time you run the client, it will install any necessary libraries.

If you run it without a command or a cluster name (for example, `./spark-client`) it will print a usage page with all possible options.

### Start Cluster
`./spark-client start CLUSTERNAME`

This will take 5-10 minutes, as it runs system updates and then pushes out the requested versions of Spark, Hadoop, etc. It also configures the AWS credentials and populates (or creates) the ephemeral and persistant HDFS volumes based on the administrator repository.

This will produce a lot of output as it runs. If there are any issues with the cluster, saving this output will make it easier to diagnose.

The basic process it follows is:
* Start the nodes (if they are not already running)
* Wait for the nodes to be ready for ssh (booted, updated, etc)
* Run the initial setup to ensure any version or configuration changes are applied:
    * Deploy files from the local `deploy.generic` directory
    * Update any changed template files
    * Clone the administrative repository on the master
    * Run the setup script from the administrative repository to set up hdfs, etc

### Stop Cluster
`./spark-client stop CLUSTERNAME`

This checks to see if any nodes in the cluster are running and stops them. Ephemeral-hdfs data WILL BE LOST, but persistant data will remain until the next start.

This is a fast operation, as it simply sends the AWS console shutdown command.

### Get Cluster Node Status
`./spark-client get-nodes CLUSTERNAME`

This will print the list of nodes and their status (running, stopped, shutting-down, terminated). If you are not sure whether a cluster is running or stopped, you can check the status with this command.

### Get Web URLs
`./spark-client get-urls CLUSTERNAME`

Print the Ganglia and Spark URLs for a running cluster.
(If the cluster master is not running, you will receive an error.)
