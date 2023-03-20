# Description of working with Cluster on AWS

## Prerequisities: flintrock
Install flintrock by `pip install flintrock`




Step

1. Create yaml file (This is a configuration file, ask for a copy of yaml file)
    * To customize the arguments to suit the personal environment
        1. identity-file: ~/.ssh/flintrock.pem (ask for private pem file)
        2. make sure the region is defined as 'en-north-1'
        3. ami is defined by Jiaji from previous work
2. Create flintrock.pem under folder ~./ssh/
3. Change the access permission by `chmod 400 flintrock.pem` such that pem file is read-only by the owner
4. (Optional) Check the caller identity by `aws sts get-caller-identity`
5. Launch cluster 

    `flintrock --config /path/to/FILENAME.yaml launch CLUSTERNAME`

    According to the configuration, we will get the instances. 
    
    After you have launched cluster, there is a url automatically generated. Copy this url for future use. An example url looks like this: 

    `ec2-13-53-123-112.eu-north-1.compute.amazonaws.com` 

6. Copy scripts from local to cluster

    `flintrock --config /path/to/FILENAME.yaml copy-file CLUSTERNAME /source_path/to/FILENAME_TO_BE_COPIED /target_path/to/` (The target_path/to/ refers to the path on cluster, e.g. `/home/ec2-user/`)

7. Login in
    `flintrock --config /path/to/FILENAME.yaml login CLUSTERNAME` 

    After having logged in cluster, you can submit a job. Here is an example snippet how the command looks like:

    `./bin/spark-submit --master spark://CLUSTER_URL:7077 --packages org.apache.hadoop:hadoop-aws:3.2.0 /path/to/script_file`

    During running time, you can use the browser to monitor the process of job completion. 

    `CLUSTER_URL:8080` with port of 8080, you have an overview of the cluster information. 
    Click `APPLICATION ID`, you will have the information of application, e.g. if the application is successfully completed. 
    `CLUSTER_URL:4040` with port of 4040, you have a detailed information for running jobs. Click `Stages`, you will updated information on running jobs. When there is no running jobs, this port is not avilable. 
    
    You can check the detailed error information in shell. The jobs are supposed to be successfully completed without complaint. 


8. Destroy a cluster 
    
    It costs when running a cluster. We need to destroy the cluster after the work is done.
    
    `flintrock --config /path/to/FILENAME.yaml destroy CLUSTERNAME` 
    
    The command above is supposed to be run on your local shell.

    To double check the state of instances, you can login to aws console. Through EC2 instances, you can see the state from running to shutting down to terminated. 
    
    Make sure you login with the place located at Stockholm if you do not find the instances you created. 

9. Happy clustering!


