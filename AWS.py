#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import boto3
import json
import configparser
import psycopg2
from botocore.exceptions import ClientError

# Configuration and global variables

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")
DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")
DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")

DWH_ENDPOINT           = config.get("CLUSTER","HOST")
DWH_DB                 = config.get("CLUSTER","DB_NAME")
DWH_DB_USER            = config.get("CLUSTER","DB_USER")
DWH_DB_PASSWORD        = config.get("CLUSTER","DB_PASSWORD")
DWH_PORT               = config.get("CLUSTER","DB_PORT")

pd.DataFrame({"Param":
                  ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
              "Value":
                  [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
             })


ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )

s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                   )

iam = boto3.client('iam', 
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET,
                   region_name='us-west-2'
                  )

redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

UdacityDbBucket = s3.Bucket("udacity-dend")


def exists(role_name):
    '''
    Description: The function checks if the named AWS role exists
    Arguments: 
        role_name: AWS role name;
    Ruturns: 
        True/False response
    '''
    try:
        iam.get_role(RoleName=role_name)
        return True
    except iam.exceptions.NoSuchEntityException:
        return False


# Create IAM Role function

def createRole():
    '''
    Description: this function creates IAM role to work with Redshift cluster further.
    Arguments:
        None
    Returns:
        None
    '''
    
    try:
        print("Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )    
    
    
    
        print("Attaching Policy")

        iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode']

        print("Get the IAM role ARN")
        roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

        print(roleArn)
        
        config.set('DWH','DWH_IAM_ROLE_ARN', roleArn)
        with open('dwh.cfg', 'w') as configfile:
            config.write(configfile)
    
    except Exception as e:
        print(e)


# # Redshift Cluster functions


def prettyRedshiftProps(props):
    '''
    Description: the function transforms cluster properties into the dataframe. Used later to describe the cluster
    
    Arguments: 
        props: the set of cluster properties from the describe_clusters function performed on Redshift client
    
    Returns: 
        dataframe with key-value pairs of the parameters
    '''
    try:
        pd.set_option('display.max_colwidth', -1)
        keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
        x = [(k, v) for k,v in props.items() if k in keysToShow]
        pr = pd.DataFrame(data=x, columns=["Key", "Value"])
    except Exception as e:
        print ('Exception' + ' ' + e)
    return pr

def createCluster():
    '''
    Description: the function creates the cluster.
        There are 3 main parts in it:
            1. Create the cluster;
            2. It checks the status of the cluster and waits until it is in 'Available' status;
            3. It updates config file dwh.cfg and displays cluster endpoint and role arn.
    
    Arguments:
        None
    
    Returns:
        None
    '''
    try:
        # 1. Create the cluster
        print('Creating the cluster')
        response = redshift.create_cluster(        
            
        # 1.1. Set hardware
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

        # 1.2. Sets identifiers & credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
        
        #1.3. Set roles (for s3 access)
            IamRoles=[iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']]  
        )
        print("The cluster {} is being created. Please wait...".format(DWH_CLUSTER_IDENTIFIER))
        
        # 2. Wait until the cluster is created
        import time
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        pr = prettyRedshiftProps(myClusterProps)
        while pr.loc[pr['Key'] == 'ClusterStatus', 'Value'].item() != 'available':
            pr = prettyRedshiftProps(redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0])
            time.sleep(10)
        print("The cluster {} is created".format(DWH_CLUSTER_IDENTIFIER))
        config.set("CLUSTER","HOST", redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['Endpoint']['Address'])
        config.set("DWH", "dwh_iam_role_arn", redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['IamRoles'][0]['IamRoleArn'])
        with open('dwh.cfg', 'w') as configfile:
                config.write(configfile)
        
        DWH_ENDPOINT = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['Endpoint']['Address']
        DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
        
        print(DWH_ENDPOINT)
        
        # Add configurations 
        
        
    except Exception as e:
        print(e)


def openPort():
    '''
    Description: the function opens an incoming TCP port to access the cluster ednpoint. No incoming and outcoming parameters
    
    Arguments:
        None
        
    Returns:
        None
    '''
    try:
        vpc = ec2.Vpc(id=redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except ClientError as err:
        if err.response['Error']['Code'] == 'InvalidPermission.Duplicate':
            print("Permission already exists")
    
    except Exception as e:
        print(e)

def main():
    
    # Check IAM role
    if (not exists(DWH_IAM_ROLE_NAME)):
        createRole()
    else:
        print ('The role already exists')
    
    # Create cluster
    if (len(redshift.describe_clusters()['Clusters']) == 0):
        createCluster()
    else:
        print('The cluster already exists')

    # Open port
    print('Open port')
    openPort()
    

if __name__ == "__main__":
    main()


