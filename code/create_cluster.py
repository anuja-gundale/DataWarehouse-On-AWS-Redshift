import pandas as pd
import boto3
import json
from botocore.exceptions import ClientError
import configparser
import time

def create_resource(KEY,SECRET,RESOURCE):
    '''
    Creates resource AWS resource
    '''
        conn_rs = boto3.resource(RESOURCE,
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )
        return conn_rs

def create_client(KEY,SECRET,CLIENT):
    '''
    Creates AWS client
    '''
        conn_cl = boto3.client(CLIENT,aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name='us-west-2'
                  )
        return conn_cl


def create_iamrole(DWH_IAM_ROLE_NAME,iam):
    '''
    Creates IAM Role for Redshift, to allow it to use AWS services
    '''       
    try:
        print("1.1 Creating a new IAM Role") 
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
    except Exception as e:
        print(e)
    
    print("1.2 Attaching Policy")
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']
    print("1.3 Get the IAM role ARN")
    
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    print(roleArn)
    return roleArn



def create_cluster(DWH_CLUSTER_TYPE,DWH_NODE_TYPE,DWH_NUM_NODES,DWH_DB,DWH_CLUSTER_IDENTIFIER,DWH_DB_USER,DWH_DB_PASSWORD,roleArn,redshift):
    '''
    Creates Redshift cluster
    '''
    try:
        
        response = redshift.create_cluster(        
            #HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
        
            #Roles (for s3 access)
            IamRoles=[roleArn]  
        )
    except Exception as e:
        
        print(e)

        
def get_cluster_props(redshift, DWH_CLUSTER_IDENTIFIER):
    '''
    Retrieve Redshift clusters properties
    '''

    def prettyRedshiftProps(props):
        pd.set_option('display.max_colwidth', -1)
        keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
        x = [(k, v) for k,v in props.items() if k in keysToShow]
        return pd.DataFrame(data=x, columns=["Key", "Value"])

    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    prettyRedshiftProps(myClusterProps)

    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)
    return myClusterProps, DWH_ENDPOINT, DWH_ROLE_ARN


def create_securitygrp(ec2,myClusterProps,DWH_PORT):
    '''
    Creates security group 
    '''
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
    )
    except Exception as e:
        print(e)
    
    

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")

    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

    (DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

    pd.DataFrame({"Param":
                  ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
              "Value":
                  [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
             })

    ec2 = create_resource(KEY,SECRET,'ec2')
    s3 =  create_resource(KEY,SECRET,'s3')
    iam =  create_client(KEY,SECRET,'iam')
    redshift =  create_client(KEY,SECRET,'redshift')
    
    roleArn = create_iamrole(DWH_IAM_ROLE_NAME,iam)
    #roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    
    create_cluster(DWH_CLUSTER_TYPE,DWH_NODE_TYPE,DWH_NUM_NODES,DWH_DB,DWH_CLUSTER_IDENTIFIER,DWH_DB_USER,DWH_DB_PASSWORD,roleArn,redshift)
    
    time.sleep(160)
    
    myClusterProps1 = get_cluster_props(redshift, DWH_CLUSTER_IDENTIFIER)
    
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        
    create_securitygrp(ec2,myClusterProps,DWH_PORT)
    
if __name__ == "__main__":
    main()