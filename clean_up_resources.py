import configparser
import boto3
from AWS import prettyRedshiftProps


def cleanUp(iam, redshift, clusterIdentifier, roleName):
    '''
        Description: This function cleans the resources exploited earlier.
        
        Arguments:
            iam: iam boto3 client
            redshift: redshift boto3 client
            clusterIdentifier: the identifier of the cluster exploited earlier
            roleName: the role used during the work with the cluster
            
        Returns:
            None
    '''
    try:
        while True: 
            prompt = input('Do you want to clean up the resources?') 
            answer = prompt[0].lower() 
            if prompt == '' or not answer in ['y','n']: 
                print('Please answer with yes or no!') 
            else: 
                if answer == 'y':
                    print('Deleting the cluster...')
                    redshift.delete_cluster( ClusterIdentifier=clusterIdentifier, SkipFinalClusterSnapshot=True)
                    iam.detach_role_policy(RoleName=roleName, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
                    iam.delete_role(RoleName=roleName)
                    break
    
                if answer == 'n': 
                    break
    
    # Waiting for the cluster to be deleted
        import time
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=clusterIdentifier)['Clusters'][0]
        pr = prettyRedshiftProps(myClusterProps)
        while pr.loc[pr['Key'] == 'ClusterStatus', 'Value'].item() == 'deleting':
            pr = prettyRedshiftProps(redshift.describe_clusters(ClusterIdentifier=clusterIdentifier)['Clusters'][0])
            time.sleep(10)
    except Exception as e:
        print("Cluster has been deleted")
    
    
def main():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')
    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","dwh_cluster_identifier")
    
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
    
    cleanUp(iam, redshift, DWH_CLUSTER_IDENTIFIER, DWH_IAM_ROLE_NAME)
    
if __name__ == "__main__":
    main()