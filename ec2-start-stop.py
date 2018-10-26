import boto3
import logging
import time
import datetime
import ast

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def action_on_instances(client_method, instance_ids, action):
    """
    Start or stop the instances based on client_method
    :param client_method: Boto method to start or stop instances.
    :param instance_ids: A list of instances ids.
    :param action: A string with the action. Mostly for logging reasons.
    """
    try:
        if instance_ids:
            logger.info('{}ing the following instances'.format(action))
            logger.info(instance_ids)
            client_method(InstanceIds=instance_ids)
            logger.info('{} has finished successfully'.format(action))
        else:
            logger.info('No action was taken because one of the following:')
            logger.info('1 - Instances have "NoShutdown" flag.')
            logger.info('2 - Instances do not have "Schedule" tag.')
            logger.info('3 - Instances have no value in "Schedule" tag.')
    except Exception as error:
        logger.error('{} failed with the following output : {}'.format(action, error))

def stop_untagged_instances(untagged_instance_ids, temporary_user):
    """
    Stop the untagged instances
    :param untagged_instance_ids: A list of all the untagged instances.
    :param temporary_user: The boto user that will be used to stop the untagged instances
    """
    try:
        logger.info('Stopping the untagged instances : ')
        logger.info(untagged_instance_ids)
        temporary_user.stop_instances(InstanceIds=untagged_instance_ids)
    except Exception as error:
        logger.info('The instances failed to stop with the following error : {}'.format(error))

def categorise_instances(data, config, temporary_user):
    """
    Sort instance ids to those have need to be started/stopped and to those that need to be left as is.
    :param data: A disctionary with information about the instances
    :param config: A disctionary with the values for `allDay` and `halfDay`
    :param temporary_user: The boto user that will be used to stop the untagged instances
    :return action_required: List of instance ids that have to be either stopped or started
    :return no_action_required: List of instance ids that should not be tempered with (`NoShutdown` tag or Schedule tag with [`allDay`] value is applied to these instances)
    """
    try:
        if data:
            action_required = []
            no_action_required = []
            untagged_instances = []
            for reservation in data['Reservations']:
                tag_list = []
                for instance in reservation['Instances']:
                    for tag in instance['Tags']:
                        tag_list.append(tag['Key'])
                    if 'NoShutdown' in tag_list:
                        no_action_required.append(reservation['Instances'][0]['InstanceId'])
                    elif 'Schedule' in tag_list:
                        for tag in instance['Tags']:
                            if 'Schedule' in tag['Key']:
                                if tag['Value'] == '':
                                    no_action_required.append(reservation['Instances'][0]['InstanceId'])
                                elif tag['Value'] == config['schedule']['allDay']:
                                    no_action_required.append(reservation['Instances'][0]['InstanceId'])
                                elif tag['Value'] == config['schedule']['halfDay']:
                                    action_required.append(reservation['Instances'][0]['InstanceId'])
                    else:
                        if 'True' in config['stop_untagged_instances']:
                            untagged_instances.append(instance['InstanceId'])
                            stop_untagged_instances(untagged_instances, temporary_user)
            return action_required, no_action_required
    except Exception as error:
        logger.info("Categorisation of instances according to tags failed with this error : {}".format(error))



def get_instance_ids(temporary_user, config, state, now, tz):
    """
    Get the instance ids depending on the state given.
    :param temporary_user: A boto user to describe instances.
    :param config: A disctionary with the configuration values that needs to be passed to categorise_instances() method.
    :param state: The state of the instances the user should describe. It will either be `stopped` or `running` based on the time the lambda will run
    :param tz: The current timezone
    :return action_required: List of instance ids that have to be either stopped or started
    :return no_action_required: List of instance ids that should not be tempered with (`NoShutdown` tag or Schedule tag with [`allDay`] value is applied to these instances)
    """
    try:
        data = temporary_user.describe_instances(Filters=[{'Name':'instance-state-name', 'Values': [state]}])
        logger.info("The date is : {} , {}".format(now.strftime("%A, %d %B %Y %H:%M:%S"), tz))

        action_required, no_action_required = categorise_instances(data, config, temporary_user)
        return action_required, no_action_required
    except Exception as error:
        logger.info("Describing the instances failed with the following error : {}".format(error))

def start_stop(now, start, stop, temporary_user, config, tz):
    """
    Depending on the time of the day, this method will get the appropriate instance ids and serve them to action_on_instances() to either be stopped or started.
    now.time() is used here to be compared with start and stop time() objects
    :param now: The current time.
    :param start: The time when the instances should be started i.e 7 am.
    :param stop: The time when the instances should be stopped i.e 7 pm
    :param temporary_user: A boto user to make the appropriate calls
    :param config: A disctionary with the configuration values that needs to be passed to get_instance_ids() method.
    :tz: Timezone of the server running the lambda
    """
    if now.time() >= start and now.time() < stop:
        action_required_ids, no_action_required_ids = get_instance_ids(temporary_user, config, 'stopped', now, tz)
        action_on_instances(temporary_user.start_instances, action_required_ids, 'Start')
    elif now.time() >= stop:
        action_required_ids, no_action_required_ids = get_instance_ids(temporary_user, config, 'running', now, tz)
        action_on_instances(temporary_user.stop_instances, action_required_ids, 'Stop')

def convert_to_datetime(config):
    """
    Convert the config values for the start/stop time of the instances to datetime.time() object so they can be comparable.
    :param config: A disctionary with the values for `startTime` and `stopTime`
    :return start_up_time: A time object for when the instances should start. It it of datetime.time() object
    :return stop_time: A time object for when the instances should stop. It it of datetime.time() object
    :return now: A datetime object for the current date. It it of datetime.datetime() object
    :tz: The current timezone
    """
    try:
        current_time = datetime.datetime.now()
        now, tz = is_dst(datetime.datetime.now())
        startTimeHour, startTimeMinute = config['startTime'].split(',')
        stopTimeHour, stopTimeMinute = config['stopTime'].split(',')
        start_up_time = datetime.time(int(startTimeHour), int(startTimeMinute))
        stop_time = datetime.time(int(stopTimeHour), int(stopTimeMinute))
        return start_up_time, stop_time, now, tz
    except Exception as error:
        logger.info("Converting config values to time objects failed with the following error : {}".format(error))

def is_dst(now):
    """
    Checks out if the current time and date is on DST. If is it on DST 1 hour is added to the current time.
    :param now: The current time
    :return now: The currnent time as datetime.datetime() object
    :return tz: The timezone of the current time and date
    """
    dst_start_date = datetime.datetime(now.year, 4, 1)
    dst_end_date = datetime.datetime(now.year, 11, 1)
    dston = dst_start_date - datetime.timedelta(days=dst_start_date.weekday() + 1)
    dstoff = dst_end_date - datetime.timedelta(days=dst_end_date.weekday() + 1)

    if dston <= now.replace(tzinfo=None) < dstoff:
        tz = "GMT +1"
        return now + datetime.timedelta(hours=1), tz
    else:
        tz = "GMT"
        return now, tz

def json_to_dict(s3_user):
    """
    Read the config the S3 Bucket (a json file), and convert it into a dictionary.
    :param s3_user: A boto user that will make the call to read the file.
    :return ast.literal_eval(s3_config): The content of the json file converted to dictionary
    """
    try:
        s3_config = s3_user.get_object(Bucket="hermes-sharedservices-data", Key="Lambdas/start-stop/config.json")['Body'].read()
        logger.info('Fetching config file..')
        return ast.literal_eval(s3_config)
    except Exception as error:
        logger.info("Reading the config from S3 failed with the following error : {}".format(error))

def create_temp_user(client, role_arn):
    """
    Create an ec2 user to make the appropriate calls.
    :param client: A boto client to assume a role.
    :param role_arn: The arn of the role that will be assumed.
    :return ec2_user: A boto user to make the appropriate calls.
    """
    try:
        response = client.assume_role(
            RoleArn=role_arn,
            RoleSessionName="Lambda-Start-Stop-functionality"
        )
        ec2_user = boto3.client(
            'ec2',
            aws_access_key_id=response['Credentials']['AccessKeyId'],
            aws_secret_access_key=response['Credentials']['SecretAccessKey'],
            aws_session_token=response['Credentials']['SessionToken']
        )
        return ec2_user
    except Exception as error:
        logger.info("Creating a temporary ec2 privileged user failed with the following error : {}".format(error))

def is_weekday(day, halfDay):
    """
    Depending on the halfDay value, decide whether the lambda should continue running.
    Example: In `12x5` value, 5 represents the days so the lambda will run on weekdays.
    Example: In `12x4` value, 4 represents the days so the lambda will run until Thursday.
    :param day: The current day
    :param halfDay: How many days this should run depending on the last digit.
    :return True/False: Depending on the current day.
    """
    hours, days = halfDay.split('x')
    if day <= int(days)-1:
        return True
    else:
        return False

def fetch_config_from_s3(client):
    """
    Fetch the config file from S3
    :param client: A boto client to assume the role that will download the config.
    :return json_to_dict(fetcher): The contents of the json file in a dictionary format
    """
    try:
        response = client.assume_role(
            RoleArn="arn:aws:iam::548760365095:role/Ec2StartStopLambdaActionRole",
            RoleSessionName="Ec2-Start-Stop-Lambda-Session-Role"
        )
        fetcher = boto3.client(
            's3',
            aws_access_key_id=response['Credentials']['AccessKeyId'],
            aws_secret_access_key=response['Credentials']['SecretAccessKey'],
            aws_session_token=response['Credentials']['SessionToken']
        )
        return json_to_dict(fetcher)
    except Exception as error:
        logger.info("Creating a temporary S3 privileged user failed with the following error : {}".format(error))

def assume_role():
    """
    Return a boto client.
    :return boto3.client('sts'): Create and return an STS boto client
    """
    try:
        return boto3.client('sts')
    except Exception as error:
        logger.info("Creating a boto client failed with the following error : {}".format(error))


def lambda_handler(event, context):
    """
    Trigger function for lambda
    """
    try:
        day = datetime.datetime.now().weekday()

        fetcher = assume_role()
        config = fetch_config_from_s3(fetcher)
        print config

        if is_weekday(day, config['schedule']['halfDay']):
            client = assume_role()
            for role_arn in config['role_arns']:
                account_number = role_arn.split(":")[4]
                ec2_user = create_temp_user(client, role_arn)

                start_up_time, stop_time, now, tz = convert_to_datetime(config['times'])
                logger.info("Lambda started for account : {}".format(config['account_names'][account_number]))
                start_stop(now, start_up_time, stop_time, ec2_user, config, tz)
        else:
            logger.info("I do not operate on weekends.")
    except Exception as error:
        logger.info("Lambda failed to run with the following error : {}".format(error))

