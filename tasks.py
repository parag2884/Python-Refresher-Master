from utilities import utils
from common.utils import Utils
from xml.etree.ElementTree import parse
from django.conf import settings
from dataextraction.cleansing.common import Strips
from celery.exceptions import MaxRetriesExceededError
from celery.utils.log import get_task_logger
from celery_webui import app
from ftplib import FTP
import celery
from indexing import IDCFolderCreation
from common.client_name import ClientName
import traceback
import os
import sys
import json
import requests

sys.path.append(settings.BASE_DIR)

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings")

configPath = settings.XML_TREE_PATH
tree = parse(configPath)

logger = get_task_logger(__name__)
cls_strip = Strips.clsStrips()
iqbot_json_url = settings.COMMON_CONTROLROOM_URL + "aari/v2/"
trigger_url = iqbot_json_url + "requests/"


class DataExtractionDocumentPending(Exception):
    # """Raised when the IQ Bot response is not downloaded."""
    pass


class BaseTask(celery.Task):

    def on_failure(self, exc, task_id, args, kwargs, info):
        logger.error(
            'Cleansing Task failed! Task ID:{0!r} : Error Description: {1!r} Detailed Error {2!r}'.format(
                task_id,
                exc,
                info))

    def before_start(self, task_id, args, kwargs):
        print('Before Start never called until trace on')

    # def after_return(self, status, retval, task_id, args, kwargs, info):
    #     print('Task {0!r} failed: {1!r} args: {2!r} kwargs: {3!r} info {4!r}'.format(task_id, exc,args,kwargs,info))

    def on_success(self, retval, task_id, args, kwargs):
        logger.info(
            'Cleansing Task {0!r} Completed successfully.'.format(task_id))

    def on_retry(self, exc, task_id, args, kwargs, info):
        logger.warning(
            'Cleansing Task {0!r} triggered because of {1!r} '.format(
                task_id, info))


def log_info(task_id, document_id, information):
    logger.info(
        'Cleansing Task : Celery Task ID: {0!r} Document ID: {1!r} information {2!r}'.format(
            task_id,
            document_id,
            information))


def log_general_info(information):
    logger.info('Cleansing Task : Details {0!r}'.format(information))


def log_warning(task_id, document_id, information):
    logger.warning(
        'Cleansing Task : Celery Task ID: {0!r} Document ID: {1!r} information {2!r}'.format(
            task_id, document_id, information))


def log_error(information):
    logger.error('Cleansing Task :' + information)


def log_system_error(e):
    e_type, value, tb = sys.exc_info()
    traceback.format_exception(e_type, value, tb)[-2:]
    logger.error('Cleansing Task,  Exception Detail:' + str(e), exc_info=True)


def update_document_for_error(document, error_step, error_response):
    if error_step in ['started', 'document_uploaded', 'de_progress']:
        document.execution_step = 'error_datacleansing'
    else:
        document.execution_step = 'error_de'
    document.step1_response = error_response
    document.save()
    document.update_test_env_status()


def update_sub_document_for_error(sub_doc, error_step, error_response):
    sub_doc.is_error = True
    sub_doc.error_step = error_step
    sub_doc.error_response = error_response
    if error_step in ['document_uploaded', 'de_progress']:
        sub_doc.execution_step = 'data_cleansing_not_implemented'
    if error_step == 'data_cleansing_progress' or error_step == 'completed':
        sub_doc.execution_step = 'not_completed'

    sub_doc.save(False)
    sub_doc.update_test_env_status()

    if sub_doc.document.sub_documents.exclude(
            execution_step__in=[
                'completed',
                'not_completed',
                'data_cleansing_not_implemented']).count() == 0:
        total_sub_doc = sub_doc.document.sub_documents.count()
        if sub_doc.document.sub_documents.filter(
                execution_step='not_completed').count() > 0:
            sub_doc.document.execution_step = 'error_de'
        elif (sub_doc.document.sub_documents.filter(execution_step='data_cleansing_not_implemented').count() > 0
              or total_sub_doc == sub_doc.document.sub_documents.filter(execution_step='data_cleansing_not_implemented')
                                            .count()):
            sub_doc.document.execution_step = 'de_completed'

    sub_doc.document.save()
    sub_doc.document.update_test_env_status()


@app.task
def clean_data(
        sub_doc,
        iq_bot_csv_path,
        output_path,
        doc_type,
        local_file,
        instance_master_id):
    from dataextraction.models import PostCleansing, InstanceMaster
    from documentanalyzer.models import DocumentReview
    import importlib
    log_general_info('================================')
    log_general_info(iq_bot_csv_path)
    log_general_info(output_path)
    log_general_info(local_file)
    log_general_info('================================')
    instance_master = InstanceMaster.objects.get(id=instance_master_id)
    try:
        module = importlib.import_module("dataextraction.cleansing." +
                                         instance_master.module_name)
        obj_class = getattr(module, instance_master.class_name)
        file_name = os.path.basename(local_file)
        # Calling cleansing task's constructor
        obj_class(local_file, output_path, file_name)
    except Exception as e:
        log_system_error(e)
        update_sub_document_for_error(
            sub_doc,
            'completed',
            'Error occurred in data cleansing - ' +
            str(e))
        raise
    op = os.path.join(output_path, os.path.basename(
        local_file).replace('.json', '').replace('.xml', ''))

    if instance_master.instance_id or sub_doc.document_type.name == 'Fraud Report':
        file_ext = '.xml'
    else:
        file_ext = '.json'
    op = op + file_ext
    sub_doc.dc_file = op
    sub_doc.save(False)
    if file_ext == '.json':
        with open(sub_doc.dc_file, "r") as read_file:
            sub_doc.flattened_json = json.load(read_file)
    else:
        csv_file_path = sub_doc.dc_file.replace('.xml', '.csv')
        sub_doc.flattened_json = Utils.get_non_tabular_json(csv_file_path)
    try:
        review_document = DocumentReview.objects.get(subdocument=sub_doc)
        post_cleansing_process = PostCleansing.objects.get(
            organizationprocess=review_document.organizationprocess,
            document_type=sub_doc.document_type)
    except BaseException:
        post_cleansing_process = None
    if post_cleansing_process:
        post_cleansing_process_module = importlib.import_module(
            "dataextraction.postcleansing." + post_cleansing_process.module_name)
        post_cleansing_obj = getattr(
            post_cleansing_process_module,
            cls_strip.getAlphaNumeric_without_space(
                post_cleansing_process.class_name))
        post_cleansing_obj(sub_doc, review_document.organizationprocess)
    sub_doc.execution_step = 'completed'
    sub_doc.error_response = 'Success'
    sub_doc.save(False)
    if sub_doc.document.sub_documents.exclude(
            execution_step__in=[
                'completed',
                'not_completed',
                'data_cleansing_not_implemented']).count() == 0:
        sub_doc.document.execution_step = 'de_completed'
        sub_doc.document.save()
        if sub_doc.document.is_test_doc:
            sub_doc.send_test_files_testenv()


@app.task(bind=True,
          name='IQBotCleansing:schedule_sub_documents_for_data_extraction_IQBot',
          default_retry_delay=30,
          max_retries=2)
def schedule_sub_documents_for_data_extraction_iq_bot(
        self, sub_doc_id=None, document_type=None, document_path=None):
    from uploader.models import SubDocument
    from dataextraction.models import InstanceMaster
    log_general_info(
        "calling:schedule_sub_documents_for_data_extraction_IQBot")

    try:
        sub_doc = SubDocument.objects.get(pk=sub_doc_id)
    except SubDocument.DoesNotExist:
        if self.request.retries < self.max_retries:
            logger.warning(
                "SubDocument with ID {} does not exist. Retrying...".format(sub_doc_id))
            self.retry()
        else:
            logger.error(
                "SubDocument with ID {} does not exist. Max retries exceeded.".format(sub_doc_id))
        return

    logger.info(
        "Executing schedule_sub_documents_for_data_extraction_iq_bot for SubDocument ID: {}".format(sub_doc_id))

    # Check if file size is within the limit
    de_file_size_limit = Utils.get_configuration_value(
        key='DE_FILE_SIZE_LIMIT')
    file_size = os.path.getsize(document_path)
    if file_size > int(de_file_size_limit) * 1024 * 1024:
        logger.error("Size exceeds the limit allowed ({} MB).".format(
            de_file_size_limit))
        update_sub_document_for_error(
            sub_doc,
            'completed',
            'Pdf file size exceeds the limit allowed ({} MB).'.format(
                de_file_size_limit))
        return

    file_name_to_download = os.path.basename(sub_doc.pdf_url.path)

    if document_type == "Fraud Report":
        if ".html" not in sub_doc.pdf_url.path.lower():
            error_message = 'Data cleansing is implemented only for the HTML version of this document'
            update_sub_document_for_error(
                sub_doc, 'document_uploaded', error_message)
            logger.error(error_message)
            return

        output_path = sub_doc.pdf_url.path.replace(file_name_to_download, "")

        try:
            instance_master = InstanceMaster.objects.get(
                document_type__name=sub_doc.document_type.name,
                organization=sub_doc.document.organization
            )

            clean_data(
                sub_doc,
                sub_doc.pdf_url.path,
                output_path,
                document_type,
                file_name_to_download,
                instance_master.id
            )
        except InstanceMaster.DoesNotExist:
            error_message = "InstanceMaster with document type '{}' and organization '{}' does not exist.".format(
                sub_doc.document_type.name, sub_doc.document.organization)
            update_sub_document_for_error(
                sub_doc,
                'completed',
                'An error occurred in data cleansing: {}'.format(error_message))
            logger.error(error_message)
            return
        except Exception as e:
            error_message = 'An error occurred in data cleansing: {}'.format(
                str(e))
            update_sub_document_for_error(sub_doc, 'completed', error_message)
            logger.error(error_message)
            raise

    else:
        instance_master = InstanceMaster.objects.filter(
            document_type__name=document_type,
            organization=sub_doc.document.organization,
            is_active=True
        ).last()

        if not instance_master:
            instance_master = InstanceMaster.objects.filter(
                document_type__name=document_type,
                organization__name='MOZAIQ',
                is_active=True
            ).last()

        if not instance_master:
            update_sub_document_for_error(sub_doc, 'de_progress', '')
            return

        upload_sub_documents_for_data_extraction_iq_bot.apply_async(
            args=[sub_doc_id, document_type,
                  document_path, instance_master.id],
            countdown=60,
            retry=True
        )


@app.task(bind=True,
          name='IQBotCleansing:upload_sub_documents_for_data_extraction_IQBot',
          default_retry_delay=30,
          max_retries=2)
def upload_sub_documents_for_data_extraction_iq_bot(
        self,
        sub_doc_id=None,
        document_type=None,
        document_path=None,
        instance_id=None):
    from uploader.models import SubDocument
    from dataextraction.models import InstanceMaster
    sub_doc = SubDocument.objects.get(pk=sub_doc_id)
    instance_master = InstanceMaster.objects.get(id=instance_id)
    if instance_master.instance_id:
        # Todo: Enhancements.
        if Utils.check_pdf_file(document_path):
            document_path = Utils.get_enhanced_pdfs(
                document_path)
        from common.singletons import SingletonDoubleChecked
        singleton_instance = SingletonDoubleChecked.instance()
        try:
            iq_bot_token = singleton_instance.get_IQBot_token()
        except Exception as e:
            self.retry()
            iq_bot_token = None
        if sub_doc.execution_step == 'de_progress':
            download_csv_for_data_extraction_iq_bot.apply_async(
                args=[
                    sub_doc_id,
                    document_path,
                    iq_bot_token,
                    None,
                    instance_id],
                countdown=60,
                retry=True)
        elif sub_doc.execution_step in ['started', 'document_uploaded']:
            if iq_bot_token:
                register_file_url = settings.COMMON_CONTROLROOM_URL + 'cognitive/v3/'
                # Checking if learning instance is available.
                url = register_file_url + 'learninginstances/name/' + \
                    instance_master.instance_name + "?enabled_fields=true"
                log_general_info(url)
                headers = {
                    'x-authorization': iq_bot_token
                }

                response = requests.request("GET", url, headers=headers)
                log_general_info(response.json())
                if 'message' in response.json().keys():
                    upload_sub_documents_for_data_extraction_iq_bot_csv.apply_async(
                        args=[
                            document_path,
                            iq_bot_token,
                            instance_id,
                            sub_doc_id],
                        countdown=60,
                        retry=True)
                else:
                    url = iqbot_json_url + "processes/publish"
                    source_uri = f"{settings.COMMON_CONTROLROOM_SOURCE_URL}{iqbot_folder_name}/{iqbot_folder_name}"\
                        .replace(" ", "%20")

                    payload = json.dumps({
                        "sourceUri": source_uri,
                        "workspace": "PUBLIC"
                    })
                    headers = {
                        'x-authorization': iq_bot_token,
                        'Content-Type': 'text/plain'
                    }

                    response = requests.request(
                        "POST", url, headers=headers, data=payload)
                    log_general_info(response.json())

                    if 'message' in response.json().keys():
                        update_sub_document_for_error(
                            sub_doc, 'data_cleansing_progress', response.json()['message'])
                        self.retry()
                    else:
                        # Registering IQ Bot Files
                        url = register_file_url + 'files'

                        payload = json.dumps({
                            "filename": file_name_to_download,
                            "repositoryReference": {
                                "uri": source_uri,
                                "workspace": "PUBLIC"
                            }
                        })
                        headers = {
                            'x-authorization': iq_bot_token,
                            'Content-Type': 'application/json'
                        }

                        response = requests.request(
                            "PUT", url, headers=headers, data=payload)
                        log_general_info(response.json())
                        if 'message' in response.json().keys():
                            self.retry()
                        else:
                            # File Upload
                            upload_url = settings.COMMON_CONTROLROOM_URL + \
                                "storage/v1/file/" + \
                                response.json()['fileAccessToken']

                            log_general_info(upload_url)
                            input_file_path = response.json(
                            )['fileReference']['string']
                            files = [
                                ('upload', (file_name_to_download, open(
                                    sub_doc.pdf_url.path, 'rb'), 'application/pdf'))]

                            log_general_info(
                                singleton_instance.verify_IQBot_token(iq_bot_token))
                            headers = {
                                'x-authorization': iq_bot_token
                            }

                            response = requests.request(
                                "POST", upload_url, headers=headers, files=files)

                            log_general_info(response.json())
                            if 'message' in response.json().keys():
                                update_sub_document_for_error(
                                    sub_doc, 'data_cleansing_progress', response.json()['message'])
                            else:
                                sub_doc.execution_step = 'document_uploaded'
                                sub_doc.pdf_page_count = Utils.get_pdf_page_count(
                                    document_path)
                                sub_doc.save(False)

                                # Trigger the bot to download response

                                url = trigger_url + "create"

                                request_uri = f"{settings.COMMON_CONTROLROOM_SOURCE_URL}{iqbot_folder_name}/" \
                                    f"{iqbot_folder_name}".replace(
                                        " ", "%20")

                                payload = json.dumps({
                                    "teamId": None,
                                    "repositoryReference": {
                                        "uri": request_uri,
                                        "workspace": "PUBLIC"
                                    },
                                    "inputs": {
                                        "LearningInstanceName": {
                                            "string": iqbot_folder_name
                                        },
                                        "InputFile": {
                                            "type": "FILE",
                                            "string": input_file_path
                                        },
                                        "InputFileName": {
                                            "string": file_name_to_download
                                        },
                                        "OutputFolder": {
                                            "string": settings.IQBOT_CSV_PATH + "/" + str(settings.ENVIRONMENT)
                                        }
                                    }
                                })
                                headers = {
                                    'x-authorization': iq_bot_token,
                                    'Content-Type': 'text/plain'
                                }
                                response = requests.request(
                                    "POST", url, headers=headers, data=payload)

                                log_general_info(response.json())

                                if 'message' in response.json().keys():
                                    update_sub_document_for_error(
                                        sub_doc, 'data_cleansing_progress', response.json()['message'])
                                else:
                                    sub_doc.execution_step = 'de_progress'
                                    sub_doc.save(False)
                                    download_csv_for_data_extraction_iq_bot.apply_async(
                                        args=[
                                            sub_doc_id,
                                            document_path,
                                            iq_bot_token,
                                            response.json()['ref'],
                                            instance_id],
                                        countdown=60,
                                        retry=True)
            else:
                update_sub_document_for_error(
                    sub_doc, 'data_cleansing_progress', 'No auth token.')
                self.retry()
    else:
        schedule_sub_documents_for_data_extraction_ms_form.apply_async(
            args=[
                sub_doc.pk,
                document_type,
                sub_doc.pdf_url.path,
                instance_id],
            countdown=5,
            retry=True)


import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from uploader.models import SubDocument
from common.singletons import SingletonDoubleChecked
from dataextraction.models import InstanceMaster
from common.utils import get_file_from_zip, log_info

logger = logging.getLogger(__name__)

@app.task(bind=True,
          autoretry_for=(DataExtractionDocumentPending,),
          name='IQBotCleansing:Downloading-Documents-From-IQBot',
          base=BaseTask,
          retry_backoff=60,
          max_retries=5,
          retry_jitter=False)
def download_csv_for_data_extraction_iq_bot(
        self,
        sub_doc_ids=None,
        iq_bot_token=None,
        request_ref=None,
        instance_id=None):
    
    def update_sub_document_for_error(sub_doc, execution_step, error_response):
        sub_doc.execution_step = execution_step
        sub_doc.error_response = error_response
        sub_doc.save(update_fields=['execution_step', 'error_response'])
        log_info(self.request.id, sub_doc.document.uid, f"Error: {error_response}")

    def process_sub_document(sub_doc_id):
        sub_doc = SubDocument.objects.get(pk=sub_doc_id)
        file_name_to_download = os.path.basename(sub_doc.document_path)
        document_type = str(sub_doc.document_type.name).replace(' ', '')

        if str(self.request.retries) == str(self.max_retries):
            update_sub_document_for_error(
                sub_doc,
                'data_cleansing_progress',
                'Max retries exceeded for the downloading output.')
            return

        if request_ref:
            url = trigger_url + "ref/" + request_ref + "?showHidden=false"
            payload = ""

            is_valid_iqbot_token = singleton_instance.verify_IQBot_token(iq_bot_token)
            log_info(self.request.id, sub_doc.document.uid, f"Valid IQBot token: {is_valid_iqbot_token}")

            if not is_valid_iqbot_token:
                iq_bot_token = singleton_instance.generate_IQBot_token()

            headers = {
                'x-authorization': iq_bot_token,
                'Accept': 'application/json'
            }

            response = requests.request(
                "GET", url, headers=headers, data=payload).json()

            log_info(self.request.id, sub_doc.document.uid, f"Response: {response}")

            if 'status' in response.keys():
                if response['status'].upper() in ('FAILED', 'ERROR'):
                    update_sub_document_for_error(
                        sub_doc,
                        'data_cleansing_progress',
                        response['info'])
                elif response['status'].upper() == 'OPENED' and len(response['steps']) > 1 and 'Validation' in response['steps'][1]['name']:
                    update_sub_document_for_error(
                        sub_doc,
                        'data_cleansing_progress',
                        'Document went into Validator.')
                elif response['status'].upper() == 'OPENED' and len(response['steps']) > 1 and 'Failed' in response['steps'][1]['name']:
                    update_sub_document_for_error(
                        sub_doc,
                        'data_cleansing_progress',
                        response['steps'][1]['name'])
                elif response['status'].upper() == 'SUCCESS':
                    url = iqbot_json_url + "tasks/bots/" + str(response['steps'][0]['id']) + "?showHidden=true"
                    payload = ""

                    is_valid_iqbot_token = singleton_instance.verify_IQBot_token(iq_bot_token)
                    log_info(self.request.id, sub_doc.document.uid, f"Valid IQBot token: {is_valid_iqbot_token}")

                    if not is_valid_iqbot_token:
                        iq_bot_token = singleton_instance.generate_IQBot_token()

                    headers = {
                        'x-authorization': iq_bot_token
                    }

                    response = requests.request(
                        "GET", url, headers=headers, data=payload).json()

                    log_info(self.request.id, sub_doc.document.uid, f"Response: {response}")

                    if response['status'].upper() == 'SUCCESS':
                        # Download the file
                        local_filename = os.path.join(
                            settings.IQBOT_CSV_PATH,
                            response['outputs']['ExtractionBotOutput']['record']['values'][0]['string'] +
                            "_" +
                            file_name_to_download.replace('.pdf', '.json'))

                        log_info(self.request.id, sub_doc.document.uid, f"Local filename: {local_filename}")
                        log_info(self.request.id, sub_doc.document.uid, "Data Cleansing Started")

                        sub_doc.de_file = local_filename
                        sub_doc.execution_step = 'data_cleansing_progress'
                        sub_doc.save(update_fields=['de_file', 'execution_step'])

                        clean_data(
                            sub_doc,
                            settings.IQBOT_CSV_PATH,
                            settings.OUTPUTPATH,
                            document_type,
                            local_filename,
                            None)
                    else:
                        raise DataExtractionDocumentPending(" Retry Count: " + str(self.request.retries))
            else:
                raise DataExtractionDocumentPending(" Retry Count: " + str(self.request.retries))
        else:
            instance_master = InstanceMaster.objects.get(id=instance_id)
            create_idc_files_directory()
            output_file_path = get_output_file_path(sub_doc, file_name_to_download)
            local_file_path = ""

            is_aws_iqbot_storage_enabled = Utils.get_configuration_value(key='AWS_IQBOT_STORAGE_ENABLED')
            if is_aws_iqbot_storage_enabled.lower() == 'true' and sub_doc.file_id is not None:
                file_name_to_download = file_name_to_download.replace(" ", "_")
                file_name_to_download = sub_doc.file_id + '-', file_name_to_download, '.csv'
                file_name_to_download = ''.join(file_name_to_download)

                iq_bot_output = get_s3_bucket_response(file_name_to_download, iq_bot_folder, 'Success', sub_doc)
                if "NoSuchKey" not in iq_bot_output.text:
                    log_info(self.request.id, sub_doc.document.uid, f"File Found: {file_name_to_download}")
                    new_file_name = cls_strip.cleanBeforeTexts(
                        sub_doc.file_id, file_name_to_download).lstrip("- ") + '.csv'
                    local_file_path = os.path.join(settings.IQBOT_CSV_PATH, new_file_name)

                    local_filename = open(local_file_path, 'wb')
                    local_filename.write(iq_bot_output.content)
                    local_filename.close()

                    sub_doc.execution_step = 'data_cleansing_progress'
                    sub_doc.save(update_fields=['execution_step'])

                    clean_data(
                        sub_doc,
                        settings.IQBOT_CSV_PATH,
                        output_file_path,
                        document_type,
                        local_file_path,
                        instance_master.instance_id)
                else:
                    file_name_to_download = file_name_to_download.replace(".csv", "")

                    iq_bot_output = get_s3_bucket_response(file_name_to_download, iq_bot_folder, 'Unclassified', sub_doc)
                    if "NoSuchKey" not in iq_bot_output.text:
                        log_info(self.request.id, sub_doc.document.uid, 'Unclassified Document.')
                        update_sub_document_for_error(sub_doc, 'data_cleansing_progress', 'Unclassified Document.')
                    else:
                        iq_bot_output = get_s3_bucket_response(file_name_to_download, iq_bot_folder, 'Invalid', sub_doc)
                        if "NoSuchKey" not in iq_bot_output.text:
                            log_info(self.request.id, sub_doc.document.uid, 'Invalid Document.')
                            update_sub_document_for_error(sub_doc, 'data_cleansing_progress', 'Invalid Document.')
                        else:
                            file_prefix = os.path.join('IQBotoutputPath', iq_bot_folder, 'Not Processed').replace("\\", "/")

                            iqbot_s3_resources = utils.get_iqbot_s3_bucket_resources()
                            iqbot_response = iqbot_s3_resources.list_objects(
                                Bucket=settings.AWS_IQ_BOT_STORAGE_BUCKET_NAME, Prefix=file_prefix)

                            for response_key in iqbot_response:
                                if response_key.lower() == 'contents':
                                    for key in iqbot_response['Contents']:
                                        if file_name_to_download in key['Key']:
                                            log_info(self.request.id, sub_doc.document.uid, 'New Group Created.')
                                            update_sub_document_for_error(
                                                sub_doc, 'data_cleansing_progress', 'New Group Created.')
                                            break

                    if sub_doc.error_response is None:
                        raise DataExtractionDocumentPending(" Retry Count: " + str(self.request.retries))

            else:
                file_url = os.path.join(settings.COMMON_IQBOT_DOWNLOAD_URL, instance_master.instance_id, 'files/archive').replace("\\", "/")
                is_valid_iqbot_token = singleton_instance.verify_IQBot_token(iq_bot_token)

                log_info(self.request.id, sub_doc.document.uid, f"Valid IQBot token: {is_valid_iqbot_token}")

                if not is_valid_iqbot_token:
                    iq_bot_token = singleton_instance.generate_IQBot_token()

                querystring = {"docStatus": "SUCCESS"}
                headers = {
                    'x-authorization': iq_bot_token,
                    'cache-control': "no-cache",
                }

                iq_bot_output = requests.get(url=file_url, headers=headers, params=querystring)

                if iq_bot_output.status_code == 200:
                    # For unzipping zip files
                    local_file_path = get_file_from_zip(
                        iq_bot_output.content,
                        sub_doc.file_id + '-' + file_name_to_download,
                        sub_doc.document.request_type)

                    if local_file_path != "":
                        log_info(self.request.id, sub_doc.document.uid, "Data Cleansing Started")
                        sub_doc.execution_step = 'data_cleansing_progress'
                        sub_doc.save(update_fields=['execution_step'])

                        log_info(self.request.id, sub_doc.document.uid, 'Checking in IQ Bot Success Folder')

                        clean_data(
                            sub_doc,
                            settings.IQBOT_CSV_PATH,
                            output_file_path,
                            document_type,
                            local_file_path,
                            instance_master.id)
                    else:
                        log_info(self.request.id, sub_doc.document.uid, f"Valid IQBot token: {is_valid_iqbot_token}")

                        if not is_valid_iqbot_token:
                            iq_bot_token = singleton_instance.generate_IQBot_token()

                        querystring = {"docStatus": "UNCLASSIFIED"}
                        headers = {
                            'x-authorization': iq_bot_token,
                            'cache-control': "no-cache",
                        }

                        log_info(self.request.id, sub_doc.document.uid, 'Checking in IQ Bot UNCLASSIFIED Folder')
                        file_url = file_url.replace('archive', 'list')

                        iq_bot_output = requests.get(url=file_url, headers=headers, params=querystring)

                        if iq_bot_output.status_code == 200:
                            if file_name_to_download in iq_bot_output.content.decode():
                                log_info(self.request.id, sub_doc.document.uid, 'Document Unclassified')
                                update_sub_document_for_error(sub_doc, 'data_cleansing_progress', 'Document Unclassified.')
                            else:
                                log_info(self.request.id, sub_doc.document.uid, f"Valid IQBot token: {is_valid_iqbot_token}")

                                if not is_valid_iqbot_token:
                                    iq_bot_token = singleton_instance.generate_IQBot_token()

                                querystring = {"docStatus": "INVALID"}
                                headers = {
                                    'x-authorization': iq_bot_token,
                                    'cache-control': "no-cache",
                                }

                                log_info(self.request.id, sub_doc.document.uid, 'Checking in IQ Bot INVALID Folder')

                                iq_bot_output = requests.get(url=file_url, headers=headers, params=querystring)

                                if iq_bot_output.status_code == 200:
                                    if file_name_to_download in iq_bot_output.content.decode():
                                        log_info(self.request.id, sub_doc.document.uid, 'Invalid Document')
                                        update_sub_document_for_error(sub_doc, 'data_cleansing_progress', 'Invalid Document.')
                                    else:
                                        log_info(self.request.id, sub_doc.document.uid, f"Valid IQBot token: {is_valid_iqbot_token}")

                                        if not is_valid_iqbot_token:
                                            iq_bot_token = singleton_instance.generate_IQBot_token()

                                        querystring = {"docStatus": "UNTRAINED"}
                                        headers = {
                                            'x-authorization': iq_bot_token,
                                            'cache-control': "no-cache",
                                        }

                                        log_info(self.request.id, sub_doc.document.uid, 'Checking in IQ Bot UNTRAINED Folder')

                                        iq_bot_output = requests.get(url=file_url, headers=headers, params=querystring)

                                        if iq_bot_output.status_code == 200:
                                            if file_name_to_download in iq_bot_output.content.decode():
                                                log_info(self.request.id, sub_doc.document.uid, 'New Group Created.')
                                                update_sub_document_for_error(
                                                    sub_doc, 'data_cleansing_progress', 'New Group Created.')
                                            else:
                                                log_info(self.request.id, sub_doc.document.uid, f"Valid IQBot token: {is_valid_iqbot_token}")

                                                if not is_valid_iqbot_token:
                                                    iq_bot_token = singleton_instance.generate_IQBot_token()

                                                querystring = {"docStatus": "VALIDATION"}
                                                headers = {
                                                    'x-authorization': iq_bot_token,
                                                    'cache-control': "no-cache",
                                                }

                                                log_info(self.request.id, sub_doc.document.uid, 'Checking in IQ Bot VALIDATION Folder')

                                                iq_bot_output = requests.get(url=file_url, headers=headers, params=querystring)

                                                if iq_bot_output.status_code == 200:
                                                    if file_name_to_download in iq_bot_output.content.decode():
                                                        log_info(self.request.id, sub_doc.document.uid, 'Document went into validator.')
                                                        update_sub_document_for_error(
                                                            sub_doc, 'data_cleansing_progress', 'Document went into validator.')
                                                    else:
                                                        raise DataExtractionDocumentPending(" Retry Count: " + str(self.request.retries))
                                                else:
                                                    raise DataExtractionDocumentPending(" Retry Count: " + str(self.request.retries))
                                            else:
                                                raise DataExtractionDocumentPending(" Retry Count: " + str(self.request.retries))
                                        else:
                                            raise DataExtractionDocumentPending(" Retry Count: " + str(self.request.retries))
                                    else:
                                        raise DataExtractionDocumentPending(" Retry Count: " + str(self.request.retries))
                                else:
                                    raise DataExtractionDocumentPending(" Retry Count: " + str(self.request.retries))
                            else:
                                raise DataExtractionDocumentPending(" Retry Count: " + str(self.request.retries))
                        else:
                            raise DataExtractionDocumentPending(" Retry Count: " + str(self.request.retries))
                    else:
                        log_info(self.request.id, file_name_to_download, 'file not Found:')
                        raise DataExtractionDocumentPending(" Retry Count: " + str(self.request.retries))

    # Create a ThreadPoolExecutor to process sub-documents concurrently
    with ThreadPoolExecutor() as executor:
        # Create a list to store the futures
        futures = []
        
        # Submit each sub-document for processing
        for sub_doc_id in sub_doc_ids:
            future = executor.submit(process_sub_document, sub_doc_id)
            futures.append(future)
        
        # Wait for all futures to complete
        for future in as_completed(futures):
            # Retrieve any exceptions raised during processing
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error occurred during processing: {str(e)}")
                # Handle the error and update the sub-document as necessary
                # For example, you can call a function to update the sub-document status
                # update_sub_document_for_error(sub_doc, 'data_cleansing_progress', str(e))




@app.task(bind=True,
          name='IQBotCleansing:Downloading-Documents-From-IQBot_FTP',
          default_retry_delay=30,
          max_retries=15)
def download_csv_for_data_extraction_iq_bot_ftp(
        self,
        sub_doc_id=None,
        document_type=None,
        document_path=None):
    ftp = FTP(settings.FTP_ADDRESS)
    status = ftp.login("deepanshu.soni", "Welcome@123$")
    ftp.set_pasv(False)
    file_name_to_download = os.path.basename(document_path)

    from uploader.models import SubDocument
    sub_doc = SubDocument.objects.get(pk=sub_doc_id)
    try:
        with open(settings.INSTANCE_MASTER_PATH) as json_file:
            data = json.load(json_file)
        iq_bot_folder = data['Instance'][0][document_type][0]['IQBotFolderName']
        cwd = iq_bot_folder + '/Success'
        ftp.cwd(cwd)
        listing = []
        ftp.retrlines("LIST", listing.append)
        files = (line.rsplit(None, 1)[1] for line in listing)
        log_info(self.request.id, file_name_to_download,
                 'File Name to Download:')
        filename = [i for i in listing if file_name_to_download in i]
        log_info(self.request.id, filename, 'File Name to Download:')

        if len(filename) > 0:
            log_info(self.request.id, file_name_to_download, 'File Found : ')
            new_filename = filename[0][filename[0].find('['):len(filename[0])]
            new_filename = new_filename[0][new_filename[0].find(
                ']_') + 2:len(new_filename[0])]
            # download the file
            local_filename = os.path.join(
                settings.IQBOT_CSV_PATH, new_filename)
            log_general_info(local_filename)
            log_general_info("-------------")
            lf = open(local_filename, "wb")
            ftp.retrbinary("RETR " + new_filename, lf.write, 1024)
            lf.close()
            log_general_info("Data Cleansing Started")
            sub_doc.de_file = local_filename
            sub_doc.extracted_data = open(local_filename, 'r').read()
            sub_doc.save(False)
            sub_doc.execution_step = 'data_cleansing_progress'
            sub_doc.save(False)
            clean_data(
                sub_doc,
                settings.IQBOT_CSV_PATH,
                settings.OUTPUTPATH,
                document_type,
                local_filename,
                None)
        else:
            log_info(self.request.id, file_name_to_download,
                     'File not found : ')
            try:
                self.retry()
            except MaxRetriesExceededError as e:
                update_sub_document_for_error(
                    sub_doc, 'data_cleansing_progress', str(e))

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback_details = {
            'filename': exc_traceback.tb_frame.f_code.co_filename,
            'lineno': exc_traceback.tb_lineno,
            'name': exc_traceback.tb_frame.f_code.co_name,
            'type': exc_type.__name__,
            'message': exc_value,  # or see traceback._some_str()
        }
        log_general_info('-------------------------------------------------')
        log_general_info("Error for:" + file_name_to_download)
        log_general_info(traceback.format_exc())
        log_general_info('-------------------------------------------------')
        update_sub_document_for_error(
            sub_doc, 'data_cleansing_progress', str(e))
        raise


def create_idc_files_directory():
    idc_files_path = os.path.join(settings.STORAGE_DIR, "idcfiles")
    create_dir(idc_files_path)
    create_dir(settings.IQBOT_CSV_PATH)
    create_dir(settings.LOCAL_FILE_PATH)
    create_dir(settings.OUTPUTPATH)


def create_dir(path):
    try:
        if not (os.path.isdir(path)):
            os.mkdir(path)
            log_general_info("Successfully created the directory %s " % path)
    except OSError:
        log_general_info("Creation of the directory %s failed" % path)


@app.task(bind=True,
          name='MSFormCleansing:schedule_sub_documents_for_data_extraction_MSForm',
          default_retry_delay=30,
          max_retries=5)
def schedule_sub_documents_for_data_extraction_ms_form(
        self, sub_doc_id, document_type, document_path, instance_master_id=None):
    """
    schedule_sub_documents_for_data_extraction_MSForm: This function will:
    1. Upload docs to MSForm server
    2. Get the documents ready.
    3. Will extract data and store it in field: "extracted_data [Class - SubDocument ]"
    """
    log_general_info('Started Data Extraction')
    from uploader.models import SubDocument
    log_general_info('Sub document ID' + str(sub_doc_id))
    try:
        sub_doc = SubDocument.objects.get(pk=sub_doc_id)
        from dataextraction.models import InstanceMaster
        if instance_master_id:
            upload_document_ms_form_server.apply_async(
                args=[
                    sub_doc_id,
                    document_type,
                    document_path,
                    instance_master_id])
        else:
            instance_master = InstanceMaster.objects.filter(
                document_type__name=document_type,
                organization=sub_doc.document.organization,
                is_active=True)
            if instance_master:
                upload_document_ms_form_server.apply_async(
                    args=[
                        sub_doc_id,
                        document_type,
                        document_path,
                        instance_master.last().id])
            else:
                instance_master = InstanceMaster.objects.filter(
                    document_type__name=document_type, organization__name='MOZAIQ', is_active=True)
                if instance_master:
                    upload_document_ms_form_server.apply_async(
                        args=[
                            sub_doc_id,
                            document_type,
                            document_path,
                            instance_master.last().id])
                else:
                    update_sub_document_for_error(
                        sub_doc,
                        'document_uploaded',
                        'Data cleansing not implemented')
    except SubDocument.DoesNotExist:
        log_info(
            self.request.id,
            sub_doc_id,
            "Data Extraction Retry, No Sub document found for Sub documentID: ")
        self.retry()
    except Exception as e:  # ObjectDoesNotExist:
        update_sub_document_for_error(
            sub_doc,
            'document_uploaded',
            str(e))
        raise  # it will retry


@app.task(bind=True, name='MSFormCleansing:upload_document_ms_form_server',
          default_retry_delay=30, max_retries=10)
def upload_document_ms_form_server(
        self,
        sub_doc_id,
        document_type,
        document_path,
        instance_master_id):
    """
    """
    try:
        import pikepdf
        from uploader.models import SubDocument
        from dataextraction.models import InstanceMaster
        instance_master = InstanceMaster.objects.get(id=instance_master_id)
        sub_doc = SubDocument.objects.get(pk=sub_doc_id)
        api_key = settings.MSFORM_APIM_KEY

        if sub_doc.document.request_type in [
            'data_extraction_only_msform',
                'mozaiq_indexing_data_extraction_msform']:
            if Utils.check_pdf_file(document_path):
                document_path = Utils.get_enhanced_pdfs(document_path)
        from azure.core.credentials import AzureKeyCredential
        from azure.ai.formrecognizer import DocumentAnalysisClient
        document_analysis_client = DocumentAnalysisClient(
            endpoint=settings.AZURE_MSFORMSERVER,
            credential=AzureKeyCredential(
                settings.AZURE_MSFORM_APIM_KEY))
        with open(document_path, "rb") as f:
            poller = document_analysis_client.begin_analyze_document(
                model_id=instance_master.instance_name, document=f
            )
        result = poller.result()
        analyze_result_dict = result.to_dict()
        sub_doc.execution_step = 'document_uploaded'
        sub_doc.pdf_page_count = Utils.get_pdf_page_count(document_path)
        sub_doc.save(False)
        file_name = os.path.basename(document_path)
        data_extract_ms_form_server.apply_async(
            args=[
                sub_doc_id,
                document_type,
                file_name,
                instance_master.id,
                analyze_result_dict],
            countdown=2,
            retry=True)
    except Exception as e:
        update_sub_document_for_error(
            sub_doc, 'data_cleansing_progress', str(e))


@app.task(bind=True, name='MSFormCleansing:Check-ready-document-MSForm',
          default_retry_delay=30, max_retries=20)
def ready_document_ms_form_server(
        self,
        sub_doc_id=None,
        document_type=None,
        document_path=None,
        doc_type=None,
        ready_url=None,
        instance_id=None):
    """
    """
    from uploader.models import SubDocument
    sub_doc = SubDocument.objects.get(uid=sub_doc_id)
    log_info(
        self.request.id,
        ready_url,
        "Ready Url")
    ready_response = requests.get(
        url=ready_url, headers={
            "Ocp-Apim-Subscription-Key": settings.MSFORM_APIM_KEY})
    ready_response_json = ready_response.json()
    if ready_response.status_code == 200:
        if ready_response_json['status'] == "succeeded":
            file_name = os.path.basename(document_path)
            sub_doc.execution_step = 'de_progress'
            sub_doc.save(False)
            data_extract_ms_form_server.apply_async(
                args=[
                    sub_doc_id,
                    document_type,
                    file_name,
                    doc_type,
                    ready_response_json],
                countdown=2,
                retry=True)
        elif ready_response_json['status'] == "failed":
            update_sub_document_for_error(
                sub_doc, 'de_progress', ready_response_json)
        else:
            self.retry()
    else:
        update_sub_document_for_error(
            sub_doc, 'de_progress', ready_response.json())


@app.task(bind=True, name='MSFormCleansing:data_extract_ms_form_server',
          default_retry_delay=30, max_retries=3)
def data_extract_ms_form_server(
        self,
        sub_doc_id,
        document_type,
        file_name,
        instance_master_id,
        response_json):
    """
    """
    from uploader.models import SubDocument
    from azure.core.serialization import AzureJSONEncoder
    sub_doc = SubDocument.objects.get(pk=sub_doc_id)
    sub_doc.execution_step = 'data_cleansing_progress'
    sub_doc.extracted_data = file_name
    sub_doc.save(False)
    create_idc_files_directory()
    local_file_path = os.path.join(
        settings.LOCAL_FILE_PATH, file_name + '.json')
    log_info(
        self.request.id,
        local_file_path,
        '---------> Saving to: ')
    with open(local_file_path, 'w') as local_file:
        json.dump(response_json, local_file, cls=AzureJSONEncoder)
    sub_doc.de_file = local_file_path
    sub_doc.save(False)
    log_general_info('---------> Data Cleansing Started')
    if sub_doc.document.is_executed_by_bot:
        output_file_path = str(sub_doc.pdf_url.path)
        output_file_path = cls_strip.cleanAfterTexts(
            file_name, output_file_path).rstrip('/ ')
    else:
        output_file_path = settings.OUTPUTPATH

    clean_data(
        sub_doc,
        settings.IQBOT_CSV_PATH,
        output_file_path,
        document_type,
        local_file_path,
        instance_master_id)


def get_file_from_zip(iq_bot_output, file_name_to_download, request_type):
    import zipfile
    # Create a folder creation object
    folder_creation_obj = IDCFolderCreation.clsfolder()

    # Remove file extension from file name
    file_name_to_download = file_name_to_download.replace(
        '.pdf', '').replace('.PDF', '')

    # Get paths for ZIP file and extract folder
    pdf_split_file = folder_creation_obj.getFolderpath(settings.IQBOT_CSV_PATH)
    zip_path = pdf_split_file[2]
    unzip_folder = pdf_split_file[3]

    # Write IQ bot output to ZIP file
    with open(os.path.join(zip_path, file_name_to_download) + ".zip", 'wb') as iq_bot_zipped_outputs:
        iq_bot_zipped_outputs.write(iq_bot_output)

    log_general_info('Unzipping Files!')

    # Unzip files from ZIP file
    with zipfile.ZipFile(os.path.join(zip_path, file_name_to_download) + ".zip", "r", allowZip64=True) as zip_ref:
        zip_ref.extractall(unzip_folder)
    # Find the downloaded file in the extracted folder
    local_file_path = ""
    for root, dirs, files in os.walk(unzip_folder):
        for file_name in files:
            if file_name_to_download in file_name:
                local_file_path = os.path.join(unzip_folder, file_name)
    return local_file_path.strip()


def get_s3_bucket_response(
        file_name_to_download,
        iq_bot_folder,
        instance_folder, sub_doc):
    try:
        file_name_to_download = os.path.join(
            'IQBotoutputPath',
            iq_bot_folder,
            instance_folder,
            file_name_to_download).replace(
            "\\",
            '/')
        file_url = utils.generate_iqbot_presigned_url(
            object_key=file_name_to_download)
        iq_bot_output = requests.get(file_url)
        return iq_bot_output
    except Exception as e:
        log_system_error(e)
        update_sub_document_for_error(
            sub_doc,
            'data_cleansing_progress',
            str(e))


@app.task(bind=True,
          name='IQBotCleansing:upload_sub_documents_for_data_extraction_iq_bot_csv',
          default_retry_delay=30,
          max_retries=2)
# upload_sub_documents_for_data_extraction_iq_bot_csv function optimisation
# 1. Used context managers to handle file objects - allows automatically handle the opening and closing of file objects,
#    as well as any other setup or teardown tasks that need to be done.
#    In this code, ExitStack is used to manage the opening and closing of files
# 2. Using os.path.exists() instead of os.path.isdir()
#    can be useful if you want to check if a path exists regardless of whether it's a directory or a file.
#    However, if you specifically want to check if a path is a directory, os.path.isdir() is more appropriate.
# 3. Removed - Repetative Code & redundant variable assignments
# 4. Use string formatting instead of string concatenation for readability
def upload_sub_documents_for_data_extraction_iq_bot_csv(
        self,
        document_path=None,
        iq_bot_token=None,
        instance_id=None,
        sub_doc_id=None):
    try:
        from contextlib import ExitStack
        from uploader.models import SubDocument
        from dataextraction.models import InstanceMaster
        from common.singletons import SingletonDoubleChecked
        singleton_instance = SingletonDoubleChecked.instance()
        instance_master = InstanceMaster.objects.get(id=instance_id)
        sub_doc = SubDocument.objects.get(pk=sub_doc_id)
        sub_doc.error_response = ""
        sub_doc.save(False)
        curl_file_name = os.path.basename(
            os.path.dirname(document_path))
        curl_file_name = cls_strip.get_alpha_numeric_text_with_underscore_only(
            curl_file_name)
        log_path = settings.LOG_PATH

        if not os.path.exists(log_path):
            os.mkdir(log_path)

        if settings.ENVIRONMENT == 'Dev':
            upload_url = settings.COMMON_11X_IQBOT_UPLOAD_URL
        else:
            upload_url = settings.COMMON_IQBOT_UPLOAD_URL
        is_valid_iqbot_token = singleton_instance.verify_IQBot_token(
            iq_bot_token)
        log_general_info(is_valid_iqbot_token)

        if not is_valid_iqbot_token:
            iq_bot_token = singleton_instance.generate_IQBot_token()
        if iq_bot_token:
            curl_command = f"{settings.CURLPATH} -F 'files=@{document_path}' --header 'x-authorization: " \
                           f"{iq_bot_token}' -X POST {upload_url}{instance_master.instance_id}/files/upload/1 -o " \
                           f"{os.path.join(log_path, curl_file_name + '_status.txt')}"
        else:
            self.retry()
        log_info(self.request.id, curl_command, 'Curl Command')
        response = os.system(curl_command)
        log_general_info(
            "method:schedule_sub_documents_for_data_extraction_IQBot")
        log_info(self.request.id, response, 'IQBot Response')
        with ExitStack() as stack:
            file = stack.enter_context(
                open(
                    os.path.join(
                        log_path,
                        curl_file_name +
                        '_status.txt'),
                    'r'))
            lines = file.readlines()
        if len(lines) > 0:
            for data in lines:
                if "message" in data or (
                        "errors" in data and "project not found" not in data):
                    log_general_info(data)
                    update_sub_document_for_error(
                        sub_doc,
                        'data_cleansing_progress',
                        f"Issue from IQ Bot Upload {data}")
                elif "error" in data:
                    update_sub_document_for_error(
                        sub_doc, 'data_cleansing_progress', 'Upload Failed')
                    self.retry()
                elif "fileId" in data:
                    file_id = cls_strip.fetchTextBetween2Strings(
                        '"', '"', data.split(":")[1])
                    sub_doc.file_id = file_id
                    sub_doc.execution_step = 'de_progress'
                    sub_doc.save(False)
                    if settings.ENVIRONMENT == 'Dev':
                        download_csv_for_data_extraction_iq_bot_ftp.apply_async(
                            args=[sub_doc.id, document_path], countdown=60, retry=True)
                    else:
                        download_csv_for_data_extraction_iq_bot.apply_async(
                            args=[
                                sub_doc_id,
                                document_path,
                                iq_bot_token,
                                None,
                                instance_id],
                            countdown=60,
                            retry=True)
        else:
            update_sub_document_for_error(
                sub_doc,
                'data_cleansing_progress',
                'Issue with IQ Bot upload, please check the queues and re-upload the document.')
    except MaxRetriesExceededError as e:
        update_sub_document_for_error(
            sub_doc, 'data_cleansing_progress', str(e))


def get_output_file_path(sub_doc, file_name_to_download):
    if sub_doc.document.is_executed_by_bot:
        output_file_path = str(sub_doc.pdf_url.path)
        output_file_path = cls_strip.cleanAfterTexts(
            file_name_to_download, output_file_path).rstrip('/ ')
    else:
        output_file_path = settings.OUTPUTPATH
    return output_file_path
