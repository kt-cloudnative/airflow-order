from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.helpers import chain, cross_downstream
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_common import *
from icis_dag_util import ICISDagUtil
COMMON = ICISCmmn(DOMAIN='oder', ENV='prd-tz', NAMESPACE='t-order', WORKFLOW_NAME='SOP-SWHWPM-24X7-ICIS-ODER-21', WORKFLOW_ID='SOP-SWHWPM-24X7-ICIS-ODER-21')

with COMMON.getICISDAG({
    'dag_id':'SOP-SWHWPM-24X7-ICIS-ODER-21',
    'schedule_interval':'None',
    'start_date': datetime(1900, 1, 1, 23, 59, 59, tzinfo=local_tz),
    'end_date': None,
    'paused': True
})as dag:

    #COMMON.getDummyTask('test', True)
    stop_batchagent = ICISDagUtil.getTask(COMMON, "stop_batchagent")
    
    switch_to_pmdb_cz = ICISDagUtil.getTask(COMMON, "switch_to_pmdb", "cz")
    switch_to_pmdb_lt_cz = ICISDagUtil.getTask(COMMON, "switch_to_pmdb_lt", "cz")
    switch_to_pmdb_name_cz = ICISDagUtil.getTask(COMMON, "switch_to_pmdb_name", "cz")
    switch_to_pmdb_name_lt_cz = ICISDagUtil.getTask(COMMON, "switch_to_pmdb_name_lt", "cz")
    switch_to_pmdb_tz = ICISDagUtil.getTask(COMMON, "switch_to_pmdb", "tz")
    switch_to_pmdb_lt_tz = ICISDagUtil.getTask(COMMON, "switch_to_pmdb_lt", "tz")
    switch_to_pmdb_name_tz = ICISDagUtil.getTask(COMMON, "switch_to_pmdb_name", "tz")
    switch_to_pmdb_name_lt_tz = ICISDagUtil.getTask(COMMON, "switch_to_pmdb_name_lt", "tz")

    restart_pmdb_daemon_pod_cz = ICISDagUtil.getTask(COMMON, "restart_pmdb_daemon_pod", "cz")
    restart_pmdb_daemon_pod_lt_cz = ICISDagUtil.getTask(COMMON, "restart_pmdb_daemon_pod_lt", "cz")
    restart_pmdb_daemon_pod_tz = ICISDagUtil.getTask(COMMON, "restart_pmdb_daemon_pod", "tz")
    restart_pmdb_daemon_pod_lt_tz = ICISDagUtil.getTask(COMMON, "restart_pmdb_daemon_pod_lt", "tz")

    restart_pmdb_pod_cz = ICISDagUtil.getTask(COMMON, "restart_pmdb_pod", "cz")
    restart_pmdb_pod_lt_cz = ICISDagUtil.getTask(COMMON, "restart_pmdb_pod_lt", "cz")
    restart_pmdb_pod_tz = ICISDagUtil.getTask(COMMON, "restart_pmdb_pod", "tz")
    restart_pmdb_pod_lt_tz = ICISDagUtil.getTask(COMMON, "restart_pmdb_pod_lt", "tz")


    stop_batchagent >> switch_to_pmdb_cz >> switch_to_pmdb_name_cz >> switch_to_pmdb_name_lt_cz >> switch_to_pmdb_lt_cz >> switch_to_pmdb_tz >> switch_to_pmdb_lt_tz >> switch_to_pmdb_name_tz >> switch_to_pmdb_name_lt_tz >> restart_pmdb_daemon_pod_cz >> restart_pmdb_daemon_pod_lt_cz >> restart_pmdb_daemon_pod_tz >> restart_pmdb_daemon_pod_lt_tz >> restart_pmdb_pod_cz >> restart_pmdb_pod_lt_cz >> restart_pmdb_pod_tz >> restart_pmdb_pod_lt_tz

