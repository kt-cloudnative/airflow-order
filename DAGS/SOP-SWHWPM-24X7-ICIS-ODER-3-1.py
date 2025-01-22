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
COMMON = ICISCmmn(DOMAIN='oder', ENV='prd-tz', NAMESPACE='t-order', WORKFLOW_NAME='SOP-SWHWPM-24X7-ICIS-ODER-3-1', WORKFLOW_ID='SOP-SWHWPM-24X7-ICIS-ODER-3-1')

with COMMON.getICISDAG({
    'dag_id':'SOP-SWHWPM-24X7-ICIS-ODER-3-1',
    'schedule_interval':'None',
    'start_date': datetime(1900, 1, 1, 0, 0, 00, tzinfo=local_tz),
    'end_date': None,
    'paused': True
})as dag:

    async_gw_close_cz = ICISDagUtil.getTask(COMMON, "async_gw_close_cz")
    async_gw_close_lt_cz = ICISDagUtil.getTask(COMMON, "async_gw_close_lt_cz")
    
    async_gw_close_tz = ICISDagUtil.getTask(COMMON, "async_gw_close_tz")
    async_gw_close_lt_tz = ICISDagUtil.getTask(COMMON, "async_gw_close_lt_tz")

    async_gw_close_cz >> async_gw_close_lt_cz >> async_gw_close_tz >> async_gw_close_lt_tz

