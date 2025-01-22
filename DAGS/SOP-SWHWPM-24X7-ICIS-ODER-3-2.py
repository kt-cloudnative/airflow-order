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
COMMON = ICISCmmn(DOMAIN='oder', ENV='prd-tz', NAMESPACE='t-order', WORKFLOW_NAME='SOP-SWHWPM-24X7-ICIS-ODER-3-2', WORKFLOW_ID='SOP-SWHWPM-24X7-ICIS-ODER-3-2')

with COMMON.getICISDAG({
    'dag_id':'SOP-SWHWPM-24X7-ICIS-ODER-3-2',
    'schedule_interval':'None',
    'start_date': datetime(1900, 1, 1, 0, 0, 00, tzinfo=local_tz),
    'end_date': None,
    'paused': True
})as dag:
    
    stop_consumer_cz = ICISDagUtil.getTask(COMMON, "stop_consumer", "cz")
    stop_consumer_lt_cz = ICISDagUtil.getTask(COMMON, "stop_consumer_lt", "cz")
    
    stop_consumer_tz = ICISDagUtil.getTask(COMMON, "stop_consumer", "tz")
    stop_consumer_lt_tz = ICISDagUtil.getTask(COMMON, "stop_consumer_lt", "tz")

    stop_consumer_cz >> stop_consumer_lt_cz >> stop_consumer_tz >> stop_consumer_lt_tz

