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
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-oder'
                , WORKFLOW_NAME='icis-oder-availability-switch-to-24by7db',WORKFLOW_ID='000000000000000000000000000005', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-availability-switch-to-24by7db'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 27, 4, 20, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': True
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('000000000000000000000000000005')

    switch_to_24by7db_cz = ICISDagUtil.getScenario(COMMON, "availability-switch-to-24by7db-test", "cz")
    switch_to_24by7db_tz = ICISDagUtil.getScenario(COMMON, "availability-switch-to-24by7db-test", "tz")

    Complete = COMMON.getICISCompleteWflowTask('000000000000000000000000000005')


    workflow = COMMON.getICISPipeline([
        authCheck,
        switch_to_24by7db_cz,
        switch_to_24by7db_tz,
        Complete
    ]) 







