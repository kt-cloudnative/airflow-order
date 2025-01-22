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
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='PP_CBOL207B',WORKFLOW_ID='ce5e16ae5bc3468cb591b51772b4f0ea', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOL207B-0.0.prd-tz.0.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 21, 21, 57, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('ce5e16ae5bc3468cb591b51772b4f0ea')

    cbol207bJob_vol = []
    cbol207bJob_volMnt = []
    cbol207bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbol207bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbol207bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbol207bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '90f562ac3de940579893f470c4c548a0',
        'volumes': cbol207bJob_vol,
        'volume_mounts': cbol207bJob_volMnt,
        'env_from':cbol207bJob_env,
        'task_id':'cbol207bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.5',
        'arguments':["--job.name=cbol207bJob", "endTranDate=${YYYYMMDD}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('ce5e16ae5bc3468cb591b51772b4f0ea')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbol207bJob,
        Complete
    ]) 

    # authCheck >> cbol207bJob >> Complete
    workflow








