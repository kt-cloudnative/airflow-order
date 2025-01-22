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
                , WORKFLOW_NAME='EI_CSAV107B',WORKFLOW_ID='197e92b09e8b433897a291cd02093329', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-EI_CSAV107B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 13, 50, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('197e92b09e8b433897a291cd02093329')

    csav107bJob_vol = []
    csav107bJob_volMnt = []
    csav107bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csav107bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csav107bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csav107bJob_env.extend([getICISConfigMap('icis-oder-entprinet-batch-mng-configmap'), getICISSecret('icis-oder-entprinet-batch-mng-secret'), getICISConfigMap('icis-oder-entprinet-batch-configmap'), getICISSecret('icis-oder-entprinet-batch-secret')])
    csav107bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csav107bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'b4b2691743d147909f24876081d1cd96',
        'volumes': csav107bJob_vol,
        'volume_mounts': csav107bJob_volMnt,
        'env_from':csav107bJob_env,
        'task_id':'csav107bJob',
        'image':'/icis/icis-oder-entprinet-batch:0.4.1.33',
        'arguments':["--job.name=csav107bJob", "requestDate=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('197e92b09e8b433897a291cd02093329')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csav107bJob,
        Complete
    ]) 

    # authCheck >> csav107bJob >> Complete
    workflow








