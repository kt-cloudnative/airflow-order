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
                , WORKFLOW_NAME='PP_CSNG633B',WORKFLOW_ID='ca2d455b43d544d3b33cd21135d5bcb6', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG633B-0.0.prd-tz.5.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 29, 11, 52, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('ca2d455b43d544d3b33cd21135d5bcb6')

    csng633bJob_vol = []
    csng633bJob_volMnt = []
    csng633bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng633bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng633bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csng633bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csng633bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng633bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '30858b85fe2342f6b64cc9f10dc2dfef',
        'volumes': csng633bJob_vol,
        'volume_mounts': csng633bJob_volMnt,
        'env_from':csng633bJob_env,
        'task_id':'csng633bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.3',
        'arguments':["--job.name=csng633bJob", "requestDate=${YYYYMMDDHHMISSSSS}"
, "pgmNm=csng633b" ],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('ca2d455b43d544d3b33cd21135d5bcb6')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng633bJob,
        Complete
    ]) 

    # authCheck >> csng633bJob >> Complete
    workflow








