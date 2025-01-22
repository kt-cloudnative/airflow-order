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
                , WORKFLOW_NAME='PP_CSNG355B',WORKFLOW_ID='cb62efa2f7b24d82b59ed21fdfd9bbbb', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG355B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 7, 9, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('cb62efa2f7b24d82b59ed21fdfd9bbbb')

    csng355bJob_vol = []
    csng355bJob_volMnt = []
    csng355bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng355bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng355bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng355bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng355bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng355bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '35094a4cc19c46a2b069efd7454b8901',
        'volumes': csng355bJob_vol,
        'volume_mounts': csng355bJob_volMnt,
        'env_from':csng355bJob_env,
        'task_id':'csng355bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.132',
        'arguments':["--job.name=csng355bJob", 
"pstnFlag=2", 
"rcvFileName=KT0000000010", 
"sndFileName=KT0000000010", 
"requestDate=${YYYYMMDDHHMISS}"
],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('cb62efa2f7b24d82b59ed21fdfd9bbbb')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng355bJob,
        Complete
    ]) 

    # authCheck >> csng355bJob >> Complete
    workflow








