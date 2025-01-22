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
                , WORKFLOW_NAME='PP_CSNG953B',WORKFLOW_ID='fe3a47c8874140058604da5603b9ce09', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG953B-0.0.prd-tz.2.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 14, 20, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('fe3a47c8874140058604da5603b9ce09')

    csng953bJob_vol = []
    csng953bJob_volMnt = []
    csng953bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng953bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng953bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng953bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng953bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng953bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'de260b944c014cdb958e5534b8c55d28',
        'volumes': csng953bJob_vol,
        'volume_mounts': csng953bJob_volMnt,
        'env_from':csng953bJob_env,
        'task_id':'csng953bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=csng953bJob", 
"endTranDate=20241129", 
"requestDate=${YYYYMMDDHHMISS}", ],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('fe3a47c8874140058604da5603b9ce09')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng953bJob,
        Complete
    ]) 

    # authCheck >> csng953bJob >> Complete
    workflow








