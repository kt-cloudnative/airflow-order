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
                , WORKFLOW_NAME='PP_CBOD703B',WORKFLOW_ID='5cdc98f1905e41dc8d04b65beab98a25', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOD703B-0.0.prd-tz.3.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 8, 17, 30, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('5cdc98f1905e41dc8d04b65beab98a25')

    cbod703bJob_vol = []
    cbod703bJob_volMnt = []
    cbod703bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbod703bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbod703bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    cbod703bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbod703bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbod703bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '8ec3770e9d004a30a1787b7960443372',
        'volumes': cbod703bJob_vol,
        'volume_mounts': cbod703bJob_volMnt,
        'env_from':cbod703bJob_env,
        'task_id':'cbod703bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.3',
        'arguments':["--job.name=cbod703bJob" , "endTranDate=20240628","trdDay=202406","requestDate=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('5cdc98f1905e41dc8d04b65beab98a25')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbod703bJob,
        Complete
    ]) 

    # authCheck >> cbod703bJob >> Complete
    workflow








