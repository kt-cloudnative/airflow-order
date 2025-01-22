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
                , WORKFLOW_NAME='PP_CSNG553B',WORKFLOW_ID='6c62c40ae30c42e0b57c26b1f405f57b', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG553B-0.0.prd-tz.5.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 7, 4, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('6c62c40ae30c42e0b57c26b1f405f57b')

    csng553bJob_vol = []
    csng553bJob_volMnt = []
    csng553bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng553bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng553bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng553bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng553bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng553bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'd3f58e8700d14ae0807086e6f9ac6685',
        'volumes': csng553bJob_vol,
        'volume_mounts': csng553bJob_volMnt,
        'env_from':csng553bJob_env,
        'task_id':'csng553bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.5',
        'arguments':["--job.name=csng553bJob", 
"tranDate=${YYYYMMDD}", 
"requestDate=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('6c62c40ae30c42e0b57c26b1f405f57b')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng553bJob,
        Complete
    ]) 

    # authCheck >> csng553bJob >> Complete
    workflow








