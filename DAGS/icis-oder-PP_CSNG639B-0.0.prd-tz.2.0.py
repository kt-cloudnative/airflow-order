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
                , WORKFLOW_NAME='PP_CSNG639B',WORKFLOW_ID='2eeccc3cabea45fcb54dee328204a017', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG639B-0.0.prd-tz.2.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 8, 17, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('2eeccc3cabea45fcb54dee328204a017')

    csng639bJob_vol = []
    csng639bJob_volMnt = []
    csng639bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng639bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng639bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csng639bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csng639bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng639bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '93494f9059e04c778a8d6d7373d21846',
        'volumes': csng639bJob_vol,
        'volume_mounts': csng639bJob_volMnt,
        'env_from':csng639bJob_env,
        'task_id':'csng639bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.57',
        'arguments':["--job.name=csng639bJob",
"requestDate="+str(datetime.now()),
"procMonths=202406"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('2eeccc3cabea45fcb54dee328204a017')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng639bJob,
        Complete
    ]) 

    # authCheck >> csng639bJob >> Complete
    workflow








