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
                , WORKFLOW_NAME='PP_CSNG110B',WORKFLOW_ID='5e6a6d2567a54fc3aeeee3ea7128398b', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG110B-0.1.prd-tz.4.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 21, 19, 49, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('5e6a6d2567a54fc3aeeee3ea7128398b')

    csng110bJob_vol = []
    csng110bJob_volMnt = []
    csng110bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng110bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng110bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csng110bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csng110bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng110bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '44ffbac7941d4677a934fce4b72b0a17',
        'volumes': csng110bJob_vol,
        'volume_mounts': csng110bJob_volMnt,
        'env_from':csng110bJob_env,
        'task_id':'csng110bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.3',
        'arguments':["--job.name=csng110bJob", "workDay=20240529", "empNo=91326888", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('5e6a6d2567a54fc3aeeee3ea7128398b')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng110bJob,
        Complete
    ]) 

    # authCheck >> csng110bJob >> Complete
    workflow








