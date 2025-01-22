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
                , WORKFLOW_NAME='PP_CSND152B',WORKFLOW_ID='602508657b224036a84d6dcfd4ffb8ac', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSND152B-0.0.prd-tz.3.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 19, 21, 11, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('602508657b224036a84d6dcfd4ffb8ac')

    csnd152bJob_vol = []
    csnd152bJob_volMnt = []
    csnd152bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csnd152bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csnd152bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csnd152bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csnd152bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnd152bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'a6105e22659146548b5f7ffb1572051e',
        'volumes': csnd152bJob_vol,
        'volume_mounts': csnd152bJob_volMnt,
        'env_from':csnd152bJob_env,
        'task_id':'csnd152bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.69',
        'arguments':["--job.name=csnd152bJob","endTranDate=20240528","requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('602508657b224036a84d6dcfd4ffb8ac')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnd152bJob,
        Complete
    ]) 

    # authCheck >> csnd152bJob >> Complete
    workflow








