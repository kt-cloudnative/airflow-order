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
                , WORKFLOW_NAME='PP_CSNG122B',WORKFLOW_ID='077be7fddc6341679d154c531792d00c', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG122B-0.0.prd-tz.3.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 19, 19, 59, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('077be7fddc6341679d154c531792d00c')

    csng122bJob_vol = []
    csng122bJob_volMnt = []
    csng122bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng122bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng122bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csng122bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csng122bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng122bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '1751f2263f5c46a398e778d78dca1791',
        'volumes': csng122bJob_vol,
        'volume_mounts': csng122bJob_volMnt,
        'env_from':csng122bJob_env,
        'task_id':'csng122bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.73',
        'arguments':["--job.name=csng122bJob", "duplPrvnDate=${YYYYMMDDHHMISSSSS}", "workDay=20240528"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('077be7fddc6341679d154c531792d00c')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng122bJob,
        Complete
    ]) 

    # authCheck >> csng122bJob >> Complete
    workflow








