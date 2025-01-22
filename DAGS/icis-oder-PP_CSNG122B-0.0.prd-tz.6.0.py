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
                , WORKFLOW_NAME='PP_CSNG122B',WORKFLOW_ID='9f9a8e9bbd5041a99588e0d00b1ef0dd', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG122B-0.0.prd-tz.6.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 29, 10, 59, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('9f9a8e9bbd5041a99588e0d00b1ef0dd')

    csng122bJob_vol = []
    csng122bJob_volMnt = []
    csng122bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng122bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng122bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csng122bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csng122bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng122bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '2ef3a30953c341388cc93533d1df0556',
        'volumes': csng122bJob_vol,
        'volume_mounts': csng122bJob_volMnt,
        'env_from':csng122bJob_env,
        'task_id':'csng122bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.3',
        'arguments':["--job.name=csng122bJob", "duplPrvnDate=${YYYYMMDDHHMISSSSS}", "workDay=20240430"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('9f9a8e9bbd5041a99588e0d00b1ef0dd')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng122bJob,
        Complete
    ]) 

    # authCheck >> csng122bJob >> Complete
    workflow








