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
                , WORKFLOW_NAME='PP_CSNG122B',WORKFLOW_ID='43711d6dfddd49e5b4f096b2ddaf6a93', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG122B-0.0.prd-tz.9.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 11, 20, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('43711d6dfddd49e5b4f096b2ddaf6a93')

    csng122bJob_vol = []
    csng122bJob_volMnt = []
    csng122bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng122bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng122bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng122bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng122bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng122bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'd152649bbeff47119ca114ae24047316',
        'volumes': csng122bJob_vol,
        'volume_mounts': csng122bJob_volMnt,
        'env_from':csng122bJob_env,
        'task_id':'csng122bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=csng122bJob", "duplPrvnDate=${YYYYMMDDHHMISSSSS}", "workDay=20241129"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('43711d6dfddd49e5b4f096b2ddaf6a93')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng122bJob,
        Complete
    ]) 

    # authCheck >> csng122bJob >> Complete
    workflow








