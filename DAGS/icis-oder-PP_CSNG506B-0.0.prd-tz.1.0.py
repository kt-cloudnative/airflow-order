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
                , WORKFLOW_NAME='PP_CSNG506B',WORKFLOW_ID='7e22fd54a06e41309dbbb2ed4e9ef561', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG506B-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 13, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('7e22fd54a06e41309dbbb2ed4e9ef561')

    csng506bJob_vol = []
    csng506bJob_volMnt = []
    csng506bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng506bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng506bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csng506bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csng506bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng506bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng506bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng506bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'd8716a2332674916ba32e53527c3ea1a',
        'volumes': csng506bJob_vol,
        'volume_mounts': csng506bJob_volMnt,
        'env_from':csng506bJob_env,
        'task_id':'csng506bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=csng506bJob", "argv2=0002", "argv3=Y", "requestDate="+str(datetime.now().strftime("%Y%m")) , "duplPrvnDate="+str(datetime.now()) ],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('7e22fd54a06e41309dbbb2ed4e9ef561')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng506bJob,
        Complete
    ]) 

    # authCheck >> csng506bJob >> Complete
    workflow








