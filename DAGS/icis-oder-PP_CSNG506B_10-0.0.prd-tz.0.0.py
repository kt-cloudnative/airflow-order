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
                , WORKFLOW_NAME='PP_CSNG506B_10',WORKFLOW_ID='b425152d555a4957b6334103280353bb', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG506B_10-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 3, 18, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('b425152d555a4957b6334103280353bb')

    csng506bJob_vol = []
    csng506bJob_volMnt = []
    csng506bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csng506bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csng506bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng506bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng506bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng506bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '2c34451c2d214e3f8e186c3b69eb6e14',
        'volumes': csng506bJob_vol,
        'volume_mounts': csng506bJob_volMnt,
        'env_from':csng506bJob_env,
        'task_id':'csng506bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.132',
        'arguments':["--job.name=csng506bJob", "argv2=0052", "argv3=Y", "requestDate=${YYYYMM}" , "duplPrvnDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('b425152d555a4957b6334103280353bb')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng506bJob,
        Complete
    ]) 

    # authCheck >> csng506bJob >> Complete
    workflow








