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
                , WORKFLOW_NAME='PP_CSNG138B',WORKFLOW_ID='ec7bf3b4633247f2bad99046f4e74d13', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG138B-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 4, 2, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('ec7bf3b4633247f2bad99046f4e74d13')

    csng138bJob_vol = []
    csng138bJob_volMnt = []
    csng138bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csng138bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csng138bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng138bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng138bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng138bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'f6ef23444bed4b5fbee80a4f3f38477b',
        'volumes': csng138bJob_vol,
        'volume_mounts': csng138bJob_volMnt,
        'env_from':csng138bJob_env,
        'task_id':'csng138bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.5',
        'arguments':["--job.name=csng138bJob", "workDate=${YYYYMM}", "duplExePram=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('ec7bf3b4633247f2bad99046f4e74d13')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng138bJob,
        Complete
    ]) 

    # authCheck >> csng138bJob >> Complete
    workflow








