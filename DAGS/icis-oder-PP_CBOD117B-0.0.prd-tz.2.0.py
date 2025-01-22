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
                , WORKFLOW_NAME='PP_CBOD117B',WORKFLOW_ID='40a407b22e2446dc839c6242af62db99', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOD117B-0.0.prd-tz.2.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 8, 12, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('40a407b22e2446dc839c6242af62db99')

    cbod117bJob_vol = []
    cbod117bJob_volMnt = []
    cbod117bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbod117bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbod117bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    cbod117bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbod117bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbod117bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '89ffd0d2cff14e4cb9ba2e24ff680e31',
        'volumes': cbod117bJob_vol,
        'volume_mounts': cbod117bJob_volMnt,
        'env_from':cbod117bJob_env,
        'task_id':'cbod117bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.57',
        'arguments':["--job.name=cbod117bJob", "endTranDate=20240628", "fromTime=14","toTime=24" , "requestDate=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('40a407b22e2446dc839c6242af62db99')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbod117bJob,
        Complete
    ]) 

    # authCheck >> cbod117bJob >> Complete
    workflow








