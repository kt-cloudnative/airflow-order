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
                , WORKFLOW_NAME='PP_CBOD703B',WORKFLOW_ID='2015ac386bf4417692eb676d4aebffaf', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOD703B-0.0.prd-tz.5.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 10, 25, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('2015ac386bf4417692eb676d4aebffaf')

    cbod703bJob_vol = []
    cbod703bJob_volMnt = []
    cbod703bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbod703bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbod703bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbod703bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbod703bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbod703bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'd6d18a41d37d4dc0867871a652c0fe7b',
        'volumes': cbod703bJob_vol,
        'volume_mounts': cbod703bJob_volMnt,
        'env_from':cbod703bJob_env,
        'task_id':'cbod703bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=cbod703bJob" , "endTranDate=20241129","trdDay=202411","requestDate=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('2015ac386bf4417692eb676d4aebffaf')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbod703bJob,
        Complete
    ]) 

    # authCheck >> cbod703bJob >> Complete
    workflow








