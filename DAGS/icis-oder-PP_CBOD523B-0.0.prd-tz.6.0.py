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
                , WORKFLOW_NAME='PP_CBOD523B',WORKFLOW_ID='7065770966004ef28f562731e1182d46', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOD523B-0.0.prd-tz.6.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 10, 25, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('7065770966004ef28f562731e1182d46')

    cbod523bJob_vol = []
    cbod523bJob_volMnt = []
    cbod523bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbod523bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbod523bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbod523bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbod523bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbod523bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'e3285b8774514997acad04ade96e42b1',
        'volumes': cbod523bJob_vol,
        'volume_mounts': cbod523bJob_volMnt,
        'env_from':cbod523bJob_env,
        'task_id':'cbod523bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=cbod523bJob","endTranDate=20241129","requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('7065770966004ef28f562731e1182d46')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbod523bJob,
        Complete
    ]) 

    # authCheck >> cbod523bJob >> Complete
    workflow








