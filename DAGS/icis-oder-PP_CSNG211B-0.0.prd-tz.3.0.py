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
                , WORKFLOW_NAME='PP_CSNG211B',WORKFLOW_ID='eef2fd56cb1343ad94376d5cb63fb8c5', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG211B-0.0.prd-tz.3.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 8, 17, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('eef2fd56cb1343ad94376d5cb63fb8c5')

    csng211bJob_vol = []
    csng211bJob_volMnt = []
    csng211bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng211bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng211bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng211bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng211bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng211bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'd8cfd3f64b4a41668620a003b012d304',
        'volumes': csng211bJob_vol,
        'volume_mounts': csng211bJob_volMnt,
        'env_from':csng211bJob_env,
        'task_id':'csng211bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.132',
        'arguments':["--job.name=csng211bJob", "workMonth=${YYYYMM}", "ver="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('eef2fd56cb1343ad94376d5cb63fb8c5')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng211bJob,
        Complete
    ]) 

    # authCheck >> csng211bJob >> Complete
    workflow








