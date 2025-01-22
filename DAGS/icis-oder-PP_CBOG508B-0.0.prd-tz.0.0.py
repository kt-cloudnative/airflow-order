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
                , WORKFLOW_NAME='PP_CBOG508B',WORKFLOW_ID='adddbd9c76d848cbb853357742b9a753', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOG508B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 7, 4, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('adddbd9c76d848cbb853357742b9a753')

    cbog508bJob_vol = []
    cbog508bJob_volMnt = []
    cbog508bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbog508bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbog508bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog508bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbog508bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog508bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '95734d463c854a91ab315ee46fb0f705',
        'volumes': cbog508bJob_vol,
        'volume_mounts': cbog508bJob_volMnt,
        'env_from':cbog508bJob_env,
        'task_id':'cbog508bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.5',
        'arguments':["--job.name=cbog508bJob", "empNo=91348440", "testRequestDate=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('adddbd9c76d848cbb853357742b9a753')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbog508bJob,
        Complete
    ]) 

    # authCheck >> cbog508bJob >> Complete
    workflow








