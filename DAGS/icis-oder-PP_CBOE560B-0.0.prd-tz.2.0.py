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
                , WORKFLOW_NAME='PP_CBOE560B',WORKFLOW_ID='301ce4239ee5470491b578d9b04489fa', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOE560B-0.0.prd-tz.2.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 11, 15, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('301ce4239ee5470491b578d9b04489fa')

    cboe560bJob_vol = []
    cboe560bJob_volMnt = []
    cboe560bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    cboe560bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    cboe560bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cboe560bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cboe560bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cboe560bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '258a162a2e574dbda4e3120e6782f2c5',
        'volumes': cboe560bJob_vol,
        'volume_mounts': cboe560bJob_volMnt,
        'env_from':cboe560bJob_env,
        'task_id':'cboe560bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=cboe560bJob", "workDate=20241129", "duplExePram=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('301ce4239ee5470491b578d9b04489fa')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cboe560bJob,
        Complete
    ]) 

    # authCheck >> cboe560bJob >> Complete
    workflow








