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
                , WORKFLOW_NAME='EI_KONETAUTOCMPLN',WORKFLOW_ID='f28add318651476d976b6dd3b8d51140', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-EI_KONETAUTOCMPLN-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 13, 35, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('f28add318651476d976b6dd3b8d51140')

    kornetAutoCmplnJob_vol = []
    kornetAutoCmplnJob_volMnt = []
    kornetAutoCmplnJob_vol.append(getVolume('shared-volume','shared-volume'))
    kornetAutoCmplnJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    kornetAutoCmplnJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    kornetAutoCmplnJob_env.extend([getICISConfigMap('icis-oder-entprinet-batch-mng-configmap'), getICISSecret('icis-oder-entprinet-batch-mng-secret'), getICISConfigMap('icis-oder-entprinet-batch-configmap'), getICISSecret('icis-oder-entprinet-batch-secret')])
    kornetAutoCmplnJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    kornetAutoCmplnJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '96e3f72000ad4f49902f4e27867636ac',
        'volumes': kornetAutoCmplnJob_vol,
        'volume_mounts': kornetAutoCmplnJob_volMnt,
        'env_from':kornetAutoCmplnJob_env,
        'task_id':'kornetAutoCmplnJob',
        'image':'/icis/icis-oder-entprinet-batch:0.4.1.33',
        'arguments':["--job.name=kornetAutoCmplnJob", "requestDate=${YYYYMMDDHHMISS}", "procDate=20241129", "progName=pidodei0748"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('f28add318651476d976b6dd3b8d51140')

    workflow = COMMON.getICISPipeline([
        authCheck,
        kornetAutoCmplnJob,
        Complete
    ]) 

    # authCheck >> kornetAutoCmplnJob >> Complete
    workflow








