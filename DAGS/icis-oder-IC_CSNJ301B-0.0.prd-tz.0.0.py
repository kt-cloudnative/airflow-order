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
                , WORKFLOW_NAME='IC_CSNJ301B',WORKFLOW_ID='29bd7a85e1984a378c4048ddfc8d3524', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IC_CSNJ301B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 14, 1, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('29bd7a85e1984a378c4048ddfc8d3524')

    csnj301bJob_vol = []
    csnj301bJob_volMnt = []
    csnj301bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csnj301bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csnj301bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnj301bJob_env.extend([getICISConfigMap('icis-oder-infocomm-batch-mng-configmap'), getICISSecret('icis-oder-infocomm-batch-mng-secret'), getICISConfigMap('icis-oder-infocomm-batch-configmap'), getICISSecret('icis-oder-infocomm-batch-secret')])
    csnj301bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnj301bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'ac9d1f964d0f4a2f8b65b65c397e8069',
        'volumes': csnj301bJob_vol,
        'volume_mounts': csnj301bJob_volMnt,
        'env_from':csnj301bJob_env,
        'task_id':'csnj301bJob',
        'image':'/icis/icis-oder-infocomm-batch:0.7.1.6',
        'arguments':["--job.name=csnj301bJob", "reqDate=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('29bd7a85e1984a378c4048ddfc8d3524')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnj301bJob,
        Complete
    ]) 

    # authCheck >> csnj301bJob >> Complete
    workflow








