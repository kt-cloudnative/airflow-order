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
                , WORKFLOW_NAME='ST_CONNCHECK',WORKFLOW_ID='7d1a79c685234a8fa85c1e5728984791', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-ST_CONNCHECK-0.4.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 12, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('7d1a79c685234a8fa85c1e5728984791')

    conncheckJob_vol = []
    conncheckJob_volMnt = []
    conncheckJob_vol.append(getVolume('shared-volume','shared-volume'))
    conncheckJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    conncheckJob_env = [getICISConfigMap('icis-oder-batch-sample-test-configmap'), getICISConfigMap('icis-oder-batch-sample-test-configmap2'), getICISSecret('icis-oder-batch-sample-test-secret')]
    conncheckJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    conncheckJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    conncheckJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'd4a4ee3a60e44ee5bc0be4282e443c08',
        'volumes': conncheckJob_vol,
        'volume_mounts': conncheckJob_volMnt,
        'env_from':conncheckJob_env,
        'task_id':'conncheckJob',
        'image':'/icis/icis-oder-batch-sample-test:0.7.1.1',
        'arguments':["--job.name=conncheckJob","date=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('7d1a79c685234a8fa85c1e5728984791')

    workflow = COMMON.getICISPipeline([
        authCheck,
        conncheckJob,
        Complete
    ]) 

    # authCheck >> conncheckJob >> Complete
    workflow








