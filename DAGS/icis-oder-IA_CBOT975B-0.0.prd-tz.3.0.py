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
                , WORKFLOW_NAME='IA_CBOT975B',WORKFLOW_ID='6dd3173d46f842a7aba7f6e7dc9c8a48', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IA_CBOT975B-0.0.prd-tz.3.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 8, 10, 21, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('6dd3173d46f842a7aba7f6e7dc9c8a48')

    cbot975bJob_vol = []
    cbot975bJob_volMnt = []
    cbot975bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbot975bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbot975bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbot975bJob_env.extend([getICISConfigMap('icis-oder-inetaplca-batch-mng-configmap'), getICISSecret('icis-oder-inetaplca-batch-mng-secret'), getICISConfigMap('icis-oder-inetaplca-batch-configmap'), getICISSecret('icis-oder-inetaplca-batch-secret')])
    cbot975bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbot975bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'dfa07b38d75647a4afcc37618395da8d',
        'volumes': cbot975bJob_vol,
        'volume_mounts': cbot975bJob_volMnt,
        'env_from':cbot975bJob_env,
        'task_id':'cbot975bJob',
        'image':'/icis/icis-oder-inetaplca-batch:0.4.1.35',
        'arguments':["--job.name=cbot975bJob", "requestDate=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('6dd3173d46f842a7aba7f6e7dc9c8a48')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbot975bJob,
        Complete
    ]) 

    # authCheck >> cbot975bJob >> Complete
    workflow








