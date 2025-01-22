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
                , WORKFLOW_NAME='ET_CBOK103B',WORKFLOW_ID='a3f039f96d5840eba30771432d78ec5d', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-ET_CBOK103B-0.4.prd-tz.2.0'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 2, 9, 11, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('a3f039f96d5840eba30771432d78ec5d')

    cbok103bJob_vol = []
    cbok103bJob_volMnt = []
    cbok103bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbok103bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbok103bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbok103bJob_env.extend([getICISConfigMap('icis-oder-etcterr-batch-mng-configmap'), getICISSecret('icis-oder-etcterr-batch-mng-secret'), getICISConfigMap('icis-oder-etcterr-batch-configmap'), getICISSecret('icis-oder-etcterr-batch-secret')])
    cbok103bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbok103bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'c0be5495d60d4f68987ff03360784f89',
        'volumes': cbok103bJob_vol,
        'volume_mounts': cbok103bJob_volMnt,
        'env_from':cbok103bJob_env,
        'task_id':'cbok103bJob',
        'image':'/icis/icis-oder-etcterr-batch:0.7.1.6',
        'arguments':["--job.name=cbok103bJob", "endTranDate=${YYYYMMDD}", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('a3f039f96d5840eba30771432d78ec5d')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbok103bJob,
        Complete
    ]) 

    # authCheck >> cbok103bJob >> Complete
    workflow








