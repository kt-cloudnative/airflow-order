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
                , WORKFLOW_NAME='IN_CBOT175B',WORKFLOW_ID='6f1c0addea0448c891f3d05daceb3c1d', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IN_CBOT175B-0.4.prd-tz.0.0'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 3, 16, 39, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('6f1c0addea0448c891f3d05daceb3c1d')

    cbot175bJob_vol = []
    cbot175bJob_volMnt = []
    cbot175bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbot175bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbot175bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbot175bJob_env.extend([getICISConfigMap('icis-oder-intelnet-batch-mng-configmap'), getICISSecret('icis-oder-intelnet-batch-mng-secret'), getICISConfigMap('icis-oder-intelnet-batch-configmap'), getICISSecret('icis-oder-intelnet-batch-secret')])
    cbot175bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbot175bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'a253608c22d748b2b03ecb5350cc43e9',
        'volumes': cbot175bJob_vol,
        'volume_mounts': cbot175bJob_volMnt,
        'env_from':cbot175bJob_env,
        'task_id':'cbot175bJob',
        'image':'/icis/icis-oder-intelnet-batch:0.7.1.6',
        'arguments':["--job.name=cbot175bJob", "inDate=${YYYYMM}01", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('6f1c0addea0448c891f3d05daceb3c1d')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbot175bJob,
        Complete
    ]) 

    # authCheck >> cbot175bJob >> Complete
    workflow








