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
                , WORKFLOW_NAME='IC_CSNJ190B',WORKFLOW_ID='e4364c66e987463d9ce59eb54eca251c', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IC_CSNJ190B-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 13, 17, 10, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('e4364c66e987463d9ce59eb54eca251c')

    csnj190bJob_vol = []
    csnj190bJob_volMnt = []
    csnj190bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csnj190bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csnj190bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnj190bJob_env.extend([getICISConfigMap('icis-oder-infocomm-batch-mng-configmap'), getICISSecret('icis-oder-infocomm-batch-mng-secret'), getICISConfigMap('icis-oder-infocomm-batch-configmap'), getICISSecret('icis-oder-infocomm-batch-secret')])
    csnj190bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnj190bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '1130cc8c183e4d95acbae18a540ea968',
        'volumes': csnj190bJob_vol,
        'volume_mounts': csnj190bJob_volMnt,
        'env_from':csnj190bJob_env,
        'task_id':'csnj190bJob',
        'image':'/icis/icis-oder-infocomm-batch:0.7.1.7',
        'arguments':["--job.name=csnj190bJob", "reqDate=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('e4364c66e987463d9ce59eb54eca251c')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnj190bJob,
        Complete
    ]) 

    # authCheck >> csnj190bJob >> Complete
    workflow








