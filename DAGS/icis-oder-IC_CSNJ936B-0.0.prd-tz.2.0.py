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
                , WORKFLOW_NAME='IC_CSNJ936B',WORKFLOW_ID='32b4b39a56364f569007cb5a5e5f7f39', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IC_CSNJ936B-0.0.prd-tz.2.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 8, 15, 21, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('32b4b39a56364f569007cb5a5e5f7f39')

    csnj936bJob_vol = []
    csnj936bJob_volMnt = []
    csnj936bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csnj936bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csnj936bJob_env = [getICISConfigMap('icis-oder-infocomm-batch-configmap'), getICISConfigMap('icis-oder-infocomm-batch-configmap2'), getICISSecret('icis-oder-infocomm-batch-secret')]
    csnj936bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csnj936bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnj936bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '3f57fa221db14c3da5ec996f38653918',
        'volumes': csnj936bJob_vol,
        'volume_mounts': csnj936bJob_volMnt,
        'env_from':csnj936bJob_env,
        'task_id':'csnj936bJob',
        'image':'/icis/icis-oder-infocomm-batch:0.7.1.3',
        'arguments':["--job.name=csnj936bJob", "requestDate=${YYYYMMDDHHMISS}", "procDate=20240630"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('32b4b39a56364f569007cb5a5e5f7f39')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnj936bJob,
        Complete
    ]) 

    # authCheck >> csnj936bJob >> Complete
    workflow








