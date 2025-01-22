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
                , WORKFLOW_NAME='PP_CSNG931B',WORKFLOW_ID='74cc6f8462c04288b9da604caabd5b73', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG931B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 27, 13, 15, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('74cc6f8462c04288b9da604caabd5b73')

    csng931bJob_vol = []
    csng931bJob_volMnt = []
    csng931bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng931bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng931bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng931bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng931bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng931bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '86ae37e815c94804bdd6152e8ce1aa24',
        'volumes': csng931bJob_vol,
        'volume_mounts': csng931bJob_volMnt,
        'env_from':csng931bJob_env,
        'task_id':'csng931bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.132',
        'arguments':["--job.name=csng931bJob", "testreqestDate=${YYYYMMDDHHMISS}", "brOfcCd=R00510", "endDate=20241101"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('74cc6f8462c04288b9da604caabd5b73')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng931bJob,
        Complete
    ]) 

    # authCheck >> csng931bJob >> Complete
    workflow








