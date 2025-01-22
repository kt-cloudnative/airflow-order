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
                , WORKFLOW_NAME='PP_CSNG552B',WORKFLOW_ID='fdf8c12cdf7348cdb5ec07b3af301b6f', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG552B-0.0.prd-tz.7.2'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 20, 13, 50, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('fdf8c12cdf7348cdb5ec07b3af301b6f')

    csng552bJob_vol = []
    csng552bJob_volMnt = []
    csng552bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng552bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng552bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng552bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng552bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng552bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '05456063441d4fd2846e228c529942ce',
        'volumes': csng552bJob_vol,
        'volume_mounts': csng552bJob_volMnt,
        'env_from':csng552bJob_env,
        'task_id':'csng552bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=csng552bJob", "tranDate=20250120", "requestDate=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('fdf8c12cdf7348cdb5ec07b3af301b6f')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng552bJob,
        Complete
    ]) 

    # authCheck >> csng552bJob >> Complete
    workflow








