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
                , WORKFLOW_NAME='PP_CSNG554B',WORKFLOW_ID='0d0aee4bcbde4b9b8ab9711649d5a75e', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG554B-0.0.prd-tz.2.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 19, 21, 52, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('0d0aee4bcbde4b9b8ab9711649d5a75e')

    csng554bJob_vol = []
    csng554bJob_volMnt = []
    csng554bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng554bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng554bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csng554bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csng554bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng554bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '261d1bf552a94bea804ed569cf396e82',
        'volumes': csng554bJob_vol,
        'volume_mounts': csng554bJob_volMnt,
        'env_from':csng554bJob_env,
        'task_id':'csng554bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.69',
        'arguments':["--job.name=csng554bJob","tranDate=20240528"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('0d0aee4bcbde4b9b8ab9711649d5a75e')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng554bJob,
        Complete
    ]) 

    # authCheck >> csng554bJob >> Complete
    workflow








