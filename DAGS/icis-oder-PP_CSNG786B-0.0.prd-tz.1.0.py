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
                , WORKFLOW_NAME='PP_CSNG786B',WORKFLOW_ID='643c2739f9944e549874d873ffd774fe', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG786B-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 8, 18, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('643c2739f9944e549874d873ffd774fe')

    csng786bJob_vol = []
    csng786bJob_volMnt = []
    csng786bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng786bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng786bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csng786bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csng786bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng786bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '9a2065b2b3434d769a70ae4c3cd6a4bc',
        'volumes': csng786bJob_vol,
        'volume_mounts': csng786bJob_volMnt,
        'env_from':csng786bJob_env,
        'task_id':'csng786bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.57',
        'arguments':["--job.name=csng786bJob","requestDate=202406"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('643c2739f9944e549874d873ffd774fe')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng786bJob,
        Complete
    ]) 

    # authCheck >> csng786bJob >> Complete
    workflow








