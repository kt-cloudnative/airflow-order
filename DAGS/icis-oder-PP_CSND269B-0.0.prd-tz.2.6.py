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
                , WORKFLOW_NAME='PP_CSND269B',WORKFLOW_ID='3f35e8b723f2485a8476fec0f35941eb', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSND269B-0.0.prd-tz.2.6'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 29, 12, 21, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('3f35e8b723f2485a8476fec0f35941eb')

    csnd269bJob_vol = []
    csnd269bJob_volMnt = []
    csnd269bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csnd269bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csnd269bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnd269bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csnd269bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnd269bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '62dc2a6c4b424e958fd4967f84e91e52',
        'volumes': csnd269bJob_vol,
        'volume_mounts': csnd269bJob_volMnt,
        'env_from':csnd269bJob_env,
        'task_id':'csnd269bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.3',
        'arguments':["--job.name=csnd269bJob", "endTranDate=20240430", "pgmNm=csnd269b", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('3f35e8b723f2485a8476fec0f35941eb')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnd269bJob,
        Complete
    ]) 

    # authCheck >> csnd269bJob >> Complete
    workflow








