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
                , WORKFLOW_NAME='PP_CSND276B',WORKFLOW_ID='fd70f81423fc4e67bc3deba0b0ec6ddc', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSND276B-0.0.prd-tz.4.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 29, 12, 11, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('fd70f81423fc4e67bc3deba0b0ec6ddc')

    csnd276bJob_vol = []
    csnd276bJob_volMnt = []
    csnd276bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csnd276bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csnd276bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csnd276bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csnd276bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnd276bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'edd79683260f485080be87f45ef0be22',
        'volumes': csnd276bJob_vol,
        'volume_mounts': csnd276bJob_volMnt,
        'env_from':csnd276bJob_env,
        'task_id':'csnd276bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.3',
        'arguments':["--job.name=csnd276bJob", "endTranDate=20240430", "pgmNm=csnd276b", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('fd70f81423fc4e67bc3deba0b0ec6ddc')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnd276bJob,
        Complete
    ]) 

    # authCheck >> csnd276bJob >> Complete
    workflow








