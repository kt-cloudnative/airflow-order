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
                , WORKFLOW_NAME='PP_CSND276B',WORKFLOW_ID='1db5f0cee05a420f868e37be9a20a678', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSND276B-0.0.prd-tz.3.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 22, 16, 11, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('1db5f0cee05a420f868e37be9a20a678')

    csnd276bJob_vol = []
    csnd276bJob_volMnt = []
    csnd276bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csnd276bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csnd276bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csnd276bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csnd276bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnd276bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '588695a7cf7748b8adadcd1d49985250',
        'volumes': csnd276bJob_vol,
        'volume_mounts': csnd276bJob_volMnt,
        'env_from':csnd276bJob_env,
        'task_id':'csnd276bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.78',
        'arguments':["--job.name=csnd276bJob", "endTranDate=20240530", "pgmNm=csnd276b", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('1db5f0cee05a420f868e37be9a20a678')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnd276bJob,
        Complete
    ]) 

    # authCheck >> csnd276bJob >> Complete
    workflow








