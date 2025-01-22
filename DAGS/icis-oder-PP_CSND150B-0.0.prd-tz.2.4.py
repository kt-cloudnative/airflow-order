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
                , WORKFLOW_NAME='PP_CSND150B',WORKFLOW_ID='cacca31a4c794a27b92f8c49cdcc48e7', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSND150B-0.0.prd-tz.2.4'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 22, 15, 51, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('cacca31a4c794a27b92f8c49cdcc48e7')

    csnd150bJob_vol = []
    csnd150bJob_volMnt = []
    csnd150bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csnd150bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csnd150bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csnd150bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csnd150bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnd150bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '7a8db76c594c49f7b6987bc46dc556ba',
        'volumes': csnd150bJob_vol,
        'volume_mounts': csnd150bJob_volMnt,
        'env_from':csnd150bJob_env,
        'task_id':'csnd150bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.57',
        'arguments':["--job.name=csnd150bJob" , "endTranDate=20240530","requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('cacca31a4c794a27b92f8c49cdcc48e7')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnd150bJob,
        Complete
    ]) 

    # authCheck >> csnd150bJob >> Complete
    workflow








