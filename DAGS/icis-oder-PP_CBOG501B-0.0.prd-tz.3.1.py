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
                , WORKFLOW_NAME='PP_CBOG501B',WORKFLOW_ID='44d4a8c37e86436d848eb503242ceeee', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOG501B-0.0.prd-tz.3.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 19, 21, 23, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('44d4a8c37e86436d848eb503242ceeee')

    cbog501bJob_vol = []
    cbog501bJob_volMnt = []
    cbog501bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbog501bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbog501bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    cbog501bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbog501bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog501bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '3fe72f46aaa44fb49dd6bf7e8ac8f1c8',
        'volumes': cbog501bJob_vol,
        'volume_mounts': cbog501bJob_volMnt,
        'env_from':cbog501bJob_env,
        'task_id':'cbog501bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.73',
        'arguments':["--job.name=cbog501bJob", "mainArgDate=20240528" , "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('44d4a8c37e86436d848eb503242ceeee')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbog501bJob,
        Complete
    ]) 

    # authCheck >> cbog501bJob >> Complete
    workflow








