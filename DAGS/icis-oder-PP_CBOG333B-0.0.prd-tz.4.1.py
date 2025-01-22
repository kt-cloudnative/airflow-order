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
                , WORKFLOW_NAME='PP_CBOG333B',WORKFLOW_ID='ee2cef08c8704a3ea5116a688564cf99', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOG333B-0.0.prd-tz.4.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 19, 21, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('ee2cef08c8704a3ea5116a688564cf99')

    cbog333bJob_vol = []
    cbog333bJob_volMnt = []
    cbog333bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbog333bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbog333bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    cbog333bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbog333bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog333bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '0f3533dc815e49058785884ec1f8c39d',
        'volumes': cbog333bJob_vol,
        'volume_mounts': cbog333bJob_volMnt,
        'env_from':cbog333bJob_env,
        'task_id':'cbog333bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.3',
        'arguments':["--job.name=cbog333bJob", "endTranDate=20240528", "ver="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('ee2cef08c8704a3ea5116a688564cf99')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbog333bJob,
        Complete
    ]) 

    # authCheck >> cbog333bJob >> Complete
    workflow








