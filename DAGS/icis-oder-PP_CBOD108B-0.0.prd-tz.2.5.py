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
                , WORKFLOW_NAME='PP_CBOD108B',WORKFLOW_ID='401315b4706e40a996a7e11f9c4fdd9e', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOD108B-0.0.prd-tz.2.5'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 29, 12, 9, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('401315b4706e40a996a7e11f9c4fdd9e')

    cbod108bJob_vol = []
    cbod108bJob_volMnt = []
    cbod108bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbod108bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbod108bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    cbod108bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbod108bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbod108bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'f3a333f483c549fd9e38f896ea874cf4',
        'volumes': cbod108bJob_vol,
        'volume_mounts': cbod108bJob_volMnt,
        'env_from':cbod108bJob_env,
        'task_id':'cbod108bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.3',
        'arguments':["--job.name=cbod108bJob", "workDate=20240430", "saveSaId=11778599831","saveOrdNo=23002AT03694092",  "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('401315b4706e40a996a7e11f9c4fdd9e')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbod108bJob,
        Complete
    ]) 

    # authCheck >> cbod108bJob >> Complete
    workflow








